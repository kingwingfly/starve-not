//! The recommended policy: grow while the bottleneck waits, but keep shutdown quick.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use bon::bon;

use super::{Diagnostics, Policy};
use crate::Sample;

/// Raises the limit while the bottleneck is waiting for input, and lowers it when the work
/// inside would take too long to finish.
///
/// This is the policy to start with. It needs at least one [`IdleProbe`](crate::IdleProbe) on
/// the pacer, because the probe is how it learns that the bottleneck is waiting. Without one it
/// never raises the limit.
///
/// # How it decides
///
/// **Raising.** If the bottleneck spent more than a little of the last tick waiting for input
/// ([`starving`]), while the gate was nearly full, then not enough work is being let in to hide
/// the upstream delay. The limit is multiplied by [`growth`] (doubled by default). Items must
/// also have finished recently. Otherwise a failure where every item errors out quickly would
/// look like a hungry bottleneck, and the limit would grow for nothing.
///
/// **Lowering.** The policy estimates how long an item stays in the pipeline: the number of
/// items inside divided by how many leave per second. That is also roughly how long a clean
/// shutdown would take. While the only problem is upstream delay, letting more in doesn't make
/// items slower, so this time stays flat. Once the bottleneck has all it can handle, extra items
/// only queue up, and the time rises. When it passes the allowed bound, the policy lowers the
/// limit to what can leave within the bound.
///
/// **The bound** is the larger of [`drain_target`] and the fastest time observed times
/// [`max_slowdown`]. That way a pipeline that is slow by nature (say, 20 seconds per item) isn't
/// squeezed below what it needs. The fastest time is only measured while the bottleneck waits
/// for input, since only then is it free of items queueing for the bottleneck. Each time, the
/// remembered one creeps up a little, so a pipeline whose upstream becomes slower for good is
/// eventually judged by its new speed.
///
/// **Measuring.** How fast items leave is counted over the most recent ticks that together saw
/// at least [`window_items`] items leave and cover one pass through the pipeline (but no more
/// than [`max_window`] of time). With few items per tick, a single tick in which nothing
/// happened to finish says little, so the window stretches over several ticks until it has
/// seen enough. And items tend to leave in waves one pass apart, since a raise lets many in at
/// once; a window as long as a pass sees a whole wave rather than its crest or trough.
///
/// **Waiting after a change.** After changing the limit, the policy waits roughly one pass
/// through the pipeline (at most [`max_settle`]) before changing it again, so the change has
/// time to show its effect. Right after a raise, more items are inside but none of the new ones
/// have come out yet, so items look slower than they are: up to [`growth`] times. Until the new
/// items show up in the measurement, the bound is widened by as much as the limit grew.
///
/// [`starving`]: DrainBoundedBuilder::starving
/// [`growth`]: DrainBoundedBuilder::growth
/// [`drain_target`]: DrainBoundedBuilder::drain_target
/// [`max_slowdown`]: DrainBoundedBuilder::max_slowdown
/// [`max_settle`]: DrainBoundedBuilder::max_settle
/// [`window_items`]: DrainBoundedBuilder::window_items
/// [`max_window`]: DrainBoundedBuilder::max_window
///
/// # Example
///
/// ```
/// # use std::time::Duration;
/// # use starve_not::DrainBounded;
/// // never below two batches of 16, and aim for shutdown to take about 5 seconds
/// let policy = DrainBounded::builder().floor(32).drain_target(Duration::from_secs(5)).build();
/// ```
///
/// # Diagnostics
///
/// Each decision reports `throughput` (items finished per second), `departures` (items leaving
/// per second, finished or not), `residence` (estimated seconds an item stays inside),
/// `fastest` (the fastest such time observed), `bound` (the residence allowed right now, in
/// seconds), `window` (the seconds the rates were measured over), and `starving` and
/// `saturated` (1 if the bottleneck was waiting, or the gate was nearly full, else 0).
#[derive(Debug, Clone)]
pub struct DrainBounded {
    config: Config,
    /// the recent ticks the rates are measured over
    window: Window,
    /// lowest residence in seconds observed while the bottleneck starved, slowly decayed to
    /// follow lasting slowdowns upstream
    fastest: f64,
    /// hold decisions until newly admitted items have had time to complete
    settle_until: Option<Instant>,
    /// raises whose new items haven't reached the whole window yet, oldest first
    ramps: VecDeque<Ramp>,
    /// seconds one pass through the pipeline takes, as of the previous decision
    pass: f64,
    /// the latest decision's inputs, for diagnostics
    residence: f64,
    bound: f64,
    starving: bool,
    saturated: bool,
}

/// A raise still working its way through the pipeline: the items it let in have not left yet,
/// or not long enough ago to fill the window, so the window underestimates departures.
#[derive(Debug, Clone, Copy)]
struct Ramp {
    /// the limit before the raise
    from: usize,
    /// when the raise's items should have started leaving
    until: Instant,
}

/// The shortest run of recent ticks that saw `window_items` departures and spans one pass
/// through the pipeline, or else spans `max_window`.
#[derive(Debug, Clone, Default)]
struct Window {
    ticks: VecDeque<Tick>,
    secs: f64,
    completed: u64,
    departed: u64,
}

#[derive(Debug, Clone, Copy)]
struct Tick {
    start: Instant,
    secs: f64,
    completed: u64,
    departed: u64,
}

impl Window {
    fn push(&mut self, tick: Tick, items: u64, min_secs: f64, max_secs: f64) {
        self.secs += tick.secs;
        self.completed += tick.completed;
        self.departed += tick.departed;
        self.ticks.push_back(tick);
        // ticks only get added at the back, so once the rest is enough, the oldest is never
        // needed again
        while self.ticks.len() > 1 {
            let oldest = self.ticks[0];
            let (departed, secs) = (self.departed - oldest.departed, self.secs - oldest.secs);
            if (departed < items || secs < min_secs) && secs < max_secs {
                break;
            }
            self.ticks.pop_front();
            self.secs -= oldest.secs;
            self.completed -= oldest.completed;
            self.departed -= oldest.departed;
        }
    }

    fn start(&self) -> Option<Instant> {
        self.ticks.front().map(|tick| tick.start)
    }

    fn rate(&self, items: u64) -> f64 {
        match self.secs > 0.0 {
            true => items as f64 / self.secs,
            false => 0.0,
        }
    }
}

#[bon]
impl DrainBounded {
    /// Set up the policy. Every setting has a default, so `DrainBounded::builder().build()`
    /// (or [`DrainBounded::default()`]) works as is.
    #[builder(finish_fn(doc {
        /// Create the policy.
        ///
        /// # Panics
        ///
        /// If a setting is out of range: `floor` is 0, `max_slowdown` is below 1, `growth` is 1
        /// or less, `window_items` is 0, `max_window` is zero, `starving` is below 0 or 1 or
        /// more, or `fastest_decay` is below 1. Such values would quietly keep the limit from ever
        /// growing, or stick it at the floor.
    }))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        /// The lowest the limit goes. Default: 1.
        ///
        /// Two batches is a good choice: the bottleneck works on one while the next waits.
        #[builder(default = 1)]
        floor: usize,
        /// The highest the limit goes, as a safety cap on memory. Default: 1024.
        ///
        /// If set below `floor`, `floor` is used.
        #[builder(default = 1024)]
        max: usize,
        /// The limit to start with. Default: `floor`. Kept between `floor` and `max`.
        initial: Option<usize>,
        /// How long the policy aims for items to stay inside the pipeline, which is also about
        /// how long a clean shutdown takes. Default: 10 seconds.
        ///
        /// For most pipelines this works as a cap: the limit comes down whenever items would
        /// take longer. The exception is a pipeline where items take longer than this even at
        /// their fastest. Capping it here would starve the bottleneck, so the policy allows
        /// [`max_slowdown`](Self::max_slowdown) times the fastest time observed instead.
        #[builder(default = Duration::from_secs(10))]
        drain_target: Duration,
        /// How many times slower than their fastest observed time items may get before the
        /// limit comes down. Default: 1.5, so items may take up to 50% longer. Must be at least 1.
        ///
        /// Only matters for pipelines where items take longer than `drain_target` even at their
        /// fastest.
        #[builder(default = 1.5)]
        max_slowdown: f64,
        /// What the limit is multiplied by each time it grows. Default: 2. Must be above 1.
        #[builder(default = 2.0)]
        growth: f64,
        /// How many items must have left the pipeline (finished or not) before the policy trusts
        /// how fast they leave. Default: 20. Must be at least 1.
        ///
        /// Rates are measured over as many recent ticks as it takes to see this many items leave,
        /// and to cover one pass through the pipeline. When plenty finish every tick and items
        /// pass quickly, that is just the latest tick. Higher is steadier but reacts more slowly
        /// when items are few.
        #[builder(default = 20)]
        window_items: u64,
        /// The longest stretch of time rates are measured over, however few items left in it.
        /// Default: 30 seconds. Must be non-zero.
        #[builder(default = Duration::from_secs(30))]
        max_window: Duration,
        /// How much of a tick the bottleneck may spend waiting before it counts as starving.
        /// Default: 0.1, that is, 10%.
        ///
        /// At least 0 and below 1. A little waiting is normal (handing over a batch takes some
        /// time), so 0 is rarely a good idea.
        #[builder(default = 0.1)]
        starving: f64,
        /// How fast the remembered fastest time creeps back up, as a factor per tick in which it
        /// is measured (the bottleneck waits and the gate is full). Default: 1.05, that is, 5%
        /// per tick. Must be at least 1.
        ///
        /// This lets the policy accept a pipeline whose upstream has become slower for good. 1
        /// means never forgetting.
        #[builder(default = 1.05)]
        fastest_decay: f64,
        /// The longest the policy waits after a change before it may change the limit again.
        /// Default: 30 seconds.
        #[builder(default = Duration::from_secs(30))]
        max_settle: Duration,
    ) -> Self {
        assert!(floor > 0, "floor must be at least 1");
        assert!(max_slowdown >= 1.0, "max_slowdown must be at least 1");
        assert!(growth > 1.0, "growth must be above 1");
        assert!((0.0..1.0).contains(&starving), "starving must be in [0, 1)");
        assert!(window_items > 0, "window_items must be at least 1");
        assert!(!max_window.is_zero(), "max_window must be non-zero");
        assert!(fastest_decay >= 1.0, "fastest_decay must be at least 1");
        Self {
            config: Config {
                floor,
                max: max.max(floor),
                initial,
                drain_target,
                max_slowdown,
                growth,
                window_items,
                max_window,
                starving,
                fastest_decay,
                max_settle,
            },
            window: Window::default(),
            fastest: f64::INFINITY,
            settle_until: None,
            ramps: VecDeque::new(),
            pass: f64::INFINITY,
            residence: f64::INFINITY,
            bound: f64::INFINITY,
            starving: false,
            saturated: false,
        }
    }

    /// Items finished per second, averaged over recent ticks.
    pub fn throughput(&self) -> f64 {
        self.window.rate(self.window.completed)
    }

    /// Items leaving per second, finished or not, averaged over recent ticks.
    fn departures(&self) -> f64 {
        self.window.rate(self.window.departed)
    }

    /// The shortest time an item took to go through the pipeline while the gate was full and
    /// the bottleneck waited for input, slowly creeping up over time. `None` until one has been
    /// observed.
    pub fn fastest(&self) -> Option<Duration> {
        // can decay past what `Duration` holds
        Duration::try_from_secs_f64(self.fastest).ok()
    }
}

impl Default for DrainBounded {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl Policy for DrainBounded {
    fn initial(&self) -> usize {
        let c = &self.config;
        c.initial.unwrap_or(c.floor).clamp(c.floor, c.max)
    }

    fn decide(&mut self, sample: &Sample) -> usize {
        let c = &self.config;
        let limit = sample.limit;
        let fresh = !sample.elapsed.is_zero();
        let max_window = c.max_window.as_secs_f64();
        if fresh {
            let tick = Tick {
                start: sample.at.checked_sub(sample.elapsed).unwrap_or(sample.at),
                secs: sample.elapsed.as_secs_f64(),
                completed: sample.completed,
                departed: sample.completed + sample.released,
            };
            // departures come in waves (a raise lets a batch in at once, which leaves together
            // one pass later), so a window shorter than a pass sees crests and troughs
            self.window
                .push(tick, c.window_items, self.pass.min(max_window), max_window);
        }
        let departures = self.departures();
        self.starving = sample.idle_shares().any(|share| share > c.starving);
        self.residence = match (sample.in_flight, departures > 0.0) {
            // nothing to drain: an idle pipeline isn't one that can't drain
            (0, _) => 0.0,
            (in_flight, true) => in_flight as f64 / departures,
            (_, false) => f64::INFINITY,
        };
        self.saturated = sample.in_flight as u128 * 10 >= limit as u128 * 9;
        // an empty interval says nothing about rates: report the state above, change nothing
        if !fresh {
            return limit;
        }
        let settling = self.settle_until.is_some_and(|until| sample.at < until);
        // a raise has ramped up once the window no longer reaches back to before its items
        // started leaving
        if let Some(start) = self.window.start() {
            while self.ramps.front().is_some_and(|ramp| start >= ramp.until) {
                self.ramps.pop_front();
            }
        }
        // sample only while the bottleneck starves: otherwise residence includes items queueing
        // for it, which is what the bound is there to limit, and a slowing bottleneck would drag
        // the bound up with it. Only while the gate is full: a near-empty pipeline (no work,
        // tail of a run) shows a short residence that says nothing about per-item latency. Only
        // from a full window (or the longest one), since a few items give a noisy rate and the
        // minimum of noise is too low. And not right after a change, when the window mixes the
        // old and new limit
        if self.starving
            && self.saturated
            && !settling
            && (self.window.departed >= c.window_items || self.window.secs >= max_window)
            && self.residence > 0.0
            && self.residence.is_finite()
        {
            self.fastest = (self.fastest * c.fastest_decay).min(self.residence);
        }
        let drain_bound = match self.fastest.is_finite() {
            true => c
                .drain_target
                .as_secs_f64()
                .max(self.fastest * c.max_slowdown),
            false => c.drain_target.as_secs_f64(),
        };
        // while raises ramp up, residence reads up to as many times high as the limit grew
        self.bound = match self.ramps.front() {
            Some(ramp) => drain_bound * (limit as f64 / ramp.from as f64).max(1.0),
            None => drain_bound,
        };
        self.pass = self.residence.min(drain_bound);

        let target = if settling {
            limit
        } else if self.residence > self.bound {
            // draining would take too long (or nothing completes): keep what finishes in time.
            // Never above the limit: while a shrink's permits still retire, more can be in
            // flight than the limit allows, and this branch must not grow it
            ((departures * drain_bound) as usize).min(limit)
        } else if self.starving && self.saturated && self.window.completed > 0 {
            // the bottleneck waits while the gate is full; growth also needs items completing
            // recently, or an outage (or startup) would grow it while every item fails
            (limit as f64 * c.growth).ceil() as usize
        } else {
            limit
        }
        .clamp(c.floor, c.max);
        if target != limit {
            // wait for one pass through the pipeline: the one measured now or, after a shrink,
            // the one it aims for. It may be infinite or past `Duration::MAX`, and a hold that
            // overflows `Instant` is no hold
            let settle = Duration::try_from_secs_f64(self.pass)
                .map_or(c.max_settle, |pass| pass.min(c.max_settle));
            self.settle_until = sample.at.checked_add(settle);
            match (target > limit, self.settle_until) {
                (true, Some(until)) => self.ramps.push_back(Ramp { from: limit, until }),
                // after a shrink, or a raise whose end can't be told, judge by the plain bound
                _ => self.ramps.clear(),
            }
        }
        target
    }

    fn diagnostics(&self) -> Diagnostics {
        let mut d = Diagnostics::default();
        d.push("throughput", self.throughput());
        d.push("departures", self.departures());
        d.push("residence", self.residence);
        d.push("fastest", self.fastest);
        d.push("bound", self.bound);
        d.push("window", self.window.secs);
        d.push("starving", f64::from(u8::from(self.starving)));
        d.push("saturated", f64::from(u8::from(self.saturated)));
        d
    }
}

/// [`DrainBounded`]'s settings, as its builder set them.
#[derive(Debug, Clone)]
struct Config {
    floor: usize,
    max: usize,
    initial: Option<usize>,
    drain_target: Duration,
    max_slowdown: f64,
    growth: f64,
    window_items: u64,
    max_window: Duration,
    starving: f64,
    fastest_decay: f64,
    max_settle: Duration,
}
