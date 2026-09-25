//! The recommended policy: grow while the bottleneck waits, but keep shutdown quick.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use bon::bon;

use super::{Diagnostics, Policy};
use crate::Sample;

/// The fewest departures a window at its longest needs before the fastest time is measured from
/// it: below this, a single item leaving early or late swings the estimate too far.
const MIN_FASTEST_ITEMS: u64 = 4;

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
/// also have finished recently (within [`max_window`], or one pass if that is longer).
/// Otherwise a failure where every item errors out quickly, or hangs, would look like a hungry
/// bottleneck, and the limit would grow for nothing.
///
/// After a raise, the policy doesn't raise again until the items it let in have had time to
/// reach the bottleneck (one pass through the pipeline, at most [`max_settle`]). Until they have,
/// the bottleneck is still waiting for them, and raising again would only overshoot.
///
/// **Lowering.** The policy estimates how long an item stays in the pipeline: the average
/// number of items inside divided by how many leave per second. That is also roughly how long a
/// clean shutdown would take. While the only problem is upstream delay, letting more in doesn't
/// make items slower, so this time stays flat. Once the bottleneck has all it can handle, extra
/// items only queue up, and the time rises. When it passes the allowed bound, the policy lowers
/// the limit to what can leave within the bound. If nothing at all leaves for a whole window,
/// the pipeline is stuck, and the limit drops to [`floor`].
///
/// Right after a raise, more items are inside but none of the new ones have come out yet, so
/// items look slower than they are: about as many times as the limit grew in one pass, which is
/// at most [`growth`]. Until the new items show up in the measurement, the bound is widened by
/// that much.
///
/// **The bound** is the larger of [`drain_target`] and the fastest time observed times
/// [`max_slowdown`]. That way a pipeline that is slow by nature (say, 20 seconds per item) isn't
/// squeezed below what it needs. Queueing only ever adds to how long items take, so the policy
/// keeps the shortest time it has measured while the gate was full, once the pipeline has
/// filled up and settled after the last change. When measured while the bottleneck waits for
/// input, which is when nothing queues for it, the remembered time also creeps up a little, so a
/// pipeline whose upstream becomes slower for good is eventually judged by its new speed. Until
/// a fastest time is known, the limit is only lowered when the pipeline is stuck: before that,
/// long times may just be the pipeline filling up.
///
/// **Measuring.** Rates are counted over the most recent ticks that together saw at least
/// [`window_items`] items leave and cover one pass through the pipeline, or else span
/// [`max_window`]. With few items per tick, a single tick in which nothing happened to finish
/// says little, so the window stretches over several ticks until it has seen enough. And items
/// tend to leave in waves one pass apart, since a raise lets many in at once; a window at least
/// as long as a pass sees a whole wave rather than its crest or trough.
///
/// **Waiting after lowering.** After lowering the limit, the policy waits until the items over
/// the new limit have had time to leave (at most [`max_settle`]) before lowering it further.
///
/// [`floor`]: DrainBoundedBuilder::floor
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
    /// items in flight at the previous sample, to average over the interval since
    last_in_flight: Option<usize>,
    /// lowest residence in seconds measured from a clean window, slowly decayed while the
    /// bottleneck starves to follow lasting slowdowns upstream
    fastest: f64,
    /// the end of the first tick in which items left: a window reaching back before it holds
    /// the pipeline filling up, when everything is inside and nothing has come out
    filled_at: Option<Instant>,
    /// the end of the latest tick in which items completed
    completed_at: Option<Instant>,
    /// when the limit last changed: a window reaching back before it mixes two limits
    changed_at: Option<Instant>,
    /// after a shrink, hold further shrinks until the excess has had time to leave
    settle_until: Option<Instant>,
    /// the latest run of raises, while their items haven't reached the whole window yet
    ramp: Option<Ramp>,
    /// after a raise, hold the next one until the new items have reached the bottleneck
    raise_after: Option<Instant>,
    /// seconds one pass through the pipeline takes, as of the previous decision, once the
    /// fastest time grounds it; `None` before, so a stall can't stretch it without end
    pass: Option<f64>,
    /// the latest decision's inputs, for diagnostics
    residence: f64,
    bound: f64,
    starving: bool,
    saturated: bool,
}

/// Raises still working their way through the pipeline: the items they let in have not left
/// yet, or not long enough ago to fill the window, so the window underestimates departures.
#[derive(Debug, Clone, Copy)]
struct Ramp {
    /// the limit before the first of these raises
    from: usize,
    /// when the latest raise's items should have started leaving
    until: Instant,
}

/// The shortest run of recent ticks that saw `window_items` departures and spans one pass
/// through the pipeline, or else spans the longest window allowed.
#[derive(Debug, Clone, Default)]
struct Window {
    ticks: VecDeque<Tick>,
    elapsed: Duration,
    completed: u64,
    departed: u64,
}

#[derive(Debug, Clone, Copy)]
struct Tick {
    start: Instant,
    elapsed: Duration,
    completed: u64,
    departed: u64,
    /// items in flight integrated over the tick, in item-seconds
    occupancy: f64,
}

impl Window {
    /// Add the newest tick, then drop the oldest ones as long as the rest spans `min` and saw
    /// `items` departures, or spans `max`.
    fn push(&mut self, tick: Tick, items: u64, min: Duration, max: Duration) {
        self.elapsed += tick.elapsed;
        self.completed += tick.completed;
        self.departed += tick.departed;
        self.ticks.push_back(tick);
        // ticks only get added at the back, so once the rest is enough, the oldest is never
        // needed again
        while self.ticks.len() > 1 {
            let oldest = self.ticks[0];
            let elapsed = self.elapsed - oldest.elapsed;
            let departed = self.departed - oldest.departed;
            if (departed < items || elapsed < min) && elapsed < max {
                break;
            }
            self.ticks.pop_front();
            self.elapsed = elapsed;
            self.completed -= oldest.completed;
            self.departed = departed;
        }
    }

    fn start(&self) -> Option<Instant> {
        self.ticks.front().map(|tick| tick.start)
    }

    fn rate(&self, items: u64) -> f64 {
        match self.elapsed.is_zero() {
            true => 0.0,
            false => items as f64 / self.elapsed.as_secs_f64(),
        }
    }

    /// Average seconds an item spent inside, by Little's law: the average number inside over
    /// the window, divided by how many left per second in the same window.
    fn residence(&self) -> f64 {
        // summed afresh: a running float total would drift over a long run
        let occupancy: f64 = self.ticks.iter().map(|tick| tick.occupancy).sum();
        match (self.departed, occupancy > 0.0) {
            // nothing was inside: an idle pipeline isn't one that can't drain
            (_, false) => 0.0,
            (0, true) => f64::INFINITY,
            (departed, true) => occupancy / departed as f64,
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
        ///
        /// Two things can stretch it: the window always covers at least one pass through the
        /// pipeline, and it is made of whole ticks, so it can run over by up to one tick.
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
        /// The longest the policy waits after lowering the limit before it may lower it again.
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
            last_in_flight: None,
            fastest: f64::INFINITY,
            filled_at: None,
            completed_at: None,
            changed_at: None,
            settle_until: None,
            ramp: None,
            raise_after: None,
            pass: None,
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

    /// Seconds one pass through the pipeline takes, as best known. Before the fastest time
    /// grounds it, the latest residence measured, which is all startup has; a stall inflates
    /// that, so it counts for at most `max_window`.
    fn pass_estimate(&self) -> Option<f64> {
        let measured = self.residence.min(self.config.max_window.as_secs_f64());
        self.pass.or(Some(measured))
    }

    /// The shortest time an item took to go through the pipeline while the gate was full,
    /// slowly creeping up while the bottleneck waits for input. `None` until one has been
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
        if fresh {
            let before = self.last_in_flight.unwrap_or(sample.in_flight);
            let tick = Tick {
                start: sample.at.checked_sub(sample.elapsed).unwrap_or(sample.at),
                elapsed: sample.elapsed,
                completed: sample.completed,
                departed: sample.completed + sample.released,
                // only the ends are known: assume in flight changed evenly in between
                occupancy: (before + sample.in_flight) as f64 / 2.0 * sample.elapsed.as_secs_f64(),
            };
            // departures come in waves (a raise lets many in at once, which leave together one
            // pass later), so a window shorter than a pass sees crests and troughs. It spans the
            // estimated             // pass. Only a grounded pass may stretch it past `max_window`
            let min = pass_duration(self.pass_estimate()).unwrap_or(c.max_window);
            let max = pass_duration(self.pass).map_or(c.max_window, |pass| pass.max(c.max_window));
            self.window.push(tick, c.window_items, min, max);
            self.last_in_flight = Some(sample.in_flight);
            if tick.departed > 0 && self.filled_at.is_none() {
                self.filled_at = Some(sample.at);
            }
            if tick.completed > 0 {
                self.completed_at = Some(sample.at);
            }
        }
        let departures = self.departures();
        self.starving = sample.idle_shares().any(|share| share > c.starving);
        self.residence = self.window.residence();
        self.saturated = sample.in_flight as u128 * 10 >= limit as u128 * 9;
        // an empty interval says nothing about rates: report the state above, change nothing
        if !fresh {
            return limit;
        }
        let start = self.window.start();
        // raises have ramped up once the window no longer reaches back to before their items
        // started leaving
        if self
            .ramp
            .is_some_and(|ramp| start.is_some_and(|start| start >= ramp.until))
        {
            self.ramp = None;
        }
        // measure only from a clean window. The gate full and not over it: a near-empty
        // pipeline (no work, tail of a run) shows a short residence that says nothing about
        // per-item latency, and one still retiring a shrink's permits is draining faster than
        // it fills. Wholly after the pipeline first filled and after the last change, so it
        // sees one limit in steady flow, not everything inside and nothing out yet. Enough
        // items, since the minimum of noise is too low. And mostly finished items: when they
        // fail fast, residence is how long failing takes, not how long working does
        let after =
            |at: Option<Instant>| start.is_some_and(|start| at.is_some_and(|at| start >= at));
        let enough = self.window.departed >= c.window_items
            || (self.window.elapsed >= c.max_window && self.window.departed >= MIN_FASTEST_ITEMS);
        if self.saturated
            && sample.in_flight <= limit
            && self.ramp.is_none()
            && after(self.filled_at)
            && (self.changed_at.is_none() || after(self.changed_at))
            && enough
            && self.window.completed * 2 >= self.window.departed
            && self.residence > 0.0
            && self.residence.is_finite()
        {
            // queueing only adds, so any clean reading may lower it. Only while the bottleneck
            // starves, when nothing queues for it, may it creep up: otherwise a slowing
            // bottleneck would drag the bound up with it
            if self.starving {
                self.fastest *= c.fastest_decay;
            }
            self.fastest = self.fastest.min(self.residence);
        }
        // no latency known yet: long times may be the pipeline filling up, so only a stall
        // lowers the limit
        let drain_bound = match self.fastest.is_finite() {
            true => c
                .drain_target
                .as_secs_f64()
                .max(self.fastest * c.max_slowdown),
            false => f64::INFINITY,
        };
        // while raises ramp up, residence reads as many times high as the limit grew in one
        // pass: the window averages what was inside and what left over the same stretch, so
        // earlier raises show in both. Raises are at least a pass apart, so that is one growth
        self.bound = match self.ramp {
            Some(ramp) => drain_bound * (limit as f64 / ramp.from as f64).clamp(1.0, c.growth),
            None => drain_bound,
        };
        // grounded in the fastest time: before it is known, a stall would stretch it without end
        self.pass = drain_bound
            .is_finite()
            .then(|| self.residence.min(drain_bound));
        // items went in, and have come out before, yet none left in a whole window
        let stuck = self.filled_at.is_some()
            && sample.in_flight > 0
            && self.window.departed == 0
            && self.window.elapsed >= c.max_window;
        // how recent a completion must be to count: not tied to the window, which a stall
        // could otherwise stretch to keep one old completion in it
        let recent = c
            .max_window
            .max(pass_duration(self.pass).unwrap_or_default());

        let settling = self.settle_until.is_some_and(|until| sample.at < until);
        let target = if stuck || self.residence > self.bound {
            match settling {
                // the latest shrink hasn't taken effect yet
                true => limit,
                // draining would take too long (or nothing leaves): keep what leaves in time.
                // Never above the limit: while a shrink's permits still retire, more can be in
                // flight than the limit allows, and this branch must not grow it
                false if stuck => 0,
                false => ((departures * drain_bound) as usize).min(limit),
            }
        } else if self.starving
            && self.saturated
            && self.raise_after.is_none_or(|after| sample.at >= after)
            && self
                .completed_at
                .is_some_and(|at| sample.at.saturating_duration_since(at) <= recent)
        {
            // the bottleneck waits while the gate is full; growth also needs items completing
            // recently, or an outage (or startup) would grow it while every item fails or hangs
            (limit as f64 * c.growth).ceil() as usize
        } else {
            limit
        }
        .clamp(c.floor, c.max);

        if target != limit {
            self.changed_at = Some(sample.at);
        }
        if target > limit {
            // the new items reach the bottleneck, and start leaving, about one pass from now; a
            // pass that can't be told counts as `max_settle`
            let pass = pass_duration(self.pass_estimate()).unwrap_or(c.max_settle);
            self.raise_after = sample.at.checked_add(pass.min(c.max_settle));
            self.ramp = sample.at.checked_add(pass).map(|until| Ramp {
                from: self.ramp.map_or(limit, |ramp| ramp.from).max(1),
                until,
            });
        } else if target < limit {
            self.ramp = None;
            self.raise_after = None;
            // wait for the items over the new limit to leave at the current rate, or
            // `max_settle` if nothing leaves; a hold that overflows `Instant` is no hold
            let settle = match sample.in_flight.saturating_sub(target) {
                0 => Duration::ZERO,
                excess => Duration::try_from_secs_f64(excess as f64 / departures)
                    .map_or(c.max_settle, |settle| settle.min(c.max_settle)),
            };
            self.settle_until = sample.at.checked_add(settle);
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
        d.push("window", self.window.elapsed.as_secs_f64());
        d.push("starving", f64::from(u8::from(self.starving)));
        d.push("saturated", f64::from(u8::from(self.saturated)));
        d
    }
}

fn pass_duration(pass: Option<f64>) -> Option<Duration> {
    pass.and_then(|pass| Duration::try_from_secs_f64(pass).ok())
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
