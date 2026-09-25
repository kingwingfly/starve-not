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
/// # What it controls
///
/// The gate only decides how many items are let in, so the limit only controls how fast
/// upstream is fed. Everything from the bottleneck on runs at its own pace. The aim is that this
/// part never waits for input, while the items inside can still finish in about
/// [`drain_target`] on shutdown.
///
/// A ticket is held until its item is done, so the time an item spends after the bottleneck
/// counts like any other. The probe on the bottleneck says when that part waits for input. If a
/// slower stage sits behind the bottleneck with room for more items than the limit, the probe
/// can't see its queue: see [Checking a raise](#checking-a-raise).
///
/// # How it decides
///
/// ### Residence
///
/// How long an item stays in the pipeline: the average number of items inside divided by how
/// many leave per second (Little's law). It is also about how long a clean shutdown takes.
///
/// It is measured over a *window* of recent ticks that saw at least [`window_items`] items leave
/// and covers one pass through the pipeline, or else spans [`max_window`]. A whole pass sees a
/// whole wave of departures, not just its crest or trough.
///
/// ### The bound
///
/// The longest residence allowed: [`drain_target`], or [`max_slowdown`] times the *fastest*
/// residence seen if that is longer, so a pipeline that is slow by nature isn't squeezed.
///
/// - Until the fastest time is known there is no bound, since a long residence may just be the
///   pipeline filling up. Raises then stop at [`drain_target`] instead.
/// - Right after a raise, the new items are inside but none has left yet, so residence reads
///   high, by at most [`growth`]. The bound is widened by as much until they show up in the
///   window.
///
/// ### The fastest time
///
/// Learned from everything since the pipeline last settled: since the first items left, the
/// limit last changed, or a raise's items came through. It needs at least [`window_items`]
/// items to have left, over at least one pass.
///
/// Queueing only adds to residence, so the lowest value seen is kept. It creeps up (by
/// [`fastest_decay`]) only while the bottleneck waits, when nothing queues for it, so a slower
/// upstream is eventually accepted but a slower bottleneck is not.
///
/// ### Raising
///
/// When the bottleneck waits for input ([`starving`]) while the gate is nearly full, the limit
/// is multiplied by [`growth`]. Only if all of these hold:
///
/// - Residence is within the bound (or [`drain_target`] while no bound is known): more items
///   can't make them leave sooner.
/// - Items have been succeeding: at least one since the last change, and at least half of those
///   leaving lately. Otherwise items that fail fast, or hang, would look like a hungry
///   bottleneck.
/// - The previous raise has shown: one pass has passed (at most [`max_settle`]), so its items
///   have reached the bottleneck, and departures have risen as in the next section.
///
/// ### Checking a raise
///
/// A waiting bottleneck turns more items into more departures. Once a raise's items come
/// through, departures should have risen by at least half as much as the limit did. If not,
/// the part after the gate wasn't short of input: the items are queueing where no probe sees,
/// such as behind the bottleneck.
///
/// If that raise also pushed residence past the bound, it is taken back, and there is no other
/// raise until departures change by a quarter or more. A raise that only failed to help but
/// kept within the bound is left alone.
///
/// This needs to know when the raise's items come through: once the fastest time is known, or
/// while a pass is shorter than [`max_window`].
///
/// ### Lowering
///
/// Letting more in only makes items slower once the bottleneck is busy: then they queue in
/// front of it, and residence rises. When residence passes the bound while the bottleneck is
/// busy, the limit is cut to what leaves within the bound. While the bottleneck waits nothing
/// queues for it, and a cut would only starve it further. After a cut, the next waits until the
/// excess has left (at most [`max_settle`]).
///
/// When nothing has left for longer than [`max_window`] and twice the longest silence seen
/// before, the pipeline is stuck. If the bottleneck waits, or no bound is known yet, the limit
/// drops to [`floor`].
///
/// [`floor`]: DrainBoundedBuilder::floor
/// [`starving`]: DrainBoundedBuilder::starving
/// [`growth`]: DrainBoundedBuilder::growth
/// [`drain_target`]: DrainBoundedBuilder::drain_target
/// [`max_slowdown`]: DrainBoundedBuilder::max_slowdown
/// [`max_settle`]: DrainBoundedBuilder::max_settle
/// [`window_items`]: DrainBoundedBuilder::window_items
/// [`max_window`]: DrainBoundedBuilder::max_window
/// [`fastest_decay`]: DrainBoundedBuilder::fastest_decay
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
/// per second, finished or not), `residence` (seconds an item stays inside), `fastest` (the
/// fastest residence seen), `bound` (the residence allowed right now), `window` (the seconds the
/// window spans), and `starving` and `saturated` (1 if the bottleneck was waiting, or the gate
/// was nearly full, else 0).
#[derive(Debug, Clone)]
pub struct DrainBounded {
    config: Config,
    /// recent ticks: residence and rates for the decisions
    window: Window,
    /// everything since the pipeline last settled: where the fastest time is learned
    settled: Tally,
    /// whether any item has left yet; until then the pipeline is still filling
    filled: bool,
    /// when an item last left (at first, when the first tick started)
    last_departure: Option<Instant>,
    /// the longest time without departures so far
    longest_silence: Duration,
    /// whether an item has succeeded since the limit last changed
    succeeded: bool,
    /// items in flight at the previous sample
    last_in_flight: Option<usize>,
    /// lowest residence seen, in seconds
    fastest: f64,
    /// one pass through the pipeline, once the fastest time is known
    pass: Option<Duration>,
    /// the raises whose items haven't reached the whole window yet
    ramp: Option<Ramp>,
    /// departures per second when a raise last failed to bring more; no raise until they
    /// change
    plateau: Option<f64>,
    /// no raise before this: the latest raise's items haven't reached the bottleneck
    raise_after: Option<Instant>,
    /// no cut before this: the latest cut's excess hasn't left
    cut_after: Option<Instant>,
    // the latest decision's inputs, for diagnostics
    residence: f64,
    bound: f64,
    starving: bool,
    saturated: bool,
}

/// Raises whose items haven't reached the whole window yet.
#[derive(Debug, Clone, Copy)]
struct Ramp {
    /// the limit before the first of these raises
    from: usize,
    /// the limit before the latest raise
    last: usize,
    /// departures per second before the latest raise
    departures: f64,
    /// whether the pass was known at the latest raise, or at least not capped at
    /// `max_window`; if not, the ramp may end before its items come through, and it can't be
    /// checked
    checked: bool,
    /// when the latest raise's items start leaving
    until: Instant,
}

impl Ramp {
    /// Whether the latest raise brought more departures. A waiting bottleneck gets more work
    /// from more items, so departures follow the limit; if they rose by less than half as much,
    /// the items queue where no probe sees.
    fn paid(&self, limit: usize, departures: f64) -> bool {
        let raised = limit as f64 / self.last as f64;
        departures >= self.departures * (1.0 + raised) / 2.0
    }
}

/// What happened over some stretch of time. All integers, so adding and removing ticks is
/// exact.
#[derive(Debug, Clone, Copy, Default)]
struct Tally {
    elapsed: Duration,
    /// items inside, summed over time, in item-nanoseconds
    occupancy: u128,
    departed: u64,
    completed: u64,
}

impl Tally {
    fn add(&mut self, other: &Tally) {
        self.elapsed += other.elapsed;
        self.occupancy += other.occupancy;
        self.departed += other.departed;
        self.completed += other.completed;
    }

    /// Remove a part of this tally, such as its oldest tick.
    fn sub(&mut self, part: &Tally) {
        self.elapsed -= part.elapsed;
        self.occupancy -= part.occupancy;
        self.departed -= part.departed;
        self.completed -= part.completed;
    }

    /// Seconds an item stays inside, by Little's law.
    fn residence(&self) -> f64 {
        match (self.departed, self.occupancy) {
            // nothing was inside: idle, not stuck
            (_, 0) => 0.0,
            (0, _) => f64::INFINITY,
            (departed, occupancy) => occupancy as f64 / 1e9 / departed as f64,
        }
    }

    /// Items finished per second.
    fn throughput(&self) -> f64 {
        match self.elapsed.is_zero() {
            true => 0.0,
            false => self.completed as f64 / self.elapsed.as_secs_f64(),
        }
    }

    /// Items leaving per second, finished or not.
    fn departures(&self) -> f64 {
        match self.elapsed.is_zero() {
            true => 0.0,
            false => self.departed as f64 / self.elapsed.as_secs_f64(),
        }
    }

    /// Whether at least half of the items that left succeeded.
    fn mostly_succeeded(&self) -> bool {
        self.completed * 2 >= self.departed
    }
}

/// The shortest run of recent ticks that saw `window_items` departures and spans `min`, or
/// else spans `max`.
#[derive(Debug, Clone, Default)]
struct Window {
    /// each tick, with when it started
    ticks: VecDeque<(Instant, Tally)>,
    /// the ticks summed
    total: Tally,
}

impl Window {
    fn push(&mut self, start: Instant, tick: Tally, items: u64, min: Duration, max: Duration) {
        self.ticks.push_back((start, tick));
        self.total.add(&tick);
        // drop the oldest ticks while the rest is still enough; ticks are only ever added at
        // the back, so a dropped tick is never needed again
        while self.ticks.len() > 1 {
            let (_, oldest) = self.ticks[0];
            let mut rest = self.total;
            rest.sub(&oldest);
            let enough = rest.departed >= items && rest.elapsed >= min;
            if !enough && rest.elapsed < max {
                break;
            }
            self.ticks.pop_front();
            self.total = rest;
        }
    }

    fn start(&self) -> Option<Instant> {
        self.ticks.front().map(|(start, _)| *start)
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
        /// How many items must have left (finished or not) before the policy trusts how fast
        /// they leave. Default: 20. Must be at least 1.
        ///
        /// The window stretches over as many recent ticks as it takes. Higher is steadier but
        /// reacts more slowly when items are few.
        #[builder(default = 20)]
        window_items: u64,
        /// The longest the window spans, however few items left in it. Default: 30 seconds.
        /// Must be non-zero.
        ///
        /// Once the fastest time is known, the window spans at least one pass through the
        /// pipeline, even if that is longer. It is made of whole ticks, so it can also run over
        /// by up to one tick.
        #[builder(default = Duration::from_secs(30))]
        max_window: Duration,
        /// How much of a tick the bottleneck may spend waiting before it counts as starving.
        /// Default: 0.1, that is, 10%.
        ///
        /// At least 0 and below 1. A little waiting is normal (handing over a batch takes some
        /// time), so 0 is rarely a good idea.
        #[builder(default = 0.1)]
        starving: f64,
        /// How fast the fastest time creeps back up, as a factor per tick in which it is
        /// measured while the bottleneck waits. Default: 1.05, that is, 5%. Must be at least 1.
        ///
        /// This lets the policy accept a pipeline whose upstream has become slower for good. 1
        /// means never forgetting.
        #[builder(default = 1.05)]
        fastest_decay: f64,
        /// The longest the policy waits after a change before it may change the limit the same
        /// way again. Default: 30 seconds.
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
            settled: Tally::default(),
            filled: false,
            last_departure: None,
            longest_silence: Duration::ZERO,
            succeeded: false,
            last_in_flight: None,
            fastest: f64::INFINITY,
            pass: None,
            ramp: None,
            plateau: None,
            raise_after: None,
            cut_after: None,
            residence: f64::INFINITY,
            bound: f64::INFINITY,
            starving: false,
            saturated: false,
        }
    }

    /// Items finished per second, averaged over recent ticks.
    pub fn throughput(&self) -> f64 {
        self.window.total.throughput()
    }

    /// The shortest time an item took to go through the pipeline while the gate was full,
    /// slowly creeping up while the bottleneck waits for input. `None` until one has been
    /// observed.
    pub fn fastest(&self) -> Option<Duration> {
        Duration::try_from_secs_f64(self.fastest).ok()
    }

    /// One pass through the pipeline, as best known: before the fastest time is known, the
    /// latest residence, which a stall inflates, so at most `max_window`.
    fn pass_estimate(&self) -> Option<Duration> {
        let measured = self.residence.min(self.config.max_window.as_secs_f64());
        self.pass
            .or_else(|| Duration::try_from_secs_f64(measured).ok())
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

        // 1. Record the tick.
        if !sample.elapsed.is_zero() {
            // only both ends are known: assume in flight changed evenly in between
            let before = self.last_in_flight.unwrap_or(sample.in_flight);
            let ends = before as u128 + sample.in_flight as u128;
            let tick = Tally {
                elapsed: sample.elapsed,
                occupancy: ends * sample.elapsed.as_nanos() / 2,
                departed: sample.completed + sample.released,
                completed: sample.completed,
            };
            self.last_in_flight = Some(sample.in_flight);
            let start = sample.at.checked_sub(sample.elapsed).unwrap_or(sample.at);
            // at least one pass, but only a known pass may go past `max_window`: a stall
            // would stretch an estimated one without end
            let min = self.pass_estimate().unwrap_or(c.max_window);
            let max = self
                .pass
                .map_or(c.max_window, |pass| pass.max(c.max_window));
            self.window.push(start, tick, c.window_items, min, max);
            if self.filled {
                self.settled.add(&tick);
            }
            // the first departures end the filling, which says nothing about the pace
            self.filled |= tick.departed > 0;
            let last_departure = *self.last_departure.get_or_insert(start);
            if tick.departed > 0 {
                let silence = sample.at.saturating_duration_since(last_departure);
                self.longest_silence = self.longest_silence.max(silence);
                self.last_departure = Some(sample.at);
            }
            self.succeeded |= tick.completed > 0;
        }
        let window = self.window.total;
        let departures = window.departures();
        self.residence = window.residence();
        self.starving = sample.idle_shares().any(|share| share > c.starving);
        self.saturated = sample.in_flight as u128 * 10 >= limit as u128 * 9;
        // no time passed: nothing to decide from
        if sample.elapsed.is_zero() {
            return limit;
        }

        // 2. End the ramp once the window starts after the raised items began to leave.
        let mut ended = None;
        if let (Some(ramp), Some(start)) = (self.ramp, self.window.start())
            && start >= ramp.until
        {
            self.ramp = None;
            self.settled = Tally::default();
            ended = Some(ramp);
        }
        // the pace changed: a raise may help now
        if let Some(plateau) = self.plateau
            && !(plateau * 0.75..=plateau / 0.75).contains(&departures)
        {
            self.plateau = None;
        }

        // 3. Learn the fastest time, from a full gate that isn't still shedding a cut's excess,
        // and from enough items over a whole pass that mostly succeeded (fast failures aren't
        // the pace, and part of a pass may catch a burst).
        let settled = self.settled.residence();
        if self.saturated
            && sample.in_flight <= limit
            && self.ramp.is_none()
            && self.settled.departed >= c.window_items
            && self.settled.elapsed.as_secs_f64() >= settled
            && self.settled.mostly_succeeded()
            && settled > 0.0
            && settled.is_finite()
        {
            // creep up only while nothing queues for the bottleneck
            if self.starving {
                self.fastest *= c.fastest_decay;
            }
            self.fastest = self.fastest.min(settled);
        }

        // 4. The bound: infinite until the fastest time is known, widened while a raise ramps.
        let drain_bound = match self.fastest.is_finite() {
            true => (self.fastest * c.max_slowdown).max(c.drain_target.as_secs_f64()),
            false => f64::INFINITY,
        };
        let widen = self.ramp.map_or(1.0, |ramp| {
            (limit as f64 / ramp.from as f64).clamp(1.0, c.growth)
        });
        self.bound = drain_bound * widen;
        // raising can't make items leave sooner: never past the bound, or past `drain_target`
        // before the bound is known
        let raise_bound = match drain_bound.is_finite() {
            true => self.bound,
            false => c.drain_target.as_secs_f64() * widen,
        };
        self.pass = match drain_bound.is_finite() {
            true => Duration::try_from_secs_f64(self.residence.min(drain_bound)).ok(),
            false => None,
        };

        // 5. Decide.
        // stuck: items are inside and have left before, but none for far longer than usual
        let silence = self
            .last_departure
            .map_or(Duration::ZERO, |at| sample.at.saturating_duration_since(at));
        let stuck = self.filled
            && sample.in_flight > 0
            && silence > c.max_window.max(self.longest_silence.saturating_mul(2));
        let may_cut = self.cut_after.is_none_or(|after| sample.at >= after);
        let may_raise = self.raise_after.is_none_or(|after| sample.at >= after);
        // a raise that brought no more departures and pushed residence past the bound only
        // lengthened a hidden queue
        let useless = ended.filter(|ramp| {
            ramp.checked
                && self.starving
                && !ramp.paid(limit, departures)
                && self.residence > raise_bound
        });
        if let Some(ramp) = useless {
            self.plateau = Some(ramp.departures);
        }
        let target = if stuck && (self.starving || drain_bound.is_infinite()) && may_cut {
            // nothing else would cut: the bound only acts on a busy bottleneck, once known
            0
        } else if let Some(ramp) = useless {
            ramp.last
        } else if self.residence > self.bound && !self.starving && may_cut {
            // keep what leaves within the bound. Not while the bottleneck waits: then nothing
            // queues, and a long residence is upstream latency, which a cut can't shorten.
            // Never above the limit: while a cut's permits retire, more can be in flight
            ((departures * drain_bound) as usize).min(limit)
        } else if self.starving
            && self.saturated
            && may_raise
            && self.residence <= raise_bound
            && self.plateau.is_none()
            // raise again within a ramp only once the latest raise has paid
            && self.ramp.is_none_or(|ramp| ramp.paid(limit, departures))
            && self.succeeded
            && window.mostly_succeeded()
        {
            (limit as f64 * c.growth).ceil() as usize
        } else {
            limit
        }
        .clamp(c.floor, c.max);

        // 6. Hold the next change until this one shows.
        if target != limit {
            self.settled = Tally::default();
            self.succeeded = false;
        }
        if target > limit {
            // the new items reach the bottleneck, and start leaving, about one pass from now
            let pass = self.pass_estimate().unwrap_or(c.max_settle);
            self.raise_after = sample.at.checked_add(pass.min(c.max_settle));
            // a raise within a ramp extends it
            let from = self.ramp.map_or(limit, |ramp| ramp.from);
            self.ramp = sample.at.checked_add(pass).map(|until| Ramp {
                from: from.max(1),
                last: limit.max(1),
                departures,
                checked: self.pass.is_some() || self.residence < c.max_window.as_secs_f64(),
                until,
            });
        } else if target < limit {
            self.ramp = None;
            self.raise_after = None;
            // until the excess leaves at the current rate
            let hold = match sample.in_flight.saturating_sub(target) {
                0 => Duration::ZERO,
                excess => Duration::try_from_secs_f64(excess as f64 / departures)
                    .map_or(c.max_settle, |hold| hold.min(c.max_settle)),
            };
            self.cut_after = sample.at.checked_add(hold);
        }
        target
    }

    fn diagnostics(&self) -> Diagnostics {
        let window = &self.window.total;
        let mut d = Diagnostics::default();
        d.push("throughput", window.throughput());
        d.push("departures", window.departures());
        d.push("residence", self.residence);
        d.push("fastest", self.fastest);
        d.push("bound", self.bound);
        d.push("window", window.elapsed.as_secs_f64());
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
