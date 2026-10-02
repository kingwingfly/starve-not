//! The recommended policy: grow while the bottleneck waits, but keep shutdown quick.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use bon::bon;

#[cfg(feature = "diagnostics")]
use super::Diagnostics;
use super::Policy;
use crate::Sample;

/// Raises the limit while the bottleneck waits for input, and lowers it when the items inside
/// would take too long to finish.
///
/// This is the policy to start with. It needs an [`IdleProbe`](crate::IdleProbe) on the
/// bottleneck: the probe is how it learns that the bottleneck waits. Without one it never raises
/// the limit.
///
/// # What it aims for
///
/// ```text
///              limit: how many items may be inside at once
///          |<------------------------------------------------->|
/// items -> gate -> upstream -> queue -> bottleneck -> ... -> done
///          |<------------------------------------------------->|
///              residence: how long an item stays inside
/// ```
///
/// It has two goals, which pull in opposite directions:
///
/// 1. **The bottleneck never waits for input.** Its probe tells when it does. More items inside
///    feed it better.
/// 2. **Items don't stay inside too long.** Residence is also about how long a clean shutdown
///    takes, so it should stay within the *bound*, about [`drain_target`]. Fewer items inside
///    keep it short.
///
/// The limit only decides how fast upstream is fed. From the bottleneck on, everything runs at
/// its own pace. A ticket is held until its item is done, so time spent after the bottleneck
/// counts too.
///
/// Residence is an average, measured from how many items are inside and how many leave. Some
/// items take longer, so the bound isn't a deadline for each item or for shutdown. For a hard cap
/// on how many items are inside, set [`max`].
///
/// # Every tick
///
/// ```text
/// is the pipeline stuck? --------------------------- yes -> drop to `floor`
///   | no
/// did a raise's trial end without it helping? ------ yes -> take it back, if it was a retry or
///   | no                                                   residence is past the bound
/// bottleneck busy, residence past the bound? ------- yes -> cut to what leaves in time
///   | no
/// bottleneck waiting, gate nearly full, and safe? -- yes -> raise: limit x `growth`
///   | no
/// keep the limit
/// ```
///
/// In a typical run, the limit doubles while the bottleneck waits, until residence nears the
/// bound or a raise stops helping:
///
/// ```text
/// limit
///  32 |                  +-----+
///     |                  |     |
///  16 |            +-----+     +-------------  the raise to 32 didn't help:
///     |            |                           taken back
///   8 |      +-----+
///     |      |
///   4 |------+
///     +-------------------------------------> time
/// ```
///
/// The sections below explain each step.
///
/// # Residence and the bound
///
/// *Residence* is how long an item stays inside: the items inside divided by how many leave per
/// second (Little's law). For the decisions, it is measured over recent ticks: enough of them to
/// see at least [`window_items`] items leave and one pass through the pipeline, but at most
/// [`max_window`], or one pass if that is longer.
///
/// The *bound* is the longest residence allowed:
///
/// ```text
/// bound = the larger of:  drain_target
///                         max_slowdown x fastest
/// ```
///
/// *Fastest* is the shortest residence measured while the gate was nearly full. Usually
/// [`drain_target`] is the larger. [`max_slowdown`] takes over for pipelines that are slow by
/// nature, so they aren't squeezed below what they need.
///
/// - Until the fastest time is known there is no bound: a long residence may just be the
///   pipeline filling up. Raises then stop at [`drain_target`], and nothing is cut by the bound.
/// - During a raise's trial, the new items are inside but haven't left yet, so residence reads
///   high, by up to [`growth`] times. The bound is widened by as much as the limit was raised.
///
/// Queueing only adds to residence, so the fastest time only goes down. To accept an upstream
/// that got slower for good, it creeps up by [`fastest_decay`] while the bottleneck waits, since
/// then nothing queues for it. Not while raises wait after one was taken back, or before the
/// retry that follows (see below): that raise may have shown a queue where no probe sees. When
/// the pipeline gets stuck and drops to [`floor`], the fastest time is learned again from scratch,
/// since a stall usually means the timing changed.
///
/// # Raising
///
/// The limit is multiplied by [`growth`] when the bottleneck waits for input (more than
/// [`starving_share`] of a tick) while the gate is nearly full (90%), and only if:
///
/// - residence is within the bound: more items can't make them leave sooner. Only a retry may
///   go past it (see below);
/// - items succeed: at least one since the last change, and at least half of those leaving
///   lately. Items that fail fast, or hang, would otherwise look like a hungry bottleneck;
/// - the previous raise had time to show: one pass, at most [`max_settle`]. If it is still on
///   trial, departures must have risen with it already. A retry, and every raise after one was
///   taken back until one helps, waits for its trial to end instead;
/// - raises aren't waiting after one was taken back.
///
/// # Checking a raise
///
/// Every raise goes on *trial*. The trial first lets as many items leave as were inside at the
/// raise, so that what it counts next is mostly items let in after it. Then it counts what
/// leaves, for as long as it takes to trust: at least [`window_items`] items over at least one
/// pass. With few items that can take longer than [`max_window`].
///
/// A waiting bottleneck turns more items into more departures, so departures should rise by at
/// least half as much as the limit did, and by more than chance:
///
/// ```text
/// raise -> as many leave as were inside -> count what leaves
///                                              |
///            rose with the limit? ------------ yes -> helped: keep it
///                                              | no
///            clearly didn't rise? ------------ yes -> useless
///                                              | no
///            keep counting; at the time limit, it couldn't tell
/// ```
///
/// A raise that didn't help is taken back if residence is past the bound while the bottleneck
/// waits: it only lengthened a queue where no probe sees, such as behind the bottleneck. If
/// residence is within the bound, it stays: it does no harm. A retry is taken back unless it
/// helped.
///
/// - The time limit is eight times the longest of: how long [`window_items`] items take to leave,
///   one pass, and [`max_settle`].
/// - A trial also stops early when the pace before the raise was too uneven to show the rise
///   needed, and the gain so far is small: counting more after the raise can't fix that.
/// - A retry must beat the needed rise by the margin for chance, not just reach it. It is
///   judged at doubling intervals, not on every tick.
/// - Raises made before the fastest time is known aren't judged: the pipeline is still filling.
///
/// How much departures vary by chance is estimated from how unevenly they fall across ticks,
/// corrected by what the policy learns while raises wait (see below): little for a stage that
/// works like clockwork, a lot for one that sends items out in batches. Counting by ticks also
/// keeps the decisions the same whatever units [weighted items](crate::Gate#weighted-items) are
/// counted in, as long as the count settings are scaled with them.
///
/// # After a raise was taken back
///
/// Under the same conditions, the same raise would be just as useless. So raises wait until the
/// *pace* changes, that is, until whatever holds the items up gets faster or slower. The policy
/// measures how many items leave per second at the restored limit, in stretches of at least
/// [`window_items`] items and one pass, and compares each stretch with the pace before the raise
/// and the stretches since:
///
/// ```text
/// stretch:  1         2       3                4
/// pace:     baseline  same    faster           faster
///                             ^ maybe chance   ^ twice in a row: the pace changed,
///                                                raises may try again
/// ```
///
/// - A stretch must differ by more than chance, and by at least 10%. Chance rarely makes two
///   stretches in a row differ the same way, so it takes two.
/// - A cut starts the measuring over at its new limit, but the wait keeps counting toward its
///   end. A cut to [`floor`] ends the wait, unless it takes the raise back: there the limit
///   itself sets the pace, so it would never change.
///
/// The raise may have been judged wrongly, so the wait also ends after a while even if the pace
/// stays the same: after 16 stretches, longer the more the pace varies by chance, and twice as
/// long for each raise taken back in a row, up to 64 times. However few items leave, it ends after
/// [`max_retry`] at the latest. A raise taken back because its trial couldn't tell, though its
/// gain looked big enough, starts over at the shortest wait: it needs a better measurement, not
/// a longer wait.
///
/// When the wait ends, the next raise is a *retry*. It may go past the bound, to find out whether
/// a raise helps now, once the current limit has been measured long enough to judge it against.
/// A retry that doesn't help is taken back, and the next wait is twice as long:
///
/// ```text
/// limit     retry          retry                    retry
///   16 |     +--+           +--+                     +--+
///    8 |-----+  +-----------+  +---------------------+  +------------->
///       wait      wait x 2            wait x 4, at most max_retry
/// ```
///
/// # Lowering
///
/// More items only make items slower once the bottleneck is busy: then they queue in front of
/// it, and residence rises. So when residence passes the bound while the bottleneck is busy,
/// the limit is cut to what leaves within the bound (departures per second x the bound). The
/// next cut waits until the excess has left, at most [`max_settle`].
///
/// While the bottleneck waits, nothing queues in front of it: a long residence is upstream
/// latency, or a queue the probe can't see. A cut would only starve the bottleneck, so then the
/// limit only comes down to take a raise back, or when the pipeline is stuck.
///
/// When nothing has left for longer than [`max_window`], and for twice the longest gap seen
/// before, the pipeline is *stuck*. If the bottleneck waits, or no bound is known yet, the limit
/// drops to [`floor`].
///
/// # Settings at a glance
///
/// | Setting | What it shapes |
/// |---|---|
/// | [`drain_target`], [`max_slowdown`] | the bound: how long items may stay inside |
/// | [`growth`] | how much each raise multiplies the limit by |
/// | [`starving_share`] | how much waiting counts as the bottleneck waiting |
/// | [`window_items`], [`max_window`] | how much is measured before it is trusted |
/// | [`max_settle`] | the longest wait before changing the limit the same way again |
/// | [`max_retry`] | the longest wait before retrying a raise that was taken back |
/// | [`fastest_decay`] | how fast an upstream that got slower is accepted |
/// | [`floor`], [`max`], [`initial`] | where the limit may go, and where it starts |
///
/// [`floor`]: DrainBoundedBuilder::floor
/// [`max`]: DrainBoundedBuilder::max
/// [`initial`]: DrainBoundedBuilder::initial
/// [`starving_share`]: DrainBoundedBuilder::starving_share
/// [`growth`]: DrainBoundedBuilder::growth
/// [`drain_target`]: DrainBoundedBuilder::drain_target
/// [`max_slowdown`]: DrainBoundedBuilder::max_slowdown
/// [`max_settle`]: DrainBoundedBuilder::max_settle
/// [`max_retry`]: DrainBoundedBuilder::max_retry
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
/// With the `diagnostics` feature, each decision reports `completion_rate` (items finished per
/// second), `departure_rate` (items leaving per second, finished or not), `residence` (seconds an
/// item stays inside), `fastest` (the fastest residence seen), `bound` (the residence allowed
/// right now), `window` (the seconds the window spans), four flags that are 1 or 0:
/// `is_starving` (the bottleneck was waiting), `is_saturated` (the gate was nearly full),
/// `is_raise_useless` (raises wait after a useless raise was taken back) and
/// `is_raise_inconclusive` (raises wait after a raise was taken back because its trial couldn't
/// tell), and `pace_dispersion` (how much more the pace varies than its ticks alone suggest).
#[derive(Debug, Clone)]
pub struct DrainBounded {
    config: Config,
    /// recent ticks: residence and rates for the decisions
    window: Window,
    /// everything since the pipeline last settled: where the fastest time is learned
    settled: Tally,
    /// whether any item has left yet; until then the pipeline is still filling
    is_filled: bool,
    /// when an item last left (at first, when the first tick started)
    last_departure: Option<Instant>,
    /// the longest time without departures so far
    longest_silence: Duration,
    /// whether an item has succeeded since the limit last changed
    is_succeeding: bool,
    /// items in flight at the previous sample
    last_in_flight: Option<usize>,
    /// lowest residence seen, in seconds
    fastest: f64,
    /// one pass through the pipeline, once the fastest time is known
    pass: Option<Duration>,
    /// raises being tried: whether they help isn't known yet
    raise_trial: Option<RaiseTrial>,
    /// set when a raise was taken back: raises wait
    raise_wait: Option<RaiseWait>,
    /// how much the pace varies by chance, learned while raises wait
    pace_dispersion: Dispersion,
    /// raises taken back in a row without a likely gain: each doubles the next wait
    taken_back_in_a_row: u32,
    /// the wait is over: the next raise is a retry, which may go past the bound
    is_retry_due: bool,
    /// no raise before this: the latest raise's items haven't reached the bottleneck
    raise_after: Option<Instant>,
    /// no cut before this: the latest cut's excess hasn't left
    cut_after: Option<Instant>,
    /// seconds an item stays inside, at the latest decision
    residence: f64,
    /// the latest decision's other inputs
    #[cfg(feature = "diagnostics")]
    inputs: Inputs,
}

/// What [`DrainBounded`] decided from, besides the residence, kept for its diagnostics.
#[cfg(feature = "diagnostics")]
#[derive(Debug, Clone)]
struct Inputs {
    bound: f64,
    is_starving: bool,
    is_saturated: bool,
}

/// One or more raises in a row, being tried. Once enough of what leaves after the latest raise
/// is counted, the trial ends: the raise is kept, or taken back if it didn't help.
#[derive(Debug, Clone, Copy)]
struct RaiseTrial {
    /// the limit before the first of these raises
    before_first: usize,
    /// the limit before the latest raise
    before_latest: usize,
    /// what to judge the latest raise against: the pace before it
    baseline: Tally,
    /// whether the pass was known at the latest raise; if not, the pipeline is still filling, and
    /// the raise isn't judged
    is_checkable: bool,
    /// a retry after a raise was taken back: no raise may follow it during its trial, and it is
    /// kept only if it clearly helps
    is_retry: bool,
    /// about when the latest raise's items start leaving
    leaving_from: Instant,
    /// items inside at the latest raise, still to leave before counting starts
    ahead: u64,
    /// what left after them: the pace at the new limit
    after: Tally,
    /// time since the latest raise
    elapsed: Duration,
    /// when to give up: the trial ends then, even if it can't tell
    timeout: Duration,
    /// when to judge a retry next, in counted time; doubles each time
    check_after: Duration,
}

/// What a raise trial shows about the latest raise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// departures rose with it, by more than chance
    Helpful,
    /// departures clearly didn't rise with it: the items queue where no probe sees
    Useless,
    /// too few departures, or too noisy, to tell
    Inconclusive,
}

impl RaiseTrial {
    /// The factor the departure rate should rise by with the latest raise: at least half as much
    /// as the limit did. A waiting bottleneck gets more work from more items, so departures
    /// follow the limit.
    fn needed_rise(&self, limit: usize) -> f64 {
        1.0 + (limit as f64 / self.before_latest as f64 - 1.0) / 2.0
    }

    /// What the departures in `measured` show about the latest raise, against its baseline. It
    /// helps if they rose by the needed factor and by `noise` times the chance; a retry must beat
    /// the needed factor by that much.
    fn verdict(
        &self,
        limit: usize,
        measured: &Tally,
        dispersion: &Dispersion,
        noise: f64,
    ) -> Verdict {
        if self.baseline.departed == 0 || measured.departed == 0 {
            return Verdict::Inconclusive;
        }
        // as logs of ratios, so twice as fast and half as fast count the same
        let needed = self.needed_rise(limit).ln();
        let rose = (measured.departure_rate() / self.baseline.departure_rate()).ln();
        let chance = measured.rate_wobble(&self.baseline) * dispersion.value().sqrt();
        let helpful = match self.is_retry {
            true => needed + noise * chance,
            false => needed.max(noise * chance),
        };
        if rose >= helpful {
            Verdict::Helpful
        } else if rose + noise * chance < needed {
            Verdict::Useless
        } else {
            Verdict::Inconclusive
        }
    }
}

/// How far departures must rise beyond chance, in multiples of it, for another raise to follow
/// one still on trial. Lower than [`PACE_NOISE`], which judges a raise when its trial ends: a
/// higher bar would slow down every climb while few items have left, and a wrong guess here only
/// lets one more raise be judged.
const RAISE_NOISE: f64 = 1.0;

/// How far apart two paces must be to differ, in multiples of how far apart chance alone would
/// put them. Also the margin a raise trial's verdict uses. A change of pace also takes two
/// stretches in a row.
const PACE_NOISE: f64 = 2.0;

/// The smallest change of pace that counts, however many items it was measured from, as a
/// fraction: 0.1 is 10% faster or slower. A smaller change isn't worth trying a useless raise
/// again.
const MIN_PACE_CHANGE: f64 = 0.1;

/// How many comparisons' worth of weight the dispersion's starting value of 1 gets, against what
/// is measured. A few comparisons can't tell much, so the measurement takes over gradually.
const ASSUMED_COMPARISONS: f64 = 4.0;

/// About how many of the latest comparisons the dispersion is measured from, so it follows a
/// pipeline that changes how it sends items out.
const DISPERSION_MEMORY: f64 = 50.0;

/// The most the dispersion is taken to be, so slow drift can't stretch waits and thresholds
/// without end.
const MAX_DISPERSION: f64 = 100.0;

/// How many measured stretches raises wait after one was taken back, before a retry even if the
/// pace stays the same. Each raise taken back in a row doubles it, up to `2^MAX_WAIT_DOUBLINGS`
/// times; `max_retry` caps it in time.
const RETRY_STRETCHES: u32 = 16;

/// See [`RETRY_STRETCHES`].
const MAX_WAIT_DOUBLINGS: u32 = 6;

/// Raises wait after one was taken back, until the pace changes or the wait runs out.
#[derive(Debug, Clone, Copy, Default)]
struct RaiseWait {
    /// the pace at the restored limit: every measured stretch not part of a change, added up
    baseline: Tally,
    /// a stretch that differed from the baseline, until the next one tells chance from a change
    differing: Option<Differing>,
    /// the stretch being measured, from after the taken-back items left
    stretch: Tally,
    /// how many stretches have been measured
    stretches: u32,
    /// time since the raise was taken back, even while nothing leaves
    elapsed: Duration,
    /// the raise was taken back because its trial couldn't tell, not because it was useless
    is_inconclusive: bool,
}

/// A stretch whose pace differed from the baseline by more than chance.
#[derive(Debug, Clone, Copy)]
struct Differing {
    stretch: Tally,
    /// how far apart its pace was from the baseline's, in units of chance: above 0 if faster
    gap: f64,
}

/// How much more the pace varies than its ticks alone suggest, learned while raises wait: near 0
/// for a stage that works like clockwork, larger for one that sends out batches spanning ticks.
/// It belongs to the pipeline, not to one wait, so it is kept across them.
#[derive(Debug, Clone, Copy, Default)]
struct Dispersion {
    /// each comparison's squared gap, in units of chance, added up with older ones fading
    squares: f64,
    /// how many comparisons that is, fading the same way
    comparisons: f64,
}

impl Dispersion {
    /// Count a comparison of two paces that turned out not to be a change of pace.
    fn add(&mut self, gap: f64) {
        let keep = 1.0 - 1.0 / DISPERSION_MEMORY;
        self.squares = self.squares * keep + gap * gap;
        self.comparisons = self.comparisons * keep + 1.0;
    }

    /// The dispersion measured so far, leaning on 1 while there are few comparisons.
    fn value(&self) -> f64 {
        let measured =
            (ASSUMED_COMPARISONS + self.squares) / (ASSUMED_COMPARISONS + self.comparisons);
        measured.min(MAX_DISPERSION)
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
    ticks: u64,
    /// each tick's departures, squared, added up: how unevenly they fall across ticks
    squared_departures: u128,
}

impl Tally {
    fn add(&mut self, other: &Tally) {
        self.elapsed += other.elapsed;
        self.occupancy += other.occupancy;
        self.departed += other.departed;
        self.completed += other.completed;
        self.ticks += other.ticks;
        self.squared_departures += other.squared_departures;
    }

    /// Remove a part of this tally, such as its oldest tick.
    fn sub(&mut self, part: &Tally) {
        self.elapsed -= part.elapsed;
        self.occupancy -= part.occupancy;
        self.departed -= part.departed;
        self.completed -= part.completed;
        self.ticks -= part.ticks;
        self.squared_departures -= part.squared_departures;
    }

    /// Seconds an item stays inside on average, by Little's law.
    fn residence(&self) -> f64 {
        match (self.departed, self.occupancy) {
            // nothing was inside: idle, not stuck
            (_, 0) => 0.0,
            (0, _) => f64::INFINITY,
            (departed, occupancy) => occupancy as f64 / 1e9 / departed as f64,
        }
    }

    /// Whether this tally is enough to trust its rates and residence. It needs both:
    /// - at least `min_departures` items left, so a few early or late items don't skew it;
    /// - it spans at least one residence, so the items inside at its start had time to leave on
    ///   average. A shorter tally may see only part of a pass, such as one burst of departures.
    fn is_trustworthy(&self, min_departures: u64) -> bool {
        self.departed >= min_departures && self.elapsed.as_secs_f64() >= self.residence()
    }

    /// Items finished per second.
    fn completion_rate(&self) -> f64 {
        match self.elapsed.is_zero() {
            true => 0.0,
            false => self.completed as f64 / self.elapsed.as_secs_f64(),
        }
    }

    /// Items leaving per second, finished or not.
    fn departure_rate(&self) -> f64 {
        match self.elapsed.is_zero() {
            true => 0.0,
            false => self.departed as f64 / self.elapsed.as_secs_f64(),
        }
    }

    /// Whether at least half of the items that left succeeded.
    fn is_mostly_successful(&self) -> bool {
        self.completed >= self.departed.div_ceil(2)
    }

    /// How much the departure rate could be off by chance, relative and squared, as if each
    /// tick's departures left as one batch. It depends on how evenly they fall across ticks, not
    /// on the units items are weighed in.
    fn rate_variance(&self) -> f64 {
        if self.ticks < 2 || self.departed == 0 {
            return f64::INFINITY;
        }
        self.squared_departures as f64 / (self.departed as f64).powi(2)
    }

    /// How far apart this departure rate and `other`'s would be by chance, as the log of their
    /// ratio. The dispersion corrects it to how items really leave.
    fn rate_wobble(&self, other: &Tally) -> f64 {
        (self.rate_variance() + other.rate_variance()).sqrt()
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
            let is_enough = rest.departed >= items && rest.elapsed >= min;
            if !is_enough && rest.elapsed < max {
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
        /// or less, `window_items` is 0, `max_window` or `max_retry` is 0, `starving_share` is
        /// below 0 or at least 1, or `fastest_decay` is below 1. Such values would quietly keep
        /// the limit from ever growing, or stick it at the floor.
    }))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        /// The lowest the limit goes. Default: 1.
        ///
        /// Two batches is a good choice: the bottleneck works on one while the next waits.
        #[builder(default = 1)]
        floor: usize,
        /// The highest the limit goes. Default: `usize::MAX`, that is, no cap.
        ///
        /// The limit is mostly kept down by `drain_target`. Set this too if memory needs a hard
        /// cap, for example a number of items, or megabytes for weighted items. If set below
        /// `floor`, `floor` is used.
        #[builder(default = usize::MAX)]
        max: usize,
        /// The limit to start with. Default: `floor`. Kept between `floor` and `max`.
        initial: Option<usize>,
        /// How long the policy aims for items to stay inside the pipeline, on average, which is
        /// also about how long a clean shutdown takes. Default: 10 seconds.
        ///
        /// For most pipelines this works as a cap: while the bottleneck is busy, the limit comes
        /// down whenever items would take longer. It is an average, so some items, and so a
        /// shutdown, can take longer. The exception is a pipeline where items take longer than
        /// this even at their fastest. Capping it here would starve the bottleneck, so the policy
        /// allows [`max_slowdown`](Self::max_slowdown) times the fastest time observed instead.
        ///
        /// For example, if the bottleneck handles 50 items per second, a 10-second target
        /// keeps at most about 500 items inside: the bottleneck gets through them in 10 seconds.
        #[builder(default = Duration::from_secs(10))]
        drain_target: Duration,
        /// How many times slower than their fastest observed time items may get before the
        /// limit comes down. Default: 1.5, so items may take up to 50% longer. Must be at least 1.
        ///
        /// For example, say items need 40 seconds even when nothing queues (a slow upstream),
        /// and the bottleneck handles 5 items per second. Keeping it busy takes 5 × 40 = 200
        /// items inside, so a 10-second `drain_target` can't be met (it allows only 5 × 10 = 50
        /// items). With default `max_slowdown = 1.5`, items may take 1.5 × 40 = 60 seconds
        /// instead: at most about 5 × 60 = 300 items inside, the 200 that keep the bottleneck
        /// busy plus 100 queued in front of it.
        ///
        /// The queue is a buffer: upstream times vary, and with only the 200 items, every item
        /// that comes back late leaves the bottleneck waiting. A bigger queue doesn't make the
        /// bottleneck any faster, though. It only makes items wait longer and shutdown take
        /// longer, so keep this small.
        #[builder(default = 1.5)]
        max_slowdown: f64,
        /// What the limit is multiplied by each time it grows. Default: 2. Must be above 1.
        #[builder(default = 2.0)]
        growth: f64,
        /// How many items must leave (finished or not) before the policy trusts what it
        /// measures from them. Default: 20. Must be at least 1.
        ///
        /// The policy measures how fast items leave, and how long they stay inside, over recent
        /// ticks. It looks back as far as it takes to see this many items leave, up to
        /// [`max_window`](Self::max_window). For example, with 5 items leaving per second, 20
        /// items leave in 4 seconds, so it looks back about 4 seconds.
        ///
        /// Higher is steadier, since a few unusually fast or slow items matter less. But it takes
        /// longer to collect, so the policy reacts more slowly when few items leave.
        ///
        /// A raise's trial, and the wait after a raise was taken back, measure on their own for
        /// as long as it takes to see this many, even past `max_window`. With
        /// [weighted items](crate::Gate#weighted-items), this counts weight: scale it with
        /// `floor` and `max` when changing units.
        #[builder(default = 20)]
        window_items: u64,
        /// How far back the policy looks, at most, when measuring how fast items leave.
        /// Default: 30 seconds. Must be non-zero.
        ///
        /// The policy looks back until it has seen [`window_items`](Self::window_items) items
        /// leave. When items are few, that could reach back to a pace that no longer holds, so
        /// this caps it. For example, with the defaults and one item leaving every 3 seconds, 20
        /// items would take 60 seconds; the policy measures over the latest 30 seconds (10
        /// items) instead.
        ///
        /// Once one pass through the pipeline is known to take longer than this, the policy
        /// looks back one pass instead, since a shorter stretch sees only part of it. It counts
        /// whole ticks, so it may also look back up to one tick further.
        ///
        /// It also decides when the pipeline is stuck: nothing has left for longer than this,
        /// and for twice the longest gap seen before. A raise's trial, and the wait after a raise
        /// was taken back, measure on their own and may look further.
        #[builder(default = Duration::from_secs(30))]
        max_window: Duration,
        /// How much of a tick the bottleneck may spend waiting before it counts as starving.
        /// Default: 0.1, that is, 10%.
        ///
        /// At least 0 and below 1. A little waiting is normal (handing over a batch takes some
        /// time), so 0 is rarely a good idea.
        #[builder(default = 0.1)]
        starving_share: f64,
        /// How fast the fastest time may rise, as a factor per tick. Default: 1.05, that is, 5%
        /// per tick. Must be at least 1.
        ///
        /// Each measurement can only lower the fastest time. But if upstream becomes slower for
        /// good, an old fastest time would keep the bound too tight and squeeze the pipeline.
        /// So on each tick where it measures while the bottleneck waits for input, the policy
        /// first raises the fastest time by this factor, then lowers it to the measurement if
        /// that is faster. Only while the bottleneck waits: then nothing queues for it, so a
        /// longer time means upstream got slower, not that items queue. And not while raises wait
        /// after one was taken back, or before the retry that follows: that raise may have shown
        /// a queue where no probe sees.
        ///
        /// For example, if the fastest time is 20 seconds and items now take 30, it reaches 30
        /// after about 9 such ticks (1.05⁹ ≈ 1.55). 1 means it never rises.
        #[builder(default = 1.05)]
        fastest_decay: f64,
        /// The longest the policy waits after changing the limit before it changes it the same
        /// way again. Default: 30 seconds. A raise's trial, or the wait after a raise was taken
        /// back, can hold the next raise longer.
        ///
        /// A change takes time to show. After a raise, the new items need about one pass
        /// through the pipeline to reach the bottleneck. After a cut, the extra items inside
        /// need time to leave. Until then the measurements still reflect the old limit, and
        /// acting on them would raise or cut twice for the same reason. So the policy waits for
        /// that time, but never longer than this. Until it knows how long a pass takes, it goes
        /// by how long items stayed inside lately.
        ///
        /// For example, if one pass takes 8 seconds, the next raise may come 8 seconds after the
        /// last. If a pass takes 2 minutes, it may come after 30 seconds, so a slow pipeline
        /// still adapts at a reasonable pace. A change the other way, such as a cut right after
        /// a raise, doesn't wait.
        #[builder(default = Duration::from_secs(30))]
        max_settle: Duration,
        /// The longest raises wait after one was taken back, before a retry. Default: six hours.
        /// Must be non-zero.
        ///
        /// The wait usually ends sooner: when the pace changes, or after a number of measured
        /// stretches. This caps it in time, even while no items leave, so a raise judged wrongly
        /// is retried in the end. The retry still needs a waiting bottleneck and a nearly full
        /// gate. Lower it to retry sooner, at the cost of more retries, each of which queues more
        /// items for a while.
        #[builder(default = Duration::from_secs(21_600))]
        max_retry: Duration,
    ) -> Self {
        assert!(floor > 0, "floor must be at least 1");
        assert!(max_slowdown >= 1.0, "max_slowdown must be at least 1");
        assert!(growth > 1.0, "growth must be above 1");
        assert!(
            (0.0..1.0).contains(&starving_share),
            "starving_share must be in [0, 1)"
        );
        assert!(window_items > 0, "window_items must be at least 1");
        assert!(!max_window.is_zero(), "max_window must be non-zero");
        assert!(fastest_decay >= 1.0, "fastest_decay must be at least 1");
        assert!(!max_retry.is_zero(), "max_retry must be non-zero");
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
                starving_share,
                fastest_decay,
                max_settle,
                max_retry,
            },
            window: Window::default(),
            settled: Tally::default(),
            is_filled: false,
            last_departure: None,
            longest_silence: Duration::ZERO,
            is_succeeding: false,
            last_in_flight: None,
            fastest: f64::INFINITY,
            pass: None,
            raise_trial: None,
            raise_wait: None,
            pace_dispersion: Dispersion::default(),
            taken_back_in_a_row: 0,
            is_retry_due: false,
            raise_after: None,
            cut_after: None,
            residence: f64::INFINITY,
            #[cfg(feature = "diagnostics")]
            inputs: Inputs {
                bound: f64::INFINITY,
                is_starving: false,
                is_saturated: false,
            },
        }
    }

    /// Items finished per second, averaged over recent ticks.
    pub fn completion_rate(&self) -> f64 {
        self.window.total.completion_rate()
    }

    /// The shortest time an item took to go through the pipeline while the gate was nearly full,
    /// slowly creeping up while the bottleneck waits for input. `None` until one has been
    /// observed, and again after the pipeline got stuck.
    pub fn fastest(&self) -> Option<Duration> {
        Duration::try_from_secs_f64(self.fastest).ok()
    }

    /// One pass through the pipeline, as best known: before the fastest time is known, the
    /// latest residence, which a stall inflates, so at most `max_window`.
    fn pass_estimate(&self) -> Duration {
        let max_window = self.config.max_window;
        let measured = self.residence.min(max_window.as_secs_f64());
        let measured = Duration::try_from_secs_f64(measured).unwrap_or(max_window);
        self.pass.unwrap_or(measured)
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
        // no time passed: nothing to decide from
        if sample.elapsed.is_zero() {
            return limit;
        }

        // 1. Record the tick.
        // only both ends are known: assume in flight changed evenly in between
        let before = self.last_in_flight.unwrap_or(sample.in_flight);
        let ends = before as u128 + sample.in_flight as u128;
        let tick = Tally {
            elapsed: sample.elapsed,
            occupancy: ends * sample.elapsed.as_nanos() / 2,
            departed: sample.completed + sample.released,
            completed: sample.completed,
            ticks: 1,
            squared_departures: ((sample.completed + sample.released) as u128).pow(2),
        };
        self.last_in_flight = Some(sample.in_flight);
        let start = sample.at.checked_sub(sample.elapsed).unwrap_or(sample.at);
        // at least one pass, but only a known pass may go past `max_window`: a stall would
        // stretch an estimated one without end
        let min = self.pass_estimate();
        let max = self
            .pass
            .map_or(c.max_window, |pass| pass.max(c.max_window));
        self.window.push(start, tick, c.window_items, min, max);
        // not while a cut's excess is still leaving: the pipeline hasn't settled yet
        if self.is_filled && before <= limit && sample.in_flight <= limit {
            self.settled.add(&tick);
            if let Some(waiting) = &mut self.raise_wait {
                waiting.stretch.add(&tick);
            }
        }
        // the first departures end the filling, which says nothing about the pace
        self.is_filled |= tick.departed > 0;
        let last_departure = *self.last_departure.get_or_insert(start);
        if tick.departed > 0 {
            let silence = sample.at.saturating_duration_since(last_departure);
            self.longest_silence = self.longest_silence.max(silence);
            self.last_departure = Some(sample.at);
        }
        self.is_succeeding |= tick.completed > 0;
        let window = self.window.total;
        let departure_rate = window.departure_rate();
        self.residence = window.residence();
        let is_starving = sample.idle_shares().any(|share| share > c.starving_share);
        let is_saturated = sample.in_flight as u128 * 10 >= limit as u128 * 9;

        // 2. Follow the raise on trial. First let as many items leave as were inside at the raise,
        // then count what leaves. Judge it from about the end of its first pass, once what was
        // counted is enough to trust; a retry only at doubling intervals. Give up at the time
        // limit.
        let mut ended = None;
        if let Some(trial) = &mut self.raise_trial {
            trial.elapsed = trial.elapsed.saturating_add(sample.elapsed);
            if trial.ahead > 0 {
                // the tick in which the last of them leave isn't counted either
                trial.ahead = trial.ahead.saturating_sub(tick.departed);
            } else {
                trial.after.add(&tick);
            }
            let is_timed_out = trial.elapsed >= trial.timeout;
            let is_due = self
                .window
                .start()
                .is_some_and(|start| start >= trial.leaving_from)
                && (!trial.is_retry || trial.after.elapsed >= trial.check_after);
            if is_due || is_timed_out {
                let is_measured = trial.after.is_trustworthy(c.window_items);
                let verdict = if !trial.is_checkable {
                    Verdict::Helpful
                } else if !is_measured {
                    Verdict::Inconclusive
                } else {
                    trial.verdict(limit, &trial.after, &self.pace_dispersion, PACE_NOISE)
                };
                // a baseline too noisy to show the needed rise: more counting after the raise
                // can't fix that, so a small gain is all the trial will ever show
                let needed = trial.needed_rise(limit);
                let is_baseline_noisy = trial.baseline.rate_variance()
                    * self.pace_dispersion.value()
                    * PACE_NOISE.powi(2)
                    >= needed.ln().powi(2);
                let is_gain_small =
                    trial.after.departure_rate() < trial.baseline.departure_rate() * needed;
                let is_decided =
                    verdict != Verdict::Inconclusive || (is_baseline_noisy && is_gain_small);
                if !trial.is_checkable || (is_measured && is_decided) || is_timed_out {
                    ended = Some((*trial, verdict));
                }
                trial.check_after = trial.check_after.saturating_mul(2);
            }
        }
        if let Some((trial, verdict)) = ended {
            self.raise_trial = None;
            // what was counted is the pace at the new limit: start from it
            if verdict == Verdict::Helpful && trial.after.is_trustworthy(c.window_items) {
                self.settled = trial.after;
            }
        }
        if let Some(waiting) = &mut self.raise_wait {
            waiting.elapsed = waiting.elapsed.saturating_add(sample.elapsed);
        }
        // While raises wait, compare each measured stretch with the pace at the restored limit.
        // Once two in a row differ the same way, the pace changed: a raise may help again.
        // Everything else adds to the baseline, including a stretch that differed by chance, and
        // teaches how much the pace varies by chance.
        if let Some(waiting) = &mut self.raise_wait
            && waiting.stretch.is_trustworthy(c.window_items)
        {
            let stretch = std::mem::take(&mut waiting.stretch);
            waiting.stretches += 1;
            let baseline = waiting.baseline;
            if baseline.departed == 0 {
                waiting.baseline = stretch;
            } else {
                let (old_rate, new_rate) = (baseline.departure_rate(), stretch.departure_rate());
                // as the log of their ratio, so twice as fast and half as fast count the same
                let apart = (new_rate / old_rate).ln();
                let wobble = stretch.rate_wobble(&baseline);
                let chance = wobble * self.pace_dispersion.value().sqrt();
                let threshold = (PACE_NOISE * chance).max(MIN_PACE_CHANGE.ln_1p());
                // in units of chance, to learn the dispersion from once it is known to be chance
                let gap = apart / wobble.max(f64::EPSILON);
                let is_apart = apart.abs() > threshold;
                let earlier = waiting.differing.take();
                if is_apart && earlier.is_some_and(|earlier| (earlier.gap > 0.0) == (gap > 0.0)) {
                    // the second in a row to differ this way: not chance. Measure afresh before
                    // the retry, so it is judged against the new pace
                    self.raise_wait = None;
                    self.is_retry_due = true;
                    self.settled = Tally::default();
                    self.raise_after = sample.at.checked_add(window.elapsed);
                } else {
                    // an earlier one that differed was chance
                    if let Some(earlier) = earlier {
                        waiting.baseline.add(&earlier.stretch);
                        self.pace_dispersion.add(earlier.gap);
                    }
                    if is_apart {
                        // the first to differ, or it differs the other way: wait for the next
                        waiting.differing = Some(Differing { stretch, gap });
                    } else {
                        waiting.baseline.add(&stretch);
                        self.pace_dispersion.add(gap);
                    }
                }
            }
        }
        // the same pace for long enough, or for `max_retry`: retry anyway, in case the raise was
        // judged wrongly. A pace that varies more by chance tells less per stretch, so the wait is
        // longer
        let doublings = self
            .taken_back_in_a_row
            .saturating_sub(1)
            .min(MAX_WAIT_DOUBLINGS);
        let stretches = (RETRY_STRETCHES << doublings) as f64;
        let stretches = stretches * self.pace_dispersion.value().max(1.0);
        if let Some(waiting) = &self.raise_wait
            && (waiting.stretches as f64 >= stretches || waiting.elapsed >= c.max_retry)
        {
            self.raise_wait = None;
            self.is_retry_due = true;
        }

        // 3. Learn the fastest time, from a full gate, and from enough items over a whole pass
        // that mostly succeeded (fast failures aren't the pace, and part of a pass may catch a
        // burst).
        let settled = self.settled.residence();
        if is_saturated
            && self.raise_trial.is_none()
            && self.settled.is_trustworthy(c.window_items)
            && self.settled.is_mostly_successful()
            && settled > 0.0
            && settled.is_finite()
        {
            // creep up only while nothing queues: the bottleneck waits, and no raise taken back is
            // waiting or about to be retried, since that raise may have shown a hidden queue
            if is_starving && self.raise_wait.is_none() && !self.is_retry_due {
                self.fastest *= c.fastest_decay;
            }
            self.fastest = self.fastest.min(settled);
        }

        // 4. The bound: infinite until the fastest time is known, widened during a raise trial.
        let drain_bound = match self.fastest.is_finite() {
            true => (self.fastest * c.max_slowdown).max(c.drain_target.as_secs_f64()),
            false => f64::INFINITY,
        };
        let widen = self.raise_trial.map_or(1.0, |trial| {
            (limit as f64 / trial.before_first as f64).clamp(1.0, c.growth)
        });
        let bound = drain_bound * widen;
        #[cfg(feature = "diagnostics")]
        {
            self.inputs = Inputs {
                bound,
                is_starving,
                is_saturated,
            };
        }
        // raising can't make items leave sooner: never past the bound, or past `drain_target`
        // before the bound is known. Only a retry may go past it
        let raise_bound = match drain_bound.is_finite() {
            true => bound,
            false => c.drain_target.as_secs_f64() * widen,
        };
        // at most the bound, since a stall inflates residence
        self.pass = match drain_bound.is_finite() {
            true => Duration::try_from_secs_f64(self.residence.min(drain_bound)).ok(),
            false => None,
        };

        // 5. Decide.
        // is_stuck: items are inside and have left before, but none for far longer than usual
        let silence = self
            .last_departure
            .map_or(Duration::ZERO, |at| sample.at.saturating_duration_since(at));
        let is_stuck = self.is_filled
            && sample.in_flight > 0
            && silence > c.max_window.max(self.longest_silence.saturating_mul(2));
        let is_cut_allowed = self.cut_after.is_none_or(|after| sample.at >= after);
        let is_raise_allowed = self.raise_after.is_none_or(|after| sample.at >= after);
        // stuck: drop to the floor. Nothing else would cut: the bound only acts on a busy
        // bottleneck, once known
        let is_dropping = is_stuck && (is_starving || drain_bound.is_infinite()) && is_cut_allowed;
        // a raise not shown to help is taken back if it was a retry, or if residence is past the
        // bound while the bottleneck waits: then it only lengthened a queue where no probe sees
        let taken_back = ended.filter(|(trial, verdict)| {
            *verdict != Verdict::Helpful
                && (trial.is_retry
                    || (is_starving && self.residence.max(trial.after.residence()) > raise_bound))
        });
        if let Some((trial, verdict)) = taken_back {
            // a trial that couldn't tell, though the gain looked big enough, needs a better
            // measurement, not a longer wait: start over at the shortest one
            if verdict == Verdict::Inconclusive
                && trial.after.departure_rate()
                    >= trial.baseline.departure_rate() * trial.needed_rise(limit)
            {
                self.taken_back_in_a_row = 0;
            }
            self.taken_back_in_a_row = self.taken_back_in_a_row.saturating_add(1);
            self.raise_wait = Some(RaiseWait {
                baseline: trial.baseline,
                is_inconclusive: verdict == Verdict::Inconclusive,
                ..RaiseWait::default()
            });
        }
        if ended.is_some_and(|(_, verdict)| verdict == Verdict::Helpful) {
            self.taken_back_in_a_row = 0;
        }
        // during a trial, raise again only once the latest raise helped, or if nothing had left
        // before it. Not after a retry, or after a raise was taken back until one helps: then
        // each raise is judged on its own
        let dispersion = &self.pace_dispersion;
        let is_trial_helping = self.raise_trial.is_none_or(|trial| {
            !trial.is_retry
                && self.taken_back_in_a_row == 0
                && (trial.baseline.departed == 0
                    || trial.verdict(limit, &window, dispersion, RAISE_NOISE) == Verdict::Helpful)
        });
        let target = if is_dropping {
            0
        } else if let Some((trial, _)) = taken_back {
            trial.before_latest
        } else if self.residence > bound && !is_starving && is_cut_allowed {
            // keep what leaves within the bound. Not while the bottleneck waits: then nothing
            // queues, and a long residence is upstream latency, which a cut can't shorten.
            // Never above the limit: while a cut's permits retire, more can be in flight
            ((departure_rate * drain_bound) as usize).min(limit)
        } else if is_starving
            && is_saturated
            && is_raise_allowed
            // past the bound only for a retry, and only once this limit was measured long enough
            // to judge the retry against
            && (self.residence <= raise_bound || self.is_retry_due)
            && self.raise_wait.is_none()
            && (!self.is_retry_due || self.settled.is_trustworthy(c.window_items))
            && is_trial_helping
            && self.is_succeeding
            && window.is_mostly_successful()
        {
            (limit as f64 * c.growth).ceil() as usize
        } else {
            limit
        }
        .clamp(c.floor, c.max);

        // what to judge a raise against: the pace at this limit since it settled, or the recent
        // window if that isn't enough yet
        let baseline = if self.settled.is_trustworthy(c.window_items) {
            self.settled
        } else {
            window
        };
        // 6. Hold the next change until this one shows.
        if target != limit {
            self.settled = Tally::default();
            self.is_succeeding = false;
        }
        if target > limit {
            // the new items reach the bottleneck, and start leaving, about one pass from now
            let pass = self.pass_estimate();
            self.raise_after = sample.at.checked_add(pass.min(c.max_settle));
            // a raise during a trial joins it
            let before_first = self.raise_trial.map_or(limit, |trial| trial.before_first);
            // about how long `window_items` items take to leave, at the pace so far
            let measure =
                Duration::try_from_secs_f64(c.window_items as f64 / baseline.departure_rate())
                    .unwrap_or(c.max_window);
            self.raise_trial = sample.at.checked_add(pass).map(|leaving_from| RaiseTrial {
                before_first: before_first.max(1),
                before_latest: limit.max(1),
                baseline,
                is_checkable: self.pass.is_some(),
                is_retry: self.is_retry_due,
                leaving_from,
                ahead: sample.in_flight as u64,
                after: Tally::default(),
                elapsed: Duration::ZERO,
                check_after: measure.max(sample.elapsed),
                // eight times the longest of that, one pass, and `max_settle`
                timeout: measure.max(pass).max(c.max_settle).saturating_mul(8),
            });
            self.is_retry_due = false;
        } else if target < limit {
            self.raise_trial = None;
            self.raise_after = None;
            // the pace at the old limit no longer holds: measure it again, still counting toward
            // the wait's end. Not for a raise just taken back: its wait just began. A cut to the
            // floor, such as when the pipeline gets stuck, ends the wait instead: there the limit
            // sets the pace, so it would never change
            let is_floor_cut = target == c.floor && (is_dropping || taken_back.is_none());
            if is_floor_cut {
                self.raise_wait = None;
                self.taken_back_in_a_row = 0;
                self.is_retry_due = false;
                // a stall usually means the timing changed: learn it again
                if is_dropping {
                    self.window = Window::default();
                    self.fastest = f64::INFINITY;
                    self.pass = None;
                    self.is_filled = false;
                }
            } else if let Some(waiting) = self.raise_wait
                && taken_back.is_none()
            {
                self.raise_wait = Some(RaiseWait {
                    stretches: waiting.stretches,
                    elapsed: waiting.elapsed,
                    is_inconclusive: waiting.is_inconclusive,
                    ..RaiseWait::default()
                });
            }
            // until the excess leaves at the current rate
            let hold = match sample.in_flight.saturating_sub(target) {
                0 => Duration::ZERO,
                excess => Duration::try_from_secs_f64(excess as f64 / departure_rate)
                    .map_or(c.max_settle, |hold| hold.min(c.max_settle)),
            };
            self.cut_after = sample.at.checked_add(hold);
        }
        target
    }

    #[cfg(feature = "diagnostics")]
    fn diagnostics(&self) -> Diagnostics {
        let window = &self.window.total;
        let i = &self.inputs;
        let mut d = Diagnostics::default();
        d.push("completion_rate", window.completion_rate());
        d.push("departure_rate", window.departure_rate());
        d.push("residence", self.residence);
        d.push("fastest", self.fastest);
        d.push("bound", i.bound);
        d.push("window", window.elapsed.as_secs_f64());
        d.push_flag("is_starving", i.is_starving);
        d.push_flag("is_saturated", i.is_saturated);
        d.push_flag(
            "is_raise_useless",
            self.raise_wait.is_some_and(|wait| !wait.is_inconclusive),
        );
        d.push_flag(
            "is_raise_inconclusive",
            self.raise_wait.is_some_and(|wait| wait.is_inconclusive),
        );
        d.push("pace_dispersion", self.pace_dispersion.value());
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
    starving_share: f64,
    fastest_decay: f64,
    max_settle: Duration,
    max_retry: Duration,
}
