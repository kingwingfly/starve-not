//! Adaptive admission using bottleneck idle time and estimated average residence.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use bon::bon;

#[cfg(feature = "diagnostics")]
use super::Diagnostics;
use super::Policy;
use crate::Sample;

/// Raises the limit while the bottleneck starves, and lowers it when items stay inside too long.
///
/// It needs an [`IdleProbe`](crate::IdleProbe) on the bottleneck. Without one it never raises
/// the limit.
///
/// # Model
///
/// ```text
///              limit: how many items may be inside at once
///          |<------------------------------------------------->|
/// items -> gate -> upstream -> queue -> bottleneck -> ... -> done
///          |<------------------------------------------------->|
///              residence: how long an item stays inside
/// ```
///
/// Two goals pull the limit in opposite directions:
///
/// 1. **Keep the bottleneck busy.** While its probe says it waits for input, raise the limit.
/// 2. **Avoid needless queues.** Keep residence within the *bound*.
///
/// ```text
/// residence = items inside / items leaving per second     (Little's law, over recent ticks)
/// bound     = max(drain_target, max_slowdown x fastest)
/// ```
///
/// *Fastest* is the lowest residence seen while the gate was nearly full. Usually
/// [`drain_target`] sets the bound; [`max_slowdown`] takes over for pipelines that are slow by
/// nature. Measurements only lower fastest, so to follow an upstream that got slower for good,
/// it creeps up by [`fastest_decay`] on each tick the bottleneck starves.
///
/// Residence is an average, so the bound is a target, not a deadline for items or shutdown.
/// For a hard cap, set [`max`].
///
/// # Each tick
///
/// The first rule that matches sets the limit:
///
/// ```text
/// stuck, and the bottleneck starves? ------ yes -> `floor`, and learn fastest again
///   | no
/// a raise didn't help, residence > bound? - yes -> take it back
///   | no
/// bottleneck busy, residence > bound? ----- yes -> cut to departures per second x bound
///   | no
/// bottleneck starves, gate >= 90% full,
/// residence <= bound, items succeed? ------ yes -> raise to limit x `growth`
///   | no
/// keep the limit
/// ```
///
/// - *Stuck*: nothing has left for longer than [`max_window`], and for twice the longest gap
///   seen before. With a busy bottleneck, the cut rule handles it instead.
/// - *Items succeed*: one was done since the last change, and at least half of those leaving
///   lately were.
/// - Cuts need a busy bottleneck: while it starves, nothing queues in front of it, and a long
///   residence is a slow upstream, which a cut can't shorten.
/// - Until fastest is known, at the start and after getting stuck, there is no bound: cuts are
///   off, and raises stop once residence reaches [`drain_target`].
/// - Raises also follow the [raise check](#checking-raises), and changes wait until the last
///   one shows (see [Timing](#timing)).
///
/// In a typical run, the limit doubles while the bottleneck starves, until residence reaches
/// the bound or a raise stops helping:
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
/// # Checking raises
///
/// A starving probe doesn't prove that more items help: something it can't see may hold them
/// up, such as a slower stage after the bottleneck. So every raise is checked, and this state
/// machine decides when raises may happen:
///
/// ```text
///      +---------------------------------------------+
///      v                                             |
/// +--------+   raise    +-------+   helped, or       |
/// | Steady | ---------> | Trial | ---- harmless -----+
/// +--------+            +-------+
///      ^                    | didn't help, and residence > bound:
///      |                    | take it back
///      |                    v
///      |              +----------+
///      |              | Cooldown | <------------------------+
///      |              +----------+                          |
///      |                    | departures per second         |
///      |                    | changed, or time is up        | didn't help:
///      |                    v                               | take it back
///      |    helped      +-------+                           |
///      +--------------- | Retry | --------------------------+
///                       +-------+
/// ```
///
/// - **Steady**: the tick rules apply as they are.
/// - **Trial**: more items should make more leave, so departures per second should rise by at
///   least half as much as the limit (+50% for a doubling), and by more than chance. The trial
///   first lets as many items leave as were inside at the raise, then counts at least
///   [`window_items`] more. Meanwhile, the bound widens by as much as the limit rose, since the
///   new items haven't left yet. A raise that didn't help is kept if residence is still within
///   the bound: it does no harm. If the trial can't tell in time, the raise counts as not
///   helping.
/// - **Cooldown**: no raises, since the same raise would be just as useless under the same
///   conditions. Departures per second at the restored limit are measured over and over, each
///   time from at least [`window_items`] items. Two measurements in a row that differ the same
///   way, by more than chance and by at least 10%, mean the conditions changed. Otherwise time
///   is up after 16 measurements (more if departures are noisy), twice as many after each retry
///   that didn't help, and after [`max_retry_interval`] at most.
/// - **Retry**: once the restored limit is measured, the next raise may go past the bound, since
///   the conditions changed, or the raise was judged wrongly. It's kept only if it clearly
///   helps.
///
/// Shortcuts: raises made before fastest is known aren't checked. While starting up, another
/// raise may join a trial once departures already rose. A cut ends a trial, and a drop to
/// [`floor`] ends a cooldown.
///
/// *Chance* is how much departures per second vary on their own. It's estimated from how many
/// items leave each tick, and corrected during cooldowns for pipelines whose items leave in
/// bursts. The more they vary, the bigger a change must be to count.
///
/// # Timing
///
/// Residence and departures per second are measured over recent ticks: enough to see
/// [`window_items`] items leave and an item go all the way through, but no more than
/// [`max_window`], unless an item takes longer than that.
///
/// A change takes time to show, and until then the measurements still reflect the old limit.
/// So after a raise, the next raise waits about one residence, for the new items to reach the
/// bottleneck. After a cut, the next cut waits for the excess items to leave. Both waits are at
/// most [`max_settle`]. A change the other way doesn't wait.
///
/// # Tuning
///
/// Start with the defaults. These are the settings most worth a look:
///
/// | Setting | Change it to |
/// |---|---|
/// | [`drain_target`] | queue less (lower), or allow more inside (higher) |
/// | [`floor`] | two batches, if the bottleneck takes items in batches |
/// | [`max`] | put a hard cap on items, or on memory with weighted items |
/// | [`max_slowdown`] | give pipelines that are slow by nature a bigger buffer (higher) |
/// | [`max_retry_interval`] | retry sooner after a raise was taken back (lower) |
///
/// The others ([`initial`], [`growth`], [`starving_share`], [`window_items`], [`max_window`],
/// [`fastest_decay`] and [`max_settle`]) fine-tune how fast it reacts, and how much it measures
/// first.
///
/// # Example
///
/// ```
/// # use std::time::Duration;
/// # use starve_not::DrainBounded;
/// // never below two batches of 16, and aim for items to stay inside about 5 seconds
/// let policy = DrainBounded::builder().floor(32).drain_target(Duration::from_secs(5)).build();
/// ```
///
/// # Diagnostics
///
/// With the `diagnostics` feature, each decision reports:
///
/// - `completion_rate` and `departure_rate`: items done, and items leaving (done or failed), per
///   second;
/// - `residence`, `fastest` and `bound`: as in the [model](#model), in seconds; `bound` includes
///   a trial's widening;
/// - `window`: how many seconds the measurements cover;
/// - `is_starving`: 1 if any probe waited more than [`starving_share`] of the tick, else 0;
/// - `is_saturated`: 1 if the gate was at least 90% full, else 0;
/// - `is_raise_useless` and `is_raise_inconclusive`: 1 during a cooldown, after a raise that
///   didn't help or whose trial couldn't tell, else 0;
/// - `pace_dispersion`: how much more departures vary than [chance](#checking-raises) from
///   ticks alone suggests, starting at 1. The larger, the bigger a change must be to count.
///
/// Counts use weights with [weighted items](crate::Gate#weighted-items). Times are infinite
/// until known.
///
/// [`floor`]: DrainBoundedBuilder::floor
/// [`max`]: DrainBoundedBuilder::max
/// [`initial`]: DrainBoundedBuilder::initial
/// [`drain_target`]: DrainBoundedBuilder::drain_target
/// [`max_slowdown`]: DrainBoundedBuilder::max_slowdown
/// [`growth`]: DrainBoundedBuilder::growth
/// [`window_items`]: DrainBoundedBuilder::window_items
/// [`max_window`]: DrainBoundedBuilder::max_window
/// [`starving_share`]: DrainBoundedBuilder::starving_share
/// [`fastest_decay`]: DrainBoundedBuilder::fastest_decay
/// [`max_settle`]: DrainBoundedBuilder::max_settle
/// [`max_retry_interval`]: DrainBoundedBuilder::max_retry_interval
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
    /// bounded timing hint for the control window and settling, once fastest is known
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
    /// end of the settling delay after the latest raise
    raise_after: Option<Instant>,
    /// end of the estimated drain delay after the latest cut
    cut_after: Option<Instant>,
    /// estimated average seconds inside, at the latest decision
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
    /// whether a pass estimate existed at the latest raise; otherwise it remains provisional
    is_checkable: bool,
    /// a retry after a raise was taken back: no raise may follow it during its trial, and it is
    /// kept only if it clearly helps
    is_retry: bool,
    /// about when the latest raise's items start leaving
    leaving_from: Instant,
    /// departures still to skip before measuring; this does not track individual items
    ahead: u64,
    /// cumulative measurements after the skipped departures, independent of the window
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
    /// measured gain met the improvement and noise thresholds
    Helpful,
    /// measured gain fell short of the required improvement, allowing for estimated noise
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

/// Margin in units of estimated noise for comparing paces and judging a raise. This is a
/// heuristic, not a calibrated confidence level. A pace change also needs two stretches in a row.
const PACE_NOISE: f64 = 2.0;

/// The smallest change of pace that counts, however many items it was measured from, as a
/// ratio: 0.1 requires a faster/slower rate ratio above 1.1, whichever rate is higher.
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
/// times; `max_retry_interval` caps it in time.
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
    /// Learn from a comparison not treated as part of a pace change.
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

    /// Estimated average seconds inside, by Little's law; transitions can skew it.
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
    /// - elapsed time reaches estimated average residence, to avoid measuring part of a burst.
    ///
    /// This checks aggregate turnover, not whether all items present at the start have left.
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

    /// Squared relative noise estimate, treating each tick's departures as one batch.
    /// Rescaling permit weights cancels out. Correlation across ticks can still affect it.
    fn rate_variance(&self) -> f64 {
        if self.ticks < 2 || self.departed == 0 {
            return f64::INFINITY;
        }
        self.squared_departures as f64 / (self.departed as f64).powi(2)
    }

    /// Combined noise estimate for the log rate ratio, before the learned correction.
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
        /// or less, `window_items` is 0, `max_window` or `max_retry_interval` is 0,
        /// `starving_share` is below 0 or at least 1, or `fastest_decay` is below 1. Such values
        /// would quietly keep the limit from ever growing, or stick it at the floor.
    }))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        /// The lowest the limit goes. Must be at least 1.
        ///
        /// Two batches is a good choice: the bottleneck works on one while the next waits.
        #[builder(default = 1)]
        floor: usize,
        /// The highest the limit goes.
        ///
        /// The bound already keeps the limit down. Set this for a hard cap, such as on memory. If
        /// below [`floor`](Self::floor), `floor` is used.
        #[builder(default = usize::MAX)]
        max: usize,
        /// The limit to start with, kept between [`floor`](Self::floor) and [`max`](Self::max).
        /// Unset, it starts at `floor`.
        initial: Option<usize>,
        /// The residence to aim for: how long items stay inside, on average.
        ///
        /// While the bottleneck is busy, the limit is cut when residence goes past it. For
        /// example, with 50 items leaving per second, 10 seconds allows about 500 items inside.
        /// Lower it for less queued work and quicker shutdowns. Pipelines that are slow by nature
        /// may take longer: see [`max_slowdown`](Self::max_slowdown).
        #[builder(default = Duration::from_secs(10))]
        drain_target: Duration,
        /// How many times its fastest residence an item may take, on average. Must be at least 1.
        ///
        /// This lets pipelines that are slow by nature go past
        /// [`drain_target`](Self::drain_target): if items take 40 seconds at best, 1.5 allows 60.
        /// The extra is a buffer that keeps the bottleneck fed while upstream speed varies, at
        /// the cost of more queued work.
        #[builder(default = 1.5)]
        max_slowdown: f64,
        /// What the limit is multiplied by on each raise, rounded up. Must be above 1.
        ///
        /// Larger climbs faster, but overshoots more with each raise.
        #[builder(default = 2.0)]
        growth: f64,
        /// How many items must leave before a measurement is trusted. Must be at least 1.
        ///
        /// More smooth out chance, but take longer to collect. Raise checks and cooldowns wait
        /// for this many even past [`max_window`](Self::max_window).
        ///
        /// With [weighted items](crate::Gate#weighted-items), this counts weight: scale it along
        /// with `floor` and `max` when changing units.
        #[builder(default = 20)]
        window_items: u64,
        /// How far back recent measurements may look. Must be non-zero.
        ///
        /// When items leave slowly, seeing [`window_items`](Self::window_items) leave would take
        /// old ticks that may no longer reflect the pipeline. This caps how old. If items take
        /// longer than this to go through, measurements look back that long instead.
        ///
        /// It also decides when the pipeline is stuck: nothing has left for longer than this,
        /// and for twice the longest gap seen before.
        #[builder(default = Duration::from_secs(30))]
        max_window: Duration,
        /// The share of a tick the bottleneck may wait before it counts as starving. At least 0
        /// and below 1.
        ///
        /// A little waiting is normal when handing over work. With several probes, any one is
        /// enough.
        #[builder(default = 0.1)]
        starving_share: f64,
        /// How fast the fastest residence may rise, as a factor per tick. Must be at least 1.
        ///
        /// Measurements only lower the fastest time, so after upstream gets slower for good, the
        /// bound would stay too tight. So on each tick the bottleneck starves, the fastest time is
        /// first raised by this factor, then lowered to what was measured if that is less. 1
        /// turns this off. It is per tick, so it depends on the pacer's
        /// [`tick`](crate::PacerBuilder::tick).
        ///
        /// Not during a trial or a cooldown, or before a retry: there, a long residence may be a
        /// queue the probe can't see.
        #[builder(default = 1.05)]
        fastest_decay: f64,
        /// The longest a change may take to show before the limit changes the same way again.
        ///
        /// After a raise, the new items take about one residence to reach the bottleneck. After
        /// a cut, the excess items take time to leave. Until then the measurements still reflect
        /// the old limit, and acting on them would change it twice for one reason. This caps
        /// that wait, so a slow pipeline still adapts. A raise check may hold the next raise
        /// longer.
        #[builder(default = Duration::from_secs(30))]
        max_settle: Duration,
        /// The longest a [cooldown](DrainBounded#checking-raises) lasts. Must be non-zero.
        ///
        /// A cooldown, after a raise that didn't help was taken back, usually ends sooner: when
        /// departures per second change, or after enough measurements. This caps it in time, so
        /// a raise judged wrongly is tried again in the end. Lower it to retry sooner. Each retry
        /// that doesn't help queues more items for a while.
        #[builder(default = Duration::from_secs(21_600))]
        max_retry_interval: Duration,
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
        assert!(
            !max_retry_interval.is_zero(),
            "max_retry_interval must be non-zero"
        );
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
                max_retry_interval,
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

    /// The lowest residence seen while the gate was nearly full (see the
    /// [model](DrainBounded#model)). `None` until measured, and again after the pipeline got
    /// stuck.
    pub fn fastest(&self) -> Option<Duration> {
        Duration::try_from_secs_f64(self.fastest).ok()
    }

    /// Timing hint for the window and settling delays, not proof that work has completed.
    /// Before fastest is known, use recent residence capped by `max_window`.
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
        // then count what leaves. This reduces overlap with old work but cannot identify it.
        // Once the window is past the estimated settling period and enough is counted, judge
        // the trial; retries only at doubling intervals. Give up at the time limit.
        let mut ended = None;
        if let Some(trial) = &mut self.raise_trial {
            trial.elapsed = trial.elapsed.saturating_add(sample.elapsed);
            if trial.ahead > 0 {
                // discard the whole tick that crosses the departure count
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
                // More observations after the raise cannot reduce baseline uncertainty.
                // End a small-gain trial early if that uncertainty dominates the comparison.
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
                // normalize the gap for learning if no pace change is detected
                let gap = apart / wobble.max(f64::EPSILON);
                let is_apart = apart.abs() > threshold;
                let earlier = waiting.differing.take();
                if is_apart && earlier.is_some_and(|earlier| (earlier.gap > 0.0) == (gap > 0.0)) {
                    // two in a row: treat this as a pace change. Measure afresh before the retry
                    self.raise_wait = None;
                    self.is_retry_due = true;
                    self.settled = Tally::default();
                    self.raise_after = sample.at.checked_add(window.elapsed);
                } else {
                    // the earlier differing stretch did not establish a change
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
        // the same pace for long enough, or for `max_retry_interval`: retry anyway, in case the
        // raise was judged wrongly. A pace that varies more by chance tells less per stretch, so
        // the wait is longer
        let doublings = self
            .taken_back_in_a_row
            .saturating_sub(1)
            .min(MAX_WAIT_DOUBLINGS);
        let stretches = (RETRY_STRETCHES << doublings) as f64;
        let stretches = stretches * self.pace_dispersion.value().max(1.0);
        if let Some(waiting) = &self.raise_wait
            && (waiting.stretches as f64 >= stretches || waiting.elapsed >= c.max_retry_interval)
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
            // A waiting probe can hide a queue elsewhere. Freeze upward learning while a raise
            // taken back is waiting or about to be retried.
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
        // Cap the timing hint when a stall inflates residence. It cannot alone end a checked trial.
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
        // bound while the bottleneck waits. An inconclusive result need not imply a hidden queue.
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
    max_retry_interval: Duration,
}
