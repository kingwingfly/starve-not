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

/// Keeps the bottleneck fed while limiting unnecessary work in flight, using an estimate of
/// average residence time.
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
/// 1. **Keep the bottleneck busy.** When its probe reports waiting for input, more items inside
///    may help feed it.
/// 2. **Avoid unnecessary queues.** Aim for average residence near [`drain_target`], allowing
///    longer for pipelines that are slow by nature. Less queued work also helps shorten shutdown.
///
/// The limit only decides how fast upstream is fed. From the bottleneck on, everything runs at
/// its own pace. A ticket is held until its item is done, so time spent after the bottleneck
/// counts too.
///
/// Residence is estimated from work in flight and departures, not from individual item timings.
/// It can be inaccurate while the pipeline changes, and some items take much longer than the
/// average. The residence bound is a control target, not a deadline for items or shutdown.
/// Set [`max`] to cap the admission limit. Lowering a limit never cancels work already inside.
///
/// # How it behaves
///
/// - **Starting up.** When the bottleneck waits, the gate is nearly full, and items succeed,
///   the policy tries higher limits. Each raise doubles the limit by default.
/// - **The bottleneck gets slower.** When it is busy and residence exceeds the bound, the
///   policy reduces admission to what the pipeline is estimated to clear within that bound.
/// - **Upstream gets slower.** More items may be needed to keep the bottleneck fed. The policy
///   can learn a longer residence target, or retry a raise past an old bound. The target is at
///   least [`drain_target`] and at least [`max_slowdown`] times the fastest measured residence.
/// - **More items don't help.** Sometimes the bottleneck waits, yet letting more items in
///   doesn't make more of them finish: something the probe can't see holds them up, such as a
///   slower stage after the bottleneck. Once timing can be measured, each raise is checked.
///   A raise without enough benefit may be taken back, followed by a cooldown before another
///   attempt. A change in departure rate can end that cooldown early.
/// - **Items fail or hang.** An idle probe alone cannot trigger growth: the policy also needs
///   recent successful departures. Failures count as departures, but not successes.
/// - **The pipeline stalls.** After an unusually long gap in departures, the policy can drop
///   to [`floor`]. A stall that causes this cut also resets the timing estimates.
///
/// Changes take time to show, so each change delays the next one in the same direction. That
/// settling delay is capped by [`max_settle`]. Checking a raise and waiting through a cooldown
/// can take longer; [`max_retry`] caps only the cooldown.
///
/// In a typical run, the limit doubles while the bottleneck waits, until items would take too
/// long or a raise stops helping:
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
/// # Tuning
///
/// Start with the defaults, then consider these settings:
///
/// | Setting | Change it to |
/// |---|---|
/// | [`drain_target`] | target less queued work (lower), or allow more inside (higher) |
/// | [`floor`] | two batches, if the bottleneck takes items in batches |
/// | [`max`] | cap admission in items or weighted permits |
/// | [`max_slowdown`] | give pipelines that are slow by nature a bigger buffer (higher) |
/// | [`max_retry`] | shorten the cooldown after a raise is taken back (lower) |
///
/// The others ([`initial`], [`growth`], [`starving_share`], [`window_items`], [`max_window`],
/// [`max_settle`] and [`fastest_decay`]) fine-tune how quickly it reacts and how much it
/// measures first.
///
/// # How it decides
///
/// You don't need this to use the policy. It explains the decisions, for example when reading
/// the [diagnostics](#diagnostics). Every tick, it asks:
///
/// ```text
/// eligible for a stall cut? ----------------------- yes -> drop to `floor`
///   | no
/// trial ended and must be taken back? ------------- yes -> restore the previous limit
///   | no
/// bottleneck busy, over bound, cut delay passed? --- yes -> cut to what leaves in time
///   | no
/// bottleneck waiting, gate nearly full, and safe? -- yes -> raise: limit x `growth`
///   | no
/// keep the limit
/// ```
///
/// ## Residence and the bound
///
/// *Residence* is estimated average work in flight divided by departures per second, including
/// unsuccessful departures. Work in flight is estimated between samples. The recent window
/// aims to cover [`window_items`] departures and an estimated pass through the pipeline, but
/// stops extending at the larger of [`max_window`] and that pass estimate. It keeps whole ticks,
/// so it may extend up to one tick further.
///
/// The pass estimate is a timing hint: recent residence, capped by the bound once known, or by
/// [`max_window`] before then. It does not prove that any particular item has finished.
/// Measurements used to check raises and learn the fastest time can span longer than the
/// recent window.
///
/// The normal residence *bound* is:
///
/// ```text
/// bound = the larger of:  drain_target
///                         max_slowdown x fastest
/// ```
///
/// *Fastest* is the lowest average residence measured while the gate was nearly full, with
/// enough departures and at least half successful. It can include hidden queue time; it is
/// not the fastest individual item's time.
///
/// - Until fastest is known, cuts by the bound are suspended and raises use [`drain_target`].
/// - During a trial, the bound widens in proportion to the raises being tried, by at most
///   [`growth`] times. Chaining raises does not multiply that allowance again.
/// - Measurements can lower fastest. To learn an upstream that became slower, fastest can
///   creep upward by [`fastest_decay`] while the bottleneck waits. That upward adjustment is
///   suspended during trials, cooldowns, and preparation for a retry.
///
/// ## Raising
///
/// The limit is multiplied by [`growth`] when the bottleneck waits for input (more than
/// [`starving_share`] of a tick) while the gate is nearly full (90%), and only if:
///
/// - residence is within the bound, except for a retry after a cooldown;
/// - items succeed: at least one since the last change, and at least half of those leaving
///   lately;
/// - the previous raise's settling delay has passed, and there is no cooldown.
///
/// During the initial climb, another raise can follow an unfinished trial if recent rates
/// already show improvement, or if nothing had left before that trial began. After a raise has
/// been taken back, trials finish one at a time until one helps. A retry never chains raises.
/// Every limit stays between [`floor`] and [`max`].
///
/// ## Checking a raise
///
/// A raise helps if it increases the departure rate enough: by at least half the limit's
/// fractional increase, and by more than estimated noise. For example, doubling the limit
/// should increase the rate by at least 50%.
///
/// Each *trial* keeps measurements from before the raise as its baseline. It first skips as
/// many departures as were in flight at the raise, including the entire tick that reaches
/// that count. Items can finish out of order, so this reduces overlap with old work without
/// proving that all old items have left. The trial then collects its own measurements, which
/// can extend beyond [`max_window`]. It needs at least [`window_items`] departures and an
/// observation time at least as long as its measured residence before reaching a verdict:
///
/// ```text
/// raise -> as many leave as were inside -> count what leaves
///                                              |
///            left faster, as expected? ------- yes -> helped: keep it
///                                              | no
///            clearly too little improvement? - yes -> useless
///                                              | no
///            otherwise: inconclusive; keep counting until the trial ends
/// ```
///
/// A retry needs a noise margin beyond the required improvement. It is checked at doubling
/// observation intervals, rather than on every tick. Other trials use a lower bar to keep the
/// initial climb responsive. A helpful trial carries its measurements into the next baseline.
///
/// A trial times out after eight times the largest of: the expected time for [`window_items`]
/// departures at the baseline rate, the pass estimate, and [`max_settle`]. If the baseline rate
/// is zero, [`max_window`] supplies the first estimate. An inconclusive trial can also end
/// early when its baseline is too noisy to resolve a small gain. More observations after the
/// raise cannot improve the earlier baseline. Raises made before a pass estimate is available
/// are provisional and are not judged this way.
///
/// When a trial ends without a helpful verdict, a retry is taken back. An ordinary raise is
/// taken back only if the bottleneck waits and residence exceeds the bound in either the
/// recent window or the trial's measurements. Otherwise that raise may stay. Taking it back
/// restores the limit before the latest raise, not before the entire climb. An inconclusive
/// result is not evidence of a hidden queue.
///
/// Noise is estimated from departure totals per tick, treating each tick's departures as one
/// batch, with a correction learned while raises wait. Changing the units of weighted permits
/// does not change that estimate when the count settings are scaled too. The estimate is a
/// heuristic: correlated departures and repeated comparisons can still produce wrong verdicts.
///
/// ## After a raise was taken back
///
/// After taking a raise back, the policy waits before trying again. At the restored limit it
/// measures consecutive, nonoverlapping stretches, each with at least [`window_items`]
/// departures and elapsed time at least as long as measured residence. It compares each
/// stretch with the accumulated baseline:
///
/// ```text
/// measurement:  1         2       3                4
/// rate:         usual     same    faster           faster
///                                 ^ maybe chance   ^ twice in a row: it changed,
///                                                    raises may try again
/// ```
///
/// Two consecutive stretches must differ in the same direction, by more than estimated noise
/// and a rate ratio greater than 1.1 (faster or slower). This is treated as a pace change, and a
/// fresh baseline is collected before retrying. Other stretches add to the baseline and help
/// estimate the noise.
///
/// Even if no change is detected, the cooldown ends after 16 stretches, increased for noisy
/// paces and doubled for consecutive raises taken back, up to six doublings. An inconclusive
/// trial whose measured gain met the required increase starts again at the shortest wait:
/// it needs a better baseline, rather than a longer penalty. [`max_retry`] caps every cooldown
/// in elapsed sample time, even when nothing leaves.
///
/// When the cooldown ends, one *retry* may cross an old residence bound. It still needs a
/// measured baseline, successful departures, a nearly full gate, and a waiting bottleneck.
/// Expiry therefore does not guarantee an immediate retry or a recovery deadline. Recovery
/// needs continuing demand and successful departures at a limit the policy is allowed to use.
///
/// ```text
/// limit     retry          retry                    retry
///   16 |     +--+           +--+                     +--+
///    8 |-----+  +-----------+  +---------------------+  +------------->
///       wait      wait x 2            wait x 4, capped by max_retry
/// ```
///
/// A cut during a cooldown restarts rate measurement at the new limit without restarting the
/// cooldown clock. A cut to [`floor`] ends the cooldown, unless it is taking a trial raise back.
///
/// ## Lowering
///
/// When residence exceeds the bound while the bottleneck is busy, the policy cuts the limit to
/// departure rate × the normal bound, rounded down and kept between [`floor`] and the current
/// limit. It waits for the estimated time to drain the excess before another cut, at most
/// [`max_settle`]. Already admitted work continues.
///
/// While the bottleneck waits, a long residence could be upstream latency or a hidden queue.
/// A cut could starve the bottleneck further, so the policy only cuts then to take a raise back
/// or respond to a stall.
///
/// The pipeline is *stuck* if work remains inside, something has left before, and no departures
/// occur for longer than both [`max_window`] and twice the longest observed gap. If the cut
/// delay has passed and the bottleneck waits (or no bound is known), the limit drops to
/// [`floor`]. When this lowers the limit, the recent window and timing estimates are reset.
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
/// // Never below two batches of 16; target about 5 seconds of average residence.
/// let policy = DrainBounded::builder().floor(32).drain_target(Duration::from_secs(5)).build();
/// ```
///
/// # Diagnostics
///
/// With the `diagnostics` feature, each decision reports:
///
/// - `completion_rate` and `departure_rate`: items finished, and items leaving (finished or
///   not), per second;
/// - `residence`: estimated average seconds inside;
/// - `fastest`: the learned minimum average residence, in seconds, with upward adjustment
///   when allowed;
/// - `bound`: the residence threshold used for this decision, including trial widening;
/// - `window`: seconds covered by the recent control window, not the trial's measurements;
/// - `is_starving`: 1 if any probe exceeded [`starving_share`], else 0;
/// - `is_saturated`: 1 if work in flight was at least 90% of the current limit, else 0;
/// - `is_raise_useless` and `is_raise_inconclusive`: 1 while the policy holds off raising after
///   taking back a raise that didn't help, or one whose check couldn't tell, else 0;
/// - `pace_dispersion`: the learned multiplier for the tick-based noise variance. It starts at
///   1; larger values require stronger evidence of change and longer waits.
///
/// Counts and rates use permit weights for [weighted items](crate::Gate#weighted-items).
/// Timing values can be infinite before enough data arrives or when no work leaves. The two
/// raise flags describe cooldowns, not an active trial's verdict.
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
        /// or less, `window_items` is 0, `max_window` or `max_retry` is 0, `starving_share` is
        /// below 0 or at least 1, or `fastest_decay` is below 1. Such values would quietly keep
        /// the limit from ever growing, or stick it at the floor.
    }))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        /// The lowest the limit goes. Must be at least 1.
        ///
        /// Two batches is a good choice: the bottleneck works on one while the next waits.
        #[builder(default = 1)]
        floor: usize,
        /// The highest admission limit the policy requests.
        ///
        /// Use this for an explicit budget in items or weighted permits. Unlike the residence
        /// target, this caps the requested limit directly. If set below [`floor`](Self::floor),
        /// `floor` is used. Work already admitted continues even after the limit is lowered.
        #[builder(default = usize::MAX)]
        max: usize,
        /// Starting limit, kept between [`floor`](Self::floor) and [`max`](Self::max).
        /// An unset value starts at `floor`.
        initial: Option<usize>,
        /// Target average time inside the pipeline.
        ///
        /// The normal bound is the larger of this and [`max_slowdown`](Self::max_slowdown)
        /// times the fastest measured average residence. When the bottleneck is busy and
        /// residence exceeds the bound, the policy reduces admission. While it waits for input,
        /// long residence alone does not trigger a cut: upstream may simply be slow.
        ///
        /// For example, a steady departure rate of 50 items per second and a 10-second bound
        /// correspond to about 500 items in flight. This estimates an average; it does not set
        /// a deadline for any item or for shutdown.
        #[builder(default = Duration::from_secs(10))]
        drain_target: Duration,
        /// Multiplier on the fastest measured average residence when setting the bound.
        /// Must be at least 1.
        ///
        /// Allows naturally slow pipelines to exceed [`drain_target`](Self::drain_target).
        /// For example, if average residence is at least 40 seconds and throughput is 5 items
        /// per second, keeping the bottleneck fed takes about 200 items. A multiplier of 1.5
        /// gives a 60-second bound, allowing about 300 items at that rate.
        ///
        /// A larger allowance can absorb uneven upstream timing, but also allows more queued
        /// work. Fastest can itself include hidden queue time, so this is not a measurement of
        /// queue-free latency.
        #[builder(default = 1.5)]
        max_slowdown: f64,
        /// What the limit is multiplied by each time it grows. Must be above 1.
        /// The result is rounded up and capped by [`max`](Self::max).
        #[builder(default = 2.0)]
        growth: f64,
        /// Minimum departure count for learning fastest, judging a trial, and measuring a
        /// stretch during a cooldown. Must be at least 1.
        ///
        /// These measurements also need elapsed time at least as long as their estimated
        /// residence. The recent control window aims for this count too, but may contain fewer
        /// departures when it reaches its time cap (see [`max_window`](Self::max_window)).
        ///
        /// A larger count smooths out unusual departures but takes longer to collect. Trial and
        /// cooldown measurements can extend beyond `max_window`; trials also have a timeout.
        ///
        /// Completed and released permits both count. With [weighted items](crate::Gate#weighted-items),
        /// this counts permit weight, not independent events: scale it along with `floor`,
        /// `max`, and `initial` when changing units.
        #[builder(default = 20)]
        window_items: u64,
        /// Time cap for the recent control window, unless the pass estimate is longer.
        /// Must be non-zero.
        ///
        /// The window seeks [`window_items`](Self::window_items) departures and one estimated
        /// pass. When departures are sparse, it stops extending at the larger of this duration
        /// and the pass estimate, to avoid relying on increasingly old observations. Whole
        /// ticks are kept, so the window may extend up to one tick further.
        ///
        /// This does not cap the measurements used for trials, cooldowns, or fastest.
        ///
        /// A stall also requires no departures for longer than this duration and twice the
        /// longest observed gap, with work still inside and at least one earlier departure.
        #[builder(default = Duration::from_secs(30))]
        max_window: Duration,
        /// Fraction of a tick a probe may report idle before it counts as starving.
        ///
        /// At least 0 and below 1. Any probe exceeding this threshold is enough. A little waiting
        /// is normal when handing over work, so a positive threshold avoids reacting to every gap.
        #[builder(default = 0.1)]
        starving_share: f64,
        /// Factor by which fastest may rise on an eligible tick. Must be at least 1.
        ///
        /// Fastest is the lowest measured average residence. To learn an upstream that became
        /// slower, the policy first multiplies fastest by this factor, then lowers it to the
        /// current measurement if that is smaller.
        ///
        /// This requires a nearly full gate, a waiting bottleneck, and enough mostly successful
        /// departures. It is suspended during trials, cooldowns, and preparation for a retry:
        /// a waiting probe alone cannot rule out a hidden queue.
        ///
        /// For example, with a factor of 1.05, fastest rises from 20 to 30 seconds in about nine
        /// eligible ticks if the measurement stays at 30. A factor of 1 disables this upward
        /// adjustment. The rate is per tick, so changing the tick interval changes its speed.
        #[builder(default = 1.05)]
        fastest_decay: f64,
        /// Cap on the settling delay between changes in the same direction.
        ///
        /// After a raise, the delay uses the pass estimate. After a cut, it uses the estimated
        /// time for the excess work to leave. This setting caps those delays so a slow pipeline
        /// can still adapt. Trial evidence and retry cooldowns may require a longer wait.
        ///
        /// For example, a 30-second cap allows another raise after a two-minute pass estimate
        /// once 30 seconds have passed, provided the other conditions for raising are met.
        /// Zero removes the settling delay; it does not disable trial checks or cooldowns.
        #[builder(default = Duration::from_secs(30))]
        max_settle: Duration,
        /// Maximum cooldown after a useless or inconclusive raise is taken back.
        /// Must be non-zero.
        ///
        /// Measured in elapsed sample time, including time with no departures. A detected pace
        /// change or enough measured stretches can end the cooldown sooner. Once it ends, one
        /// retry may cross an old residence bound, but a measured baseline, successes, and the
        /// other conditions for raising are still required.
        ///
        /// This caps suppression of a retry, not the total time to recovery. Lower it to revisit
        /// decisions sooner, at the cost of more experiments and temporary queued work.
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

    /// Lowest measured average residence while the gate was nearly full, with upward
    /// adjustment when allowed by [`fastest_decay`](DrainBoundedBuilder::fastest_decay).
    /// It can include hidden queue time. `None` until measured, or after a stall lowers the
    /// limit to the floor and resets the estimate.
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
    max_retry: Duration,
}
