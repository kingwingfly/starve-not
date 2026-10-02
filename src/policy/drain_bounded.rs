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
/// # Every tick
///
/// ```text
/// is the pipeline stuck? --------------------------- yes -> drop to `floor`
///   | no
/// did the latest raise turn out useless? ----------- yes -> take it back
///   | no
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
/// second (Little's law). It is measured over recent ticks: enough of them to see at least
/// [`window_items`] items leave and one pass through the pipeline, but at most [`max_window`].
///
/// The *bound* is the longest residence allowed:
///
/// ```text
/// bound = the larger of:  drain_target
///                         max_slowdown x fastest
/// ```
///
/// *Fastest* is the shortest residence seen while the gate was full. Usually [`drain_target`]
/// is the larger. [`max_slowdown`] takes over for pipelines that are slow by nature, so they
/// aren't squeezed below what they need.
///
/// - Until the fastest time is known, there is no bound: a long residence may just be the
///   pipeline filling up. Raises then stop at [`drain_target`].
/// - Right after a raise, the new items are inside but none has left yet, so residence reads
///   high, by up to [`growth`] times. The bound is widened by as much until they show up.
///
/// Queueing only adds to residence, so the fastest time only goes down. To accept an upstream
/// that got slower for good, it creeps up by [`fastest_decay`] while the bottleneck waits, since
/// then nothing queues for it. Not after a useless raise, though (see below), until a raise
/// helps again: that raise showed items queue where no probe sees.
///
/// # Raising
///
/// The limit is multiplied by [`growth`] when the bottleneck waits for input (more than
/// [`starving_share`] of a tick) while the gate is nearly full (90%), and only if:
///
/// - residence is within the bound: more items can't make them leave sooner. Only a retry after
///   a useless raise may go past it (see below);
/// - items succeed: at least one since the last change, and at least half of those leaving
///   lately. Items that fail fast, or hang, would otherwise look like a hungry bottleneck;
/// - the previous raise had time to show: one pass, at most [`max_settle`]. If it is still on
///   trial (see below), departures must have risen with it, and it must not be a retry;
/// - raises aren't waiting after a useless raise.
///
/// # Checking a raise
///
/// A raise is on *trial* until its items reach the whole measuring window. A waiting bottleneck
/// turns more items into more departures, so by then departures should be up by at least half
/// as much as the limit, and by more than chance:
///
/// ```text
/// raise -> trial: its items are on their way -> trial ends
///                                                   |
///            departures up with the limit? -------- yes -> keep it
///                                                   | no
///            residence past the bound? ------------ no --> keep it: it does no harm
///                                                   | yes
///            departures fell? --------------------- yes -> take it back. Something else
///                                                   | no     changed, such as upstream
///                                                   |        hanging: raises wait only if
///                                                   v        an earlier raise was useless
///                     take it back: items queue where no probe sees,
///                     such as behind the bottleneck. Raises wait
/// ```
///
/// This needs to know when the raise's items come through, so only raises made once the
/// fastest time is known are checked.
///
/// # After a useless raise
///
/// Under the same conditions, the same raise would be just as useless. So raises wait until the
/// *pace* changes, that is, until whatever holds the items up gets faster or slower. The policy
/// measures how many items leave per second at the restored limit, in stretches of at least
/// [`window_items`] items and one pass, and compares each stretch with the ones before:
///
/// ```text
/// stretch:  1         2       3                4
/// pace:     baseline  same    faster           faster
///                             ^ maybe chance   ^ twice in a row: the pace changed,
///                                                raises may try again
/// ```
///
/// - A stretch must differ by more than chance, and by at least 10%. How much the pace varies by
///   chance is learned as it goes: little for a stage that works like clockwork, a lot for one
///   that sends items out in batches.
/// - Chance rarely makes two stretches in a row differ the same way, so it takes two.
/// - A cut starts the measuring over, since the pace at the old limit no longer holds. A cut to
///   [`floor`] ends the wait instead: there the limit itself sets the pace, so it would never
///   change.
///
/// The raise may have been judged wrongly, so the wait also ends after a while even if the pace
/// stays the same: after 16 stretches, longer the more the pace varies by chance. However the
/// wait ends, one *retry* may go past the bound, to find out whether a raise helps now. A retry
/// that is useless again is taken back, and the next wait is twice as long:
///
/// ```text
/// limit      retry          retry                    retry
///   16 |      +--+           +--+                     +--+
///    8 |------+  +-----------+  +---------------------+  +------------->
///       wait      wait x 2            wait x 4
///      +--------------------------------------------------------------> time
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
/// latency, or a queue the probe can't see. A cut would only starve the bottleneck.
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
/// right now), `window` (the seconds the window spans), three flags that are 1 or 0:
/// `is_starving` (the bottleneck was waiting), `is_saturated` (the gate was nearly full) and
/// `is_raise_useless` (a raise was taken back, and raises wait for the pace to change), and
/// `pace_dispersion` (how much more the pace varies by chance than if items left at random).
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
    /// set when a raise was taken back because it didn't help
    useless_raise: Option<UselessRaise>,
    /// how much the pace varies by chance, learned while raises wait after a useless raise
    pace_dispersion: Dispersion,
    /// raises taken back as useless since one last helped. Each doubles the wait after the next.
    /// While there are any, items queue where no probe sees
    useless_in_a_row: u32,
    /// a wait after a useless raise just ended: the next raise may go past the bound, to find
    /// out whether it helps now
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

/// One or more raises in a row, being tried. Their items haven't reached the whole window yet,
/// so whether they help isn't known. Once they have, the trial ends: the latest raise is kept,
/// or taken back if it didn't help.
#[derive(Debug, Clone, Copy)]
struct RaiseTrial {
    /// the limit before the first of these raises
    before_first: usize,
    /// the limit before the latest raise
    before_latest: usize,
    /// the window just before the latest raise
    window_before: Tally,
    /// whether the pass was known at the latest raise; if not, its items may come through only
    /// after the trial ends, so it can't be judged
    is_checkable: bool,
    /// when the latest raise's items start leaving
    leaving_from: Instant,
}

impl RaiseTrial {
    /// What the departures in `window` show about the latest raise, if it takes a rise of
    /// `noise` times the chance to count as helping. A waiting bottleneck gets more work from more
    /// items, so departures follow the limit.
    fn verdict(
        &self,
        limit: usize,
        window: &Tally,
        dispersion: &Dispersion,
        noise: f64,
    ) -> Verdict {
        if self.window_before.departed == 0 {
            return Verdict::Helped;
        }
        if window.departed == 0 {
            return Verdict::Unclear;
        }
        let raised = limit as f64 / self.before_latest as f64 - 1.0;
        let needed = (1.0 + raised / 2.0).ln();
        let rose = (window.departure_rate() / self.window_before.departure_rate()).ln();
        let chance = window.rate_wobble(&self.window_before) * dispersion.value().sqrt();
        if rose >= needed.max(noise * chance) {
            Verdict::Helped
        } else if rose < -PACE_NOISE * chance {
            Verdict::Unclear
        } else {
            Verdict::Useless
        }
    }
}

/// What a raise trial shows about the latest raise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// departures rose with it: by at least half as much, and by more than chance
    Helped,
    /// departures didn't rise with it: the items queue where no probe sees
    Useless,
    /// departures fell, by more than chance: something else changed, such as upstream hanging,
    /// so the raise can't be judged
    Unclear,
}

/// How far departures must rise, during a raise's trial, for another raise to follow it, in
/// multiples of how far chance alone would move them. Lower than the [`PACE_NOISE`] that judges
/// the raise when its trial ends, since a higher bar slows down every climb while few items have
/// left, and a wrong guess here only lets one raise more be judged. When the pace varies a lot by
/// chance, even a raise that helped may not be shown to: then no raise follows during its trial,
/// and if residence is past the bound, it is taken back and raises wait, though not for good
/// (see [`USELESS_WAIT_STRETCHES`]).
const RAISE_NOISE: f64 = 1.0;

/// How far apart two paces must be to differ, in multiples of how far apart chance alone would
/// put them. At 2, chance alone gets there about one time in 20, so a change only counts once two
/// stretches in a row differ the same way.
const PACE_NOISE: f64 = 2.0;

/// The smallest change of pace that counts, however many items it was measured from, as a
/// fraction: 0.1 is 10% faster or slower. A smaller change isn't worth trying a useless raise
/// again.
const MIN_PACE_CHANGE: f64 = 0.1;

/// How many comparisons' worth of weight the assumption that items leave at random gets, against
/// the dispersion measured. A few comparisons can't tell much, so the measurement takes over
/// gradually.
const ASSUMED_COMPARISONS: f64 = 4.0;

/// About how many of the latest comparisons the dispersion is measured from, so it follows a
/// pipeline that changes how it sends items out.
const DISPERSION_MEMORY: f64 = 50.0;

/// How many measured stretches raises wait after a useless raise before trying again, even if
/// the pace stays the same. The raise may have been judged wrongly, and waiting for good could
/// then keep the limit too low forever. Each useless raise in a row doubles the wait, up to
/// `2^MAX_WAIT_DOUBLINGS` times this, so trying again stays rare.
const USELESS_WAIT_STRETCHES: u32 = 16;

/// See [`USELESS_WAIT_STRETCHES`].
const MAX_WAIT_DOUBLINGS: u32 = 6;

/// A raise was taken back because it didn't help. While the pace stays the same, raising again
/// would be just as useless.
#[derive(Debug, Clone, Copy, Default)]
struct UselessRaise {
    /// the pace at the restored limit: every measured stretch not part of a change, added up
    baseline: Tally,
    /// a stretch that differed from the baseline, until the next one tells chance from a change
    differing: Option<Differing>,
    /// the stretch being measured, from after the taken-back items left
    stretch: Tally,
    /// how many stretches have been measured
    stretches: u32,
}

/// A stretch whose pace differed from the baseline by more than chance.
#[derive(Debug, Clone, Copy)]
struct Differing {
    stretch: Tally,
    /// how far apart its pace was from the baseline's, in Poisson wobbles: above 0 if faster
    gap: f64,
}

/// How much more the pace varies by chance than if each item left at random, on its own (the
/// index of dispersion). About 1 for a stage whose time per item varies at random, near 0 for
/// one that works like clockwork, and about B for one that sends items out in batches of B.
///
/// It belongs to the pipeline, not to one useless raise, so it is kept across them.
#[derive(Debug, Clone, Copy, Default)]
struct Dispersion {
    /// each comparison's squared gap, in Poisson wobbles, added up with older ones fading
    squares: f64,
    /// how many comparisons that is, fading the same way
    comparisons: f64,
}

impl Dispersion {
    /// Count a comparison of two paces that differed only by chance. If items left at random,
    /// its gap squared would be about 1 on average; what it really is on average is the
    /// dispersion.
    fn add(&mut self, gap: f64) {
        let keep = 1.0 - 1.0 / DISPERSION_MEMORY;
        self.squares = self.squares * keep + gap * gap;
        self.comparisons = self.comparisons * keep + 1.0;
    }

    /// The dispersion measured so far, leaning on 1 while there are few comparisons.
    fn value(&self) -> f64 {
        (ASSUMED_COMPARISONS + self.squares) / (ASSUMED_COMPARISONS + self.comparisons)
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

    /// Whether this tally is enough to trust its rates and residence. It needs both:
    /// - at least `min_departures` items left, so a few early or late items don't skew it;
    /// - it spans at least one residence, so the items inside at its start had time to leave.
    ///   A shorter tally sees only part of a pass, such as one burst of departures.
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
        self.completed * 2 >= self.departed
    }

    /// How far apart this departure rate and `other`'s would be by chance alone, as the log of
    /// their ratio, if items left at random: a rate counted from n items is off by about 1/√n.
    /// The dispersion scales it to how items really leave.
    fn rate_wobble(&self, other: &Tally) -> f64 {
        (1.0 / self.departed as f64 + 1.0 / other.departed as f64).sqrt()
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
        /// or less, `window_items` is 0, `max_window` is 0, `starving_share` is below 0 or at
        /// least 1, or `fastest_decay` is below 1. Such values would quietly keep the limit from
        /// ever growing, or stick it at the floor.
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
        /// How long the policy aims for items to stay inside the pipeline, which is also about
        /// how long a clean shutdown takes. Default: 10 seconds.
        ///
        /// For most pipelines this works as a cap: the limit comes down whenever items would
        /// take longer. The exception is a pipeline where items take longer than this even at
        /// their fastest. Capping it here would starve the bottleneck, so the policy allows
        /// [`max_slowdown`](Self::max_slowdown) times the fastest time observed instead.
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
        #[builder(default = 20)]
        window_items: u64,
        /// How far the policy looks back, at most, when measuring how fast items leave.
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
        /// and for twice the longest gap seen before.
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
        /// longer time means upstream got slower, not that items queue. And not after a raise
        /// that didn't help, until a raise helps again: that raise showed items queue where no
        /// probe sees.
        ///
        /// For example, if the fastest time is 20 seconds and items now take 30, it reaches 30
        /// after about 9 such ticks (1.05⁹ ≈ 1.55). 1 means it never rises.
        #[builder(default = 1.05)]
        fastest_decay: f64,
        /// The longest the policy waits after changing the limit before it changes it the same
        /// way again. Default: 30 seconds.
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
            useless_raise: None,
            pace_dispersion: Dispersion::default(),
            useless_in_a_row: 0,
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

    /// The shortest time an item took to go through the pipeline while the gate was full,
    /// slowly creeping up while the bottleneck waits for input. `None` until one has been
    /// observed.
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
            if let Some(useless) = &mut self.useless_raise {
                useless.stretch.add(&tick);
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

        // 2. A raise trial ends once the window starts after its items began to leave.
        let mut ended = None;
        if let (Some(trial), Some(start)) = (self.raise_trial, self.window.start())
            && start >= trial.leaving_from
        {
            self.raise_trial = None;
            self.settled = Tally::default();
            ended = Some(trial);
        }
        // After a useless raise, compare each measured stretch with the pace at the restored
        // limit. Once two in a row differ the same way, the pace changed: a raise may help again.
        // Everything else adds to the baseline, including a stretch that differed by chance, and
        // teaches how much the pace varies by chance.
        if let Some(useless) = &mut self.useless_raise
            && useless.stretch.is_trustworthy(c.window_items)
        {
            let stretch = std::mem::take(&mut useless.stretch);
            useless.stretches += 1;
            let baseline = useless.baseline;
            if baseline.departed == 0 {
                useless.baseline = stretch;
            } else {
                let (old_rate, new_rate) = (baseline.departure_rate(), stretch.departure_rate());
                // as the log of their ratio, so twice as fast and half as fast count the same
                let apart = (new_rate / old_rate).ln();
                let wobble = stretch.rate_wobble(&baseline);
                let chance = wobble * self.pace_dispersion.value().sqrt();
                let threshold = (PACE_NOISE * chance).max(MIN_PACE_CHANGE.ln_1p());
                // in wobbles, to learn the dispersion from once it is known to be chance
                let gap = apart / wobble;
                let is_apart = apart.abs() > threshold;
                let earlier = useless.differing.take();
                if is_apart && earlier.is_some_and(|earlier| (earlier.gap > 0.0) == (gap > 0.0)) {
                    // the second in a row to differ this way: not chance
                    self.useless_raise = None;
                    self.is_retry_due = true;
                } else {
                    // an earlier one that differed was chance
                    if let Some(earlier) = earlier {
                        useless.baseline.add(&earlier.stretch);
                        self.pace_dispersion.add(earlier.gap);
                    }
                    if is_apart {
                        // the first to differ, or it differs the other way: wait for the next
                        useless.differing = Some(Differing { stretch, gap });
                    } else {
                        useless.baseline.add(&stretch);
                        self.pace_dispersion.add(gap);
                    }
                }
            }
        }
        // the same pace for long enough: try again anyway, in case the raise was judged wrongly.
        // A pace that varies more by chance tells less per stretch, so the wait is longer
        let doublings = self
            .useless_in_a_row
            .saturating_sub(1)
            .min(MAX_WAIT_DOUBLINGS);
        let stretches = (USELESS_WAIT_STRETCHES << doublings) as f64;
        let stretches = stretches * self.pace_dispersion.value().max(1.0);
        if let Some(useless) = &self.useless_raise
            && useless.stretches as f64 >= stretches
        {
            self.useless_raise = None;
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
            // creep up only while nothing queues: the bottleneck waits, and no useless raise
            // showed that items queue where no probe sees
            if is_starving && self.useless_in_a_row == 0 {
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
        // before the bound is known
        let raise_bound = match drain_bound.is_finite() {
            true => bound,
            false => c.drain_target.as_secs_f64() * widen,
        };
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
        // what the trial that just ended shows, if it can be judged
        let verdict = ended
            .filter(|trial| trial.is_checkable)
            .map(|trial| trial.verdict(limit, &window, &self.pace_dispersion, PACE_NOISE));
        // a raise not shown to help that pushed residence past the bound is taken back. If
        // departures stayed the same, it only lengthened a hidden queue: raises then wait. If
        // they fell, something else changed, and the raise says nothing new: raises wait only
        // if a hidden queue is already known
        let taken_back = ended.filter(|_| {
            verdict.is_some_and(|verdict| verdict != Verdict::Helped)
                && is_starving
                && self.residence > raise_bound
        });
        let is_useless = taken_back.is_some() && verdict == Some(Verdict::Useless);
        if is_useless {
            self.useless_in_a_row += 1;
        }
        if taken_back.is_some() && self.useless_in_a_row > 0 {
            self.useless_raise = Some(UselessRaise::default());
        }
        if verdict == Some(Verdict::Helped) {
            self.useless_in_a_row = 0;
        }
        // during a trial, raise again only once the latest raise helped. Not at all while a
        // hidden queue is known: then a raise is a retry, to be judged on its own
        let dispersion = &self.pace_dispersion;
        let is_trial_helping = self.raise_trial.is_none_or(|trial| {
            self.useless_in_a_row == 0
                && trial.verdict(limit, &window, dispersion, RAISE_NOISE) == Verdict::Helped
        });
        let target = if is_dropping {
            0
        } else if let Some(trial) = taken_back {
            trial.before_latest
        } else if self.residence > bound && !is_starving && is_cut_allowed {
            // keep what leaves within the bound. Not while the bottleneck waits: then nothing
            // queues, and a long residence is upstream latency, which a cut can't shorten.
            // Never above the limit: while a cut's permits retire, more can be in flight
            ((departure_rate * drain_bound) as usize).min(limit)
        } else if is_starving
            && is_saturated
            && is_raise_allowed
            // past the bound only to retry after a wait. Otherwise, once upstream got slower
            // for good, the limit could never rise again, since the fastest time doesn't creep
            // up while a hidden queue is known
            && (self.residence <= raise_bound || self.is_retry_due)
            && self.useless_raise.is_none()
            && is_trial_helping
            && self.is_succeeding
            && window.is_mostly_successful()
        {
            (limit as f64 * c.growth).ceil() as usize
        } else {
            limit
        }
        .clamp(c.floor, c.max);

        // 6. Hold the next change until this one shows.
        if target != limit {
            self.settled = Tally::default();
            self.is_succeeding = false;
        }
        if target > limit {
            self.is_retry_due = false;
            // the new items reach the bottleneck, and start leaving, about one pass from now
            let pass = self.pass_estimate();
            self.raise_after = sample.at.checked_add(pass.min(c.max_settle));
            // a raise during a trial joins it
            let before_first = self.raise_trial.map_or(limit, |trial| trial.before_first);
            self.raise_trial = sample.at.checked_add(pass).map(|leaving_from| RaiseTrial {
                before_first: before_first.max(1),
                before_latest: limit.max(1),
                window_before: window,
                is_checkable: self.pass.is_some(),
                leaving_from,
            });
        } else if target < limit {
            self.raise_trial = None;
            self.raise_after = None;
            // the pace at the old limit no longer holds: measure it again. Not after a cut to
            // the floor, such as when the pipeline gets stuck: there the limit sets the pace, so
            // it would never change, and what the useless raises showed no longer holds. Taking
            // a raise back to the floor is different: its wait just began
            let is_floor_cut = target == c.floor && (is_dropping || taken_back.is_none());
            if self.useless_raise.is_some() {
                self.useless_raise = (!is_floor_cut).then(UselessRaise::default);
            }
            if is_floor_cut {
                self.useless_in_a_row = 0;
                self.is_retry_due = false;
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
        d.push_flag("is_raise_useless", self.useless_raise.is_some());
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
}
