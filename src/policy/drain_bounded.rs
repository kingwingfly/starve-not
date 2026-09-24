//! The recommended policy: grow while the bottleneck waits, but keep shutdown quick.

use std::time::{Duration, Instant};

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
/// also have finished during the tick. Otherwise a failure where every item errors out quickly
/// would look like a hungry bottleneck, and the limit would grow for nothing.
///
/// **Lowering.** The policy estimates how long an item stays in the pipeline: the number of
/// items inside divided by how many leave per second. That is also roughly how long a clean
/// shutdown would take. While the only problem is upstream delay, letting more in doesn't make
/// items slower, so this time stays flat. Once the bottleneck has all it can handle, extra items
/// only queue up, and the time rises. When it passes the allowed bound, the policy lowers the
/// limit to what can leave within the bound.
///
/// **The bound** is the larger of [`max_drain`] and [`headroom`] times the fastest time
/// observed. That way a pipeline that is slow by nature (say, 20 seconds per item) isn't
/// squeezed below what it needs. The fastest time slowly creeps up again, so a pipeline that
/// becomes slower for good is eventually judged by its new speed.
///
/// **Waiting after a change.** After changing the limit, the policy waits roughly one pass
/// through the pipeline (at most [`max_settle`]) before changing it again, so the change has
/// time to show its effect.
///
/// [`starving`]: DrainBoundedBuilder::starving
/// [`growth`]: DrainBoundedBuilder::growth
/// [`max_drain`]: DrainBoundedBuilder::max_drain
/// [`headroom`]: DrainBoundedBuilder::headroom
/// [`max_settle`]: DrainBoundedBuilder::max_settle
///
/// # Example
///
/// ```
/// # use std::time::Duration;
/// # use starve_not::DrainBounded;
/// // never below two batches of 16, and shutdown should take about 5 seconds at most
/// let policy = DrainBounded::builder().floor(32).max_drain(Duration::from_secs(5)).build();
/// ```
///
/// # Diagnostics
///
/// Each decision reports `throughput` (items finished per second), `departures` (items leaving
/// per second, finished or not), `residence` (estimated seconds an item stays inside),
/// `fastest` (the fastest such time observed), and `starving` and `saturated` (1 if the
/// bottleneck was waiting, or the gate was nearly full, else 0).
#[derive(Debug, Clone)]
pub struct DrainBounded {
    config: Config,
    /// completed items/s, exponentially smoothed
    throughput: f64,
    /// items/s leaving the gate, completed or released, exponentially smoothed; by Little's law
    /// this, not `throughput`, sets how long what's in flight takes to drain
    departures: f64,
    /// lowest observed residence in seconds, slowly decayed to follow lasting slowdowns
    fastest: f64,
    /// hold decisions until newly admitted items have had time to complete
    settle_until: Option<Instant>,
    /// the latest decision's inputs, for diagnostics
    residence: f64,
    starving: bool,
    saturated: bool,
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
        /// If a setting is out of range: `floor` is 0, `headroom` is below 1, `growth` is 1
        /// or less, `smoothing` is 0 or above 1, `starving` is below 0 or 1 or more, or
        /// `fastest_decay` is below 1. Such values would quietly keep the limit from ever
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
        /// How long items may stay inside the pipeline, which is also about how long a clean
        /// shutdown takes. Default: 10 seconds.
        ///
        /// The policy always allows at least this much, even if items used to go through much
        /// faster.
        #[builder(default = Duration::from_secs(10))]
        max_drain: Duration,
        /// How much slower than the fastest observed time an item may get, as a multiple.
        /// Default: 1.5. Must be at least 1.
        ///
        /// Only matters for pipelines where items take longer than `max_drain` even at best.
        #[builder(default = 1.5)]
        headroom: f64,
        /// What the limit is multiplied by each time it grows. Default: 2. Must be above 1.
        #[builder(default = 2.0)]
        growth: f64,
        /// How much the newest tick counts when averaging how fast items leave. Default: 0.5.
        ///
        /// Between 0 (not included) and 1. Higher reacts faster, lower is steadier; 1 uses only
        /// the latest tick.
        #[builder(default = 0.5)]
        smoothing: f64,
        /// How much of a tick the bottleneck may spend waiting before it counts as starving.
        /// Default: 0.1, that is, 10%.
        ///
        /// At least 0 and below 1. A little waiting is normal (handing over a batch takes some
        /// time), so 0 is rarely a good idea.
        #[builder(default = 0.1)]
        starving: f64,
        /// How fast the remembered fastest time creeps back up, as a factor per tick.
        /// Default: 1.05, that is, 5% per tick. Must be at least 1.
        ///
        /// This lets the policy accept a pipeline that has become slower for good. 1 means
        /// never forgetting.
        #[builder(default = 1.05)]
        fastest_decay: f64,
        /// The longest the policy waits after a change before it may change the limit again.
        /// Default: 30 seconds.
        #[builder(default = Duration::from_secs(30))]
        max_settle: Duration,
    ) -> Self {
        assert!(floor > 0, "floor must be at least 1");
        assert!(headroom >= 1.0, "headroom must be at least 1");
        assert!(growth > 1.0, "growth must be above 1");
        assert!((0.0..1.0).contains(&starving), "starving must be in [0, 1)");
        assert!(
            smoothing > 0.0 && smoothing <= 1.0,
            "smoothing must be in (0, 1]"
        );
        assert!(fastest_decay >= 1.0, "fastest_decay must be at least 1");
        Self {
            config: Config {
                floor,
                max: max.max(floor),
                initial,
                max_drain,
                headroom,
                growth,
                smoothing,
                starving,
                fastest_decay,
                max_settle,
            },
            throughput: 0.0,
            departures: 0.0,
            fastest: f64::INFINITY,
            settle_until: None,
            residence: f64::INFINITY,
            starving: false,
            saturated: false,
        }
    }

    /// Items finished per second, averaged over recent ticks.
    pub fn throughput(&self) -> f64 {
        self.throughput
    }

    /// The shortest time an item took to go through the pipeline while the gate was full,
    /// slowly creeping up over time. `None` until one has been observed.
    pub fn fastest(&self) -> Option<Duration> {
        // `fastest` keeps decaying upward while nothing completes, and can outgrow `Duration`
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
        let elapsed = sample.elapsed.as_secs_f64();
        let fresh = elapsed > 0.0;
        if fresh {
            let completed = sample.completed as f64 / elapsed;
            let departed = (sample.completed + sample.released) as f64 / elapsed;
            self.throughput += c.smoothing * (completed - self.throughput);
            self.departures += c.smoothing * (departed - self.departures);
        }
        self.starving = sample.idle_shares().any(|share| share > c.starving);
        self.residence = match (sample.in_flight, self.departures > 0.0) {
            // nothing to drain: an idle pipeline isn't one that can't drain
            (0, _) => 0.0,
            (in_flight, true) => in_flight as f64 / self.departures,
            (_, false) => f64::INFINITY,
        };
        self.saturated = sample.in_flight as u128 * 10 >= limit as u128 * 9;
        // an empty interval says nothing about rates: report the state above, change nothing
        if !fresh {
            return limit;
        }
        self.fastest *= c.fastest_decay;
        // sample only while the gate is full: a near-empty pipeline (no work, tail of a run)
        // shows a short residence that says nothing about per-item latency
        if self.saturated && self.residence > 0.0 && self.residence.is_finite() {
            self.fastest = self.fastest.min(self.residence);
        }
        let drain_bound = c.max_drain.as_secs_f64().max(self.fastest * c.headroom);

        let target = if self.settle_until.is_some_and(|until| sample.at < until) {
            limit
        } else if self.residence > drain_bound {
            // draining would take too long (or nothing completes): keep what finishes in time.
            // Never above the limit: while a shrink's permits still retire, more can be in
            // flight than the limit allows, and this branch must not grow it
            ((self.departures * drain_bound) as usize).min(limit)
        } else if self.starving && self.saturated && sample.completed > 0 {
            // the bottleneck waits while the gate is full; growth also needs items completing
            // in this interval, or an outage (or startup) would grow it while every item fails:
            // smoothed throughput stays positive for a while after completions stop
            (limit as f64 * c.growth).ceil() as usize
        } else {
            limit
        }
        .clamp(c.floor, c.max);
        if target != limit {
            // `fastest` may be infinite or past `Duration::MAX`; a hold that overflows `Instant`
            // is no hold
            let settle = Duration::try_from_secs_f64(self.fastest)
                .map_or(c.max_settle, |fastest| fastest.min(c.max_settle));
            self.settle_until = sample.at.checked_add(settle);
        }
        target
    }

    fn diagnostics(&self) -> Diagnostics {
        let mut d = Diagnostics::default();
        d.push("throughput", self.throughput);
        d.push("departures", self.departures);
        d.push("residence", self.residence);
        d.push("fastest", self.fastest);
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
    max_drain: Duration,
    headroom: f64,
    growth: f64,
    smoothing: f64,
    starving: f64,
    fastest_decay: f64,
    max_settle: Duration,
}
