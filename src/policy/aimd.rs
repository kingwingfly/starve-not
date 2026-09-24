//! Grow slowly, cut quickly, as TCP does.

use std::time::Duration;

use bon::bon;

use super::{Diagnostics, Policy};
use crate::Sample;

/// Raises the limit a little on every good tick, and cuts it by a percentage on every bad one.
///
/// This is the classic rule TCP uses to share a network, known as AIMD (additive increase,
/// multiplicative decrease). It doesn't need an [`IdleProbe`](crate::IdleProbe). It looks only
/// at failures and at how long items take, so it suits pipelines with no single bottleneck.
///
/// # How it decides
///
/// A tick is **bad** if either:
///
/// - more than [`tolerance`] of the items that left were released rather than completed
///   (failures, timeouts), or
/// - items take longer than [`max_residence`] to go through the pipeline. This time is
///   estimated as the number of items inside divided by how many leave per second. If nothing
///   leaves at all, it's the time since items last left, counted across ticks, so a pipeline
///   that is stuck counts as bad once it has been stuck for `max_residence`.
///
/// On a bad tick the limit is multiplied by [`backoff`]. On a good tick where items finished and
/// at least half the limit is in use, [`increase`] is added. Otherwise the limit stays: with
/// less than half in use more room wouldn't be used anyway, and with nothing finishing there's
/// no sign that more work would get through.
///
/// Since it can't see whether a bottleneck is waiting, it keeps pushing the limit up until
/// something goes wrong, then backs off. The limit ends up going up and down in a sawtooth just
/// below the point where trouble starts.
///
/// [`tolerance`]: AimdBuilder::tolerance
/// [`max_residence`]: AimdBuilder::max_residence
/// [`backoff`]: AimdBuilder::backoff
/// [`increase`]: AimdBuilder::increase
///
/// # Example
///
/// ```
/// # use std::time::Duration;
/// # use starve_not::Aimd;
/// let policy = Aimd::builder().floor(8).increase(4).max_residence(Duration::from_secs(5)).build();
/// ```
///
/// # Diagnostics
///
/// Each decision reports `residence` (estimated seconds an item stays inside), `released_share`
/// (the fraction of leaving items that were released), and `congested` and `in_use` (1 if the
/// tick was bad, or at least half the limit was in use, else 0).
#[derive(Debug, Clone)]
pub struct Aimd {
    config: Config,
    /// the latest decision's inputs, for diagnostics
    residence: f64,
    released_share: f64,
    /// time items have been inside with none leaving, across ticks
    stalled: Duration,
    congested: bool,
    in_use: bool,
}

#[bon]
impl Aimd {
    /// Set up the policy. Every setting has a default, so `Aimd::builder().build()` (or
    /// [`Aimd::default()`]) works as is.
    #[builder(finish_fn(doc {
        /// Create the policy.
        ///
        /// # Panics
        ///
        /// If `floor` or `increase` is 0, `backoff` isn't strictly between 0 and 1, or
        /// `tolerance` is below 0 or 1 or more.
    }))]
    pub fn new(
        /// The lowest the limit goes. Default: 1.
        #[builder(default = 1)]
        floor: usize,
        /// The highest the limit goes, as a safety cap on memory. Default: 1024.
        ///
        /// If set below `floor`, `floor` is used.
        #[builder(default = 1024)]
        max: usize,
        /// The limit to start with. Default: `floor`. Kept between `floor` and `max`.
        initial: Option<usize>,
        /// How much the limit grows on each good tick. Default: 1.
        ///
        /// The default grows slowly: with 2-second ticks, going from 10 to 100 takes three
        /// minutes. Raise it if your pipeline needs hundreds of items inside.
        #[builder(default = 1)]
        increase: usize,
        /// What the limit is multiplied by on a bad tick. Default: 0.9, a 10% cut.
        ///
        /// Must be strictly between 0 and 1.
        #[builder(default = 0.9)]
        backoff: f64,
        /// How long items may take to go through the pipeline before a tick counts as bad.
        /// Default: 10 seconds.
        #[builder(default = Duration::from_secs(10))]
        max_residence: Duration,
        /// The fraction of items that may be released, rather than completed, on a good tick.
        /// Default: 0, so any release makes the tick bad.
        ///
        /// At least 0 and below 1. Raise it if some failures are normal, for example 0.05 to
        /// allow 5%.
        #[builder(default = 0.0)]
        tolerance: f64,
    ) -> Self {
        assert!(floor > 0, "floor must be at least 1");
        assert!(increase > 0, "increase must be at least 1");
        assert!(backoff > 0.0 && backoff < 1.0, "backoff must be in (0, 1)");
        assert!(
            (0.0..1.0).contains(&tolerance),
            "tolerance must be in [0, 1)"
        );
        Self {
            config: Config {
                floor,
                max: max.max(floor),
                initial,
                increase,
                backoff,
                max_residence,
                tolerance,
            },
            residence: 0.0,
            released_share: 0.0,
            stalled: Duration::ZERO,
            congested: false,
            in_use: false,
        }
    }
}

impl Default for Aimd {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl Policy for Aimd {
    fn initial(&self) -> usize {
        let c = &self.config;
        c.initial.unwrap_or(c.floor).clamp(c.floor, c.max)
    }

    fn decide(&mut self, sample: &Sample) -> usize {
        let c = &self.config;
        let limit = sample.limit;
        let elapsed = sample.elapsed.as_secs_f64();
        if elapsed <= 0.0 {
            return limit;
        }
        let departed = sample.completed + sample.released;
        self.released_share = match departed {
            0 => 0.0,
            departed => sample.released as f64 / departed as f64,
        };
        self.residence = match (sample.in_flight, departed) {
            (0, _) => {
                self.stalled = Duration::ZERO;
                0.0
            }
            // nothing left: the items inside have been there at least as long as nothing has
            // left, which may span many ticks. Counting only this tick would let a stall
            // longer than `max_residence` go unnoticed when ticks are shorter than it
            (_, 0) => {
                self.stalled = self.stalled.saturating_add(sample.elapsed);
                self.stalled.as_secs_f64()
            }
            (in_flight, departed) => {
                self.stalled = Duration::ZERO;
                in_flight as f64 * elapsed / departed as f64
            }
        };
        self.congested =
            self.released_share > c.tolerance || self.residence > c.max_residence.as_secs_f64();
        self.in_use = sample.in_flight as u128 * 2 >= limit as u128;

        if self.congested {
            // while a previous cut's permits still retire, what's in flight reflects the old
            // limit: cutting again would count the same congestion twice
            if sample.in_flight > limit {
                return limit.clamp(c.floor, c.max);
            }
            ((limit as f64 * c.backoff) as usize).clamp(c.floor, c.max)
        } else if self.in_use && sample.completed > 0 {
            // growth needs items finishing now: a tick where nothing left (the start of a
            // stall) or everything failed within `tolerance` is no evidence of room
            limit.saturating_add(c.increase).clamp(c.floor, c.max)
        } else {
            limit.clamp(c.floor, c.max)
        }
    }

    fn diagnostics(&self) -> Diagnostics {
        let mut d = Diagnostics::default();
        d.push("residence", self.residence);
        d.push("released_share", self.released_share);
        d.push("congested", f64::from(u8::from(self.congested)));
        d.push("in_use", f64::from(u8::from(self.in_use)));
        d
    }
}

/// [`Aimd`]'s settings, as its builder set them.
#[derive(Debug, Clone)]
struct Config {
    floor: usize,
    max: usize,
    initial: Option<usize>,
    increase: usize,
    backoff: f64,
    max_residence: Duration,
    tolerance: f64,
}
