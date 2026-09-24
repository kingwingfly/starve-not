//! The pacer, which adjusts the gate's limit on a timer.

use std::{
    fmt,
    time::{Duration, Instant},
};

use bon::bon;

use crate::{Diagnostics, Gate, IdleProbe, Policy, Sample, sample::Sampler};

type OnDecision<P> = Box<dyn FnMut(&Decision, &P) + Send>;

/// Adjusts a [`Gate`]'s limit at a regular interval, as its [`Policy`] decides.
///
/// Every *tick* (2 seconds by default), the pacer looks at how many items finished, how many
/// are inside and how long the bottleneck waited. It passes all that to the policy as a
/// [`Sample`] and sets the gate's limit to the policy's answer.
///
/// On tokio, [`spawn`](Self::spawn) runs this in the background. Elsewhere, call
/// [`step`](Self::step) on your own timer.
pub struct Pacer<P: Policy> {
    gate: Gate,
    probes: Vec<IdleProbe>,
    policy: P,
    tick: Duration,
    sampler: Sampler,
    on_decision: Option<OnDecision<P>>,
}

#[bon]
impl<P: Policy> Pacer<P> {
    /// Set up a pacer that adjusts `gate` using `policy`.
    ///
    /// Add the bottleneck's probes, then call `build`:
    ///
    /// ```
    /// # use starve_not::{DrainBounded, Gate, IdleProbe, Pacer};
    /// let (gate, gpu0, gpu1) = (Gate::new(1), IdleProbe::new(), IdleProbe::new());
    /// let pacer = Pacer::builder(&gate, DrainBounded::default())
    ///     .probes([&gpu0, &gpu1])
    ///     .build();
    /// ```
    #[builder(finish_fn(doc {
        /// Create the pacer, and set the gate's limit to the policy's starting value.
        ///
        /// # Panics
        ///
        /// If `tick` is zero.
    }))]
    pub fn new(
        #[builder(start_fn)] gate: &Gate,
        #[builder(start_fn)] policy: P,
        /// Probes on the bottleneck, added with [`probe`](PacerBuilder::probe) or
        /// [`probes`](PacerBuilder::probes).
        #[builder(field)]
        probes: Vec<IdleProbe>,
        /// How often a [`spawn`](Pacer::spawn)ed pacer decides. Default: 2 seconds.
        ///
        /// Shorter ticks react faster but judge from fewer finished items. A tick should
        /// comfortably cover a few batches going through the bottleneck.
        #[builder(default = Duration::from_secs(2))]
        tick: Duration,
        /// A function called after every decision, for example to log or export metrics. It
        /// gets the [`Decision`] and the policy.
        #[builder(with = |f: impl FnMut(&Decision, &P) + Send + 'static| Box::new(f) as OnDecision<P>)]
        on_decision: Option<OnDecision<P>>,
    ) -> Self {
        assert!(!tick.is_zero(), "pacer tick must be non-zero");
        gate.resize(policy.initial());
        Self {
            gate: gate.clone(),
            probes,
            policy,
            tick,
            sampler: Sampler::default(),
            on_decision,
        }
    }

    /// The policy, for example to read its current state.
    pub fn policy(&self) -> &P {
        &self.policy
    }

    /// Run one tick by hand: look at the gate and probes, ask the policy, set the new limit.
    ///
    /// Call it at regular intervals, passing the current time as `now`. Any clock works, as
    /// long as every call uses the same one. (Probes still measure idle time on the system
    /// clock.)
    ///
    /// Returns `None` on the first call, which only records a starting point, and when `now`
    /// equals the previous call's, since no time has passed to judge.
    pub fn step(&mut self, now: Instant) -> Option<Decision> {
        let sample = self.sampler.sample(&self.gate, &self.probes, now)?;
        self.gate.resize(self.policy.decide(&sample));
        let decision = Decision {
            limit: sample.limit,
            // what the gate applied, which caps what the policy asked for
            target: self.gate.limit(),
            sample,
            diagnostics: self.policy.diagnostics(),
        };
        #[cfg(feature = "tracing")]
        tracing::debug!(
            limit = decision.limit,
            target = decision.target,
            in_flight = decision.sample.in_flight,
            completed = decision.sample.completed,
            released = decision.sample.released,
            idle = ?decision.sample.idle,
            diagnostics = %decision.diagnostics,
            "starve-not decision"
        );
        if let Some(on_decision) = &mut self.on_decision {
            on_decision(&decision, &self.policy);
        }
        Some(decision)
    }

    /// Run the pacer in the background on the current tokio runtime.
    ///
    /// It keeps running until the returned [`PacerHandle`] is dropped. Timing uses tokio's
    /// clock, so it follows `tokio::time::pause` and `advance` in tests. [`IdleProbe`]s still
    /// use the system clock.
    ///
    /// # Panics
    ///
    /// If called outside a tokio runtime.
    ///
    /// The background task stops if the runtime has no timer enabled, or if the policy or the
    /// [`on_decision`](PacerBuilder::on_decision) function panics. Check with
    /// [`PacerHandle::is_finished`].
    #[cfg(feature = "rt")]
    #[must_use = "dropping the handle stops the pacer"]
    pub fn spawn(mut self) -> PacerHandle {
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(self.tick);
            // after a stall, don't fire catch-up ticks with near-zero elapsed time
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // the first tick completes at once and takes the baseline
            loop {
                ticker.tick().await;
                self.step(tokio::time::Instant::now().into_std());
            }
        });
        PacerHandle { task }
    }
}

impl<P: Policy + fmt::Debug> fmt::Debug for Pacer<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pacer")
            .field("gate", &self.gate)
            .field("probes", &self.probes)
            .field("policy", &self.policy)
            .field("tick", &self.tick)
            .finish_non_exhaustive()
    }
}

impl<P: Policy, S: pacer_builder::State> PacerBuilder<'_, P, S> {
    /// Add a probe on the bottleneck.
    pub fn probe(mut self, probe: &IdleProbe) -> Self {
        self.probes.push(probe.clone());
        self
    }

    /// Add several probes at once, for example one per GPU.
    pub fn probes<'p>(mut self, probes: impl IntoIterator<Item = &'p IdleProbe>) -> Self {
        self.probes.extend(probes.into_iter().cloned());
        self
    }
}

/// What the pacer decided on one tick, and why.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Decision {
    /// The limit before this decision.
    pub limit: usize,
    /// The limit from now on. This can differ from what the policy asked for if the gate had to
    /// cap it.
    pub target: usize,
    /// The data the policy decided from.
    pub sample: Sample,
    /// The policy's own explanation of the decision.
    pub diagnostics: Diagnostics,
}

/// Keeps a [`spawn`](Pacer::spawn)ed pacer running.
///
/// Dropping the handle stops the pacer. The gate keeps whatever limit it had at that point.
#[cfg(feature = "rt")]
#[derive(Debug)]
#[must_use = "dropping the handle stops the pacer"]
pub struct PacerHandle {
    task: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "rt")]
impl PacerHandle {
    /// Whether the pacer has stopped by itself, which only happens if it panicked. The limit
    /// then stays where it was.
    pub fn is_finished(&self) -> bool {
        self.task.is_finished()
    }
}

#[cfg(feature = "rt")]
impl Drop for PacerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}
