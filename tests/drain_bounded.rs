//! `DrainBounded` driven by a simulated pipeline: items are admitted up to the limit, spend a
//! random time upstream, then queue for a bottleneck that works in batches. Time is simulated,
//! so each run takes milliseconds and always gives the same result.
//!
//! Set `SIM_TRACE=1` to print every decision.

// reads the policy's diagnostics; the repo turns the feature on for its own tests, but a
// published copy of the crate doesn't
#![cfg(feature = "diagnostics")]

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    ops::{Range, RangeBounds},
    time::{Duration, Instant},
};

use starve_not::{DrainBounded, Policy, Sample};

/// Seconds between decisions.
const TICK: f64 = 2.0;
/// Items the bottleneck takes at once.
const BATCH: usize = 2;
/// The floor of the policies below.
const FLOOR: usize = 4;
/// The `max` of the policies below, so tests can check that the limit climbed all the way.
const MAX: usize = 1024;

/// The policy most tests use.
fn policy() -> DrainBounded {
    policy_from(FLOOR)
}

/// Like [`policy`], but starting at `initial`.
fn policy_from(initial: usize) -> DrainBounded {
    DrainBounded::builder()
        .floor(FLOOR)
        .max(MAX)
        .initial(initial)
        .drain_target(Duration::from_secs(10))
        .build()
}

/// Values that change over time: `(from second, value)`, in order.
type Changes = Vec<(f64, f64)>;

/// The value in effect at `t`.
fn at(changes: &[(f64, f64)], t: f64) -> Option<f64> {
    changes
        .iter()
        .rev()
        .find(|(from, _)| t >= *from)
        .map(|(_, v)| *v)
}

/// The simulated pipeline. Build one with struct update syntax:
/// `Pipeline { latency: 5.0..10.0, secs: 400.0, ..Pipeline::default() }`.
struct Pipeline {
    /// How long the simulation runs, in seconds.
    secs: f64,
    /// Seconds an item spends upstream, drawn uniformly.
    latency: Range<f64>,
    /// Items per second the bottleneck handles.
    bottleneck_rate: Changes,
    /// Items admitted in this time range fail upstream after 0.2s.
    failing: Option<Range<f64>>,
    /// Upstream hangs in this time range: nothing gets through until it ends.
    hanging: Option<Range<f64>>,
    /// Items per second a stage after the bottleneck handles. Its queue is unbounded and the
    /// probe doesn't see it. Empty: no such stage.
    sink_rate: Changes,
    /// Items the sink takes at once. They leave together.
    sink_batch: usize,
    /// Whether the sink's time per batch is random (exponential, with the same mean) instead of
    /// fixed, so the pace it sets varies by chance.
    is_sink_random: bool,
    /// Seeds the random latencies and sink times.
    seed: u64,
}

impl Default for Pipeline {
    fn default() -> Self {
        Self {
            secs: 300.0,
            latency: 1.0..2.0,
            bottleneck_rate: vec![(0.0, 120.0)],
            failing: None,
            hanging: None,
            sink_rate: Vec::new(),
            sink_batch: 1,
            is_sink_random: false,
            seed: 0x9e37_79b9_7f4a_7c15,
        }
    }
}

/// Deterministic xorshift, so every run sees the same latencies.
struct Rng(u64);

impl Rng {
    /// A number in `0.0..1.0`.
    fn unit(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

/// What happens when an item comes back from upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Event {
    /// It reaches the bottleneck's queue.
    Arrive,
    /// It failed, and leaves the pipeline.
    Fail,
}

/// One decision of the policy.
#[derive(Debug)]
struct Step {
    t: f64,
    limit: usize,
    target: usize,
    residence: f64,
    bound: f64,
}

/// The outcome of a simulation.
struct Run {
    steps: Vec<Step>,
    completed: u64,
    policy: DrainBounded,
}

impl Run {
    /// The decisions that lowered the limit.
    fn shrinks(&self) -> Vec<&Step> {
        self.steps.iter().filter(|s| s.target < s.limit).collect()
    }

    /// How many decisions raised the limit during `during`.
    fn raises(&self, during: impl RangeBounds<f64>) -> usize {
        let steps = self.steps.iter().filter(|s| during.contains(&s.t));
        steps.filter(|s| s.target > s.limit).count()
    }

    /// The share of decisions during `during` that left the limit above `limit`.
    fn share_above(&self, limit: usize, during: impl RangeBounds<f64>) -> f64 {
        let steps: Vec<_> = self
            .steps
            .iter()
            .filter(|s| during.contains(&s.t))
            .collect();
        let above = steps.iter().filter(|s| s.target > limit).count();
        above as f64 / steps.len() as f64
    }

    /// The highest limit set during `during`.
    fn peak(&self, during: impl RangeBounds<f64>) -> usize {
        let steps = self.steps.iter().filter(|s| during.contains(&s.t));
        steps.map(|s| s.target).max().unwrap()
    }

    /// The limit in effect at `t`.
    fn limit_at(&self, t: f64) -> usize {
        self.steps
            .iter()
            .rfind(|s| s.t <= t)
            .map_or(0, |s| s.target)
    }

    fn last(&self) -> &Step {
        self.steps.last().unwrap()
    }
}

/// Simulated seconds as whole microseconds, so event times can be ordered in a heap.
fn micros(t: f64) -> u64 {
    (t * 1e6).round() as u64
}

/// Run `pipeline` under `policy`, jumping from one event to the next.
fn run(pipeline: &Pipeline, mut policy: DrainBounded) -> Run {
    let is_tracing = std::env::var_os("SIM_TRACE").is_some();
    let origin = Instant::now();
    let mut rng = Rng(pipeline.seed);
    let mut limit = policy.initial();
    let mut in_flight = 0usize;
    // items upstream, by when they come back, soonest first
    let mut upstream = BinaryHeap::<Reverse<(u64, Event)>>::new();
    // items waiting for the bottleneck, and the batch on it: (when it finishes, size)
    let mut waiting = 0usize;
    let mut batch: Option<(f64, usize)> = None;
    // items waiting for the sink, and the batch on it: (when it finishes, size)
    let mut sink_waiting = 0usize;
    let mut sink_busy: Option<(f64, usize)> = None;
    // what the next sample reports, counted since the last tick
    let (mut completed, mut released, mut idle) = (0u64, 0u64, 0.0f64);
    let mut total_completed = 0u64;
    let (mut t, mut last_tick) = (0.0f64, 0.0f64);
    let mut steps = Vec::new();

    while t < pipeline.secs {
        // 1. Admit items up to the limit and send them upstream.
        while in_flight < limit {
            in_flight += 1;
            let is_failing = pipeline.failing.as_ref().is_some_and(|f| f.contains(&t));
            let (after, event) = match is_failing {
                true => (0.2, Event::Fail),
                false => {
                    let hang = match &pipeline.hanging {
                        Some(h) if h.contains(&t) => h.end - t,
                        _ => 0.0,
                    };
                    let span = pipeline.latency.end - pipeline.latency.start;
                    let latency = pipeline.latency.start + rng.unit() * span;
                    (hang + latency, Event::Arrive)
                }
            };
            upstream.push(Reverse((micros(t + after), event)));
        }

        // 2. A hang also holds back what was already on its way.
        if let Some(h) = &pipeline.hanging
            && h.contains(&t)
        {
            let held = std::mem::take(&mut upstream)
                .into_iter()
                .map(|Reverse((at, event))| match event {
                    Event::Arrive => Reverse((at.max(micros(h.end)), event)),
                    Event::Fail => Reverse((at, event)),
                });
            upstream = held.collect();
        }

        // 3. Start the bottleneck and the sink if they are free and have work.
        if batch.is_none() && waiting > 0 {
            let n = waiting.min(BATCH);
            waiting -= n;
            let rate = at(&pipeline.bottleneck_rate, t).unwrap();
            batch = Some((t + n as f64 / rate, n));
        }
        if let Some(rate) = at(&pipeline.sink_rate, t)
            && sink_busy.is_none()
            && sink_waiting > 0
        {
            let n = sink_waiting.min(pipeline.sink_batch);
            sink_waiting -= n;
            let time = match pipeline.is_sink_random {
                true => -(1.0 - rng.unit()).ln() * n as f64 / rate,
                false => n as f64 / rate,
            };
            sink_busy = Some((t + time, n));
        }

        // 4. Jump to the next event. Without a batch the bottleneck is idle until then.
        let next_tick = last_tick + TICK;
        let next_upstream = upstream
            .peek()
            .map_or(f64::INFINITY, |Reverse((at, _))| *at as f64 / 1e6);
        let next_batch = batch.map_or(f64::INFINITY, |(at, _)| at);
        let next_sink = sink_busy.map_or(f64::INFINITY, |(at, _)| at);
        let next = next_tick.min(next_upstream).min(next_batch).min(next_sink);
        if batch.is_none() {
            idle += next - t;
        }
        t = next;

        // 5. Handle it. On a tie the bottleneck goes first, then the sink, upstream, the tick.
        if next == next_batch {
            let (_, n) = batch.take().unwrap();
            match pipeline.sink_rate.is_empty() {
                false => sink_waiting += n,
                true => {
                    completed += n as u64;
                    total_completed += n as u64;
                    in_flight -= n;
                }
            }
        } else if next == next_sink {
            let (_, n) = sink_busy.take().unwrap();
            completed += n as u64;
            total_completed += n as u64;
            in_flight -= n;
        } else if next == next_upstream {
            let Reverse((_, event)) = upstream.pop().unwrap();
            match event {
                Event::Arrive => waiting += 1,
                Event::Fail => {
                    released += 1;
                    in_flight -= 1;
                }
            }
        } else {
            // a tick: show the policy what happened since the last one
            let elapsed = Duration::from_secs_f64(t - last_tick);
            let at = origin + Duration::from_secs_f64(t);
            let mut sample = Sample::new(at, elapsed, in_flight, limit);
            sample.completed = completed;
            sample.released = released;
            sample.idle.push(Duration::from_secs_f64(idle));
            let target = policy.decide(&sample);

            let diagnostics = policy.diagnostics();
            if is_tracing {
                println!(
                    "{t:6.1} limit={limit:<5} target={target:<5} in_flight={in_flight:<5} {diagnostics}"
                );
            }
            let get = |name| {
                diagnostics
                    .iter()
                    .find(|(n, _)| *n == name)
                    .map_or(f64::INFINITY, |(_, v)| v)
            };
            steps.push(Step {
                t,
                limit,
                target,
                residence: get("residence"),
                bound: get("bound"),
            });

            limit = target;
            (completed, released, idle) = (0, 0, 0.0);
            last_tick = t;
        }
    }

    Run {
        steps,
        completed: total_completed,
        policy,
    }
}

/// The pipeline from issue #1: items spend 8 to 16 seconds upstream, far longer than the
/// bottleneck needs. The limit has to climb all the way instead of shrinking after each growth.
#[test]
fn grows_through_long_upstream_latency() {
    let pipeline = Pipeline {
        latency: 8.0..16.0,
        secs: 300.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy());

    let shrinks = run.shrinks();
    assert!(shrinks.is_empty(), "{shrinks:?}");
    assert_eq!(run.limit_at(300.0), MAX);
    assert!(run.completed > 10_000, "only {} completed", run.completed);
}

/// Items take longer than `drain_target` even at their fastest, and longer than `max_window`:
/// the policy must learn their pace instead of squeezing them, and must not mistake the gap
/// between two waves of departures for a stall.
#[test]
fn slow_by_nature_is_not_squeezed() {
    for latency in [15.0..25.0, 35.0..45.0] {
        let pipeline = Pipeline {
            latency: latency.clone(),
            secs: 900.0,
            ..Pipeline::default()
        };
        let run = run(&pipeline, policy());

        let shrinks = run.shrinks();
        assert!(shrinks.is_empty(), "{latency:?}: {shrinks:?}");
        assert_eq!(run.limit_at(900.0), MAX, "{latency:?}");
    }
}

/// When the bottleneck slows down, the queue in front of it grows, and the limit must come down
/// to what drains within the bound.
#[test]
fn shrinks_when_the_bottleneck_slows() {
    let pipeline = Pipeline {
        latency: 5.0..10.0,
        bottleneck_rate: vec![(0.0, 120.0), (200.0, 20.0)],
        secs: 400.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy());

    assert_eq!(run.limit_at(200.0), MAX);
    // 20 items/s for at most 1.5 × the ~8s it takes when fed, plus some slack
    let limit = run.limit_at(400.0);
    assert!((100..=400).contains(&limit), "limit {limit}");
    let last = run.last();
    assert!(last.residence <= last.bound, "{last:?}");
}

/// A fast pipeline that becomes bottleneck-bound must end up draining within `drain_target`.
#[test]
fn fast_pipeline_drains_in_time() {
    let pipeline = Pipeline {
        latency: 0.2..0.3,
        bottleneck_rate: vec![(0.0, 120.0), (60.0, 20.0)],
        secs: 120.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy());

    let last = run.last();
    assert!(last.residence <= 10.0, "{last:?}");
}

/// `initial` must survive the first decisions, before any item has had time to leave.
#[test]
fn initial_limit_is_kept_at_startup() {
    let policy = DrainBounded::builder()
        .floor(32)
        .max(MAX)
        .initial(256)
        .drain_target(Duration::from_secs(10))
        .build();
    let pipeline = Pipeline {
        latency: 5.0..5.0,
        secs: 20.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy);

    assert!(run.steps.iter().all(|s| s.target >= 256), "{:?}", run.steps);
}

/// Items that fail fast say how long failing takes, not how long working does: an outage must
/// not teach the policy a fastest time that squeezes the pipeline once it recovers.
#[test]
fn fast_failures_do_not_set_the_fastest_time() {
    let pipeline = Pipeline {
        latency: 15.0..25.0,
        failing: Some(300.0..360.0),
        secs: 600.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy());

    let fastest = run.policy.fastest().unwrap();
    assert!(fastest >= Duration::from_secs(10), "fastest {fastest:?}");
    assert_eq!(run.limit_at(600.0), MAX);
}

/// During a raise trial the bound is widened, but by no more than one raise's growth.
#[test]
fn raise_trial_widening_does_not_compound() {
    let policy = DrainBounded::builder()
        .floor(FLOOR)
        .max(4096)
        .drain_target(Duration::from_secs(150))
        .build();
    let pipeline = Pipeline {
        latency: 60.0..120.0,
        secs: 1200.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy);

    // one raise's growth (2) × `max_slowdown` (1.5) × the slowest pass (120s), with 20% slack
    // for the fastest time creeping up
    let widest = 2.0 * 1.5 * 120.0 * 1.2;
    for step in &run.steps {
        assert!(step.bound.is_infinite() || step.bound <= widest, "{step:?}");
    }
    let shrinks = run.shrinks();
    assert!(shrinks.is_empty(), "{shrinks:?}");
}

/// When upstream hangs, the bottleneck starves while the gate stays full. That must not read as
/// a hungry bottleneck: at most one raise before nothing has left for long enough, then the
/// limit comes down instead of up.
#[test]
fn does_not_grow_through_a_hang() {
    let pipeline = Pipeline {
        latency: 0.5..1.0,
        hanging: Some(100.0..160.0),
        secs: 200.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy());

    let before = run.limit_at(100.0);
    let peak = run.peak(100.0..160.0);
    assert!(
        peak <= before * 2,
        "grew from {before} to {peak} while hung"
    );
    let after = run.limit_at(160.0);
    assert!(after <= before, "{after} after the hang");
}

/// Upstream stalls after a single item made it through, before any latency could be learned.
/// That one old completion must not keep authorising raises, and the stall must bring the limit
/// back down.
#[test]
fn early_stall_does_not_keep_growing() {
    let mut policy = policy();
    let origin = Instant::now();
    let tick = Duration::from_secs(2);
    let mut limit = policy.initial();
    let mut peak = limit;
    // the gate stays full and the bottleneck waits; one item leaves in the first tick only
    for i in 1..=300u32 {
        let mut sample = Sample::new(origin + tick * i, tick, limit, limit);
        sample.completed = u64::from(i == 1);
        sample.idle.push(tick);
        limit = policy.decide(&sample);
        if std::env::var_os("SIM_TRACE").is_some() {
            println!("{:6} limit={limit:<5} {}", i * 2, policy.diagnostics());
        }
        peak = peak.max(limit);
    }

    // one raise at most: another needs a success after it
    assert!(peak <= FLOOR * 2, "grew to {peak} on one completion");
    assert_eq!(limit, FLOOR);
}

/// Starting high on a pipeline slower than `drain_target`, the fill transient (everything
/// inside, nothing out yet) must not be taken for a pipeline that can't drain.
#[test]
fn startup_fill_is_not_taken_for_overload() {
    let pipeline = Pipeline {
        latency: 15.0..25.0,
        bottleneck_rate: vec![(0.0, 20.0)],
        secs: 1200.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy_from(512));

    // 20 items/s for 1200s, less the first pass
    assert!(run.completed > 20_000, "only {} completed", run.completed);
}

/// A fastest time inflated by startup must not stick once the bottleneck is busy: when the
/// bottleneck later slows, the limit still has to come down.
#[test]
fn startup_does_not_inflate_the_fastest_time() {
    let pipeline = Pipeline {
        latency: 35.0..45.0,
        bottleneck_rate: vec![(0.0, 10.0), (400.0, 1.0)],
        secs: 1200.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy_from(512));

    let fastest = run.policy.fastest().unwrap();
    assert!(fastest < Duration::from_secs(200), "fastest {fastest:?}");
    // 1 item/s for at most 1.5 × the fastest time
    let limit = run.limit_at(1200.0);
    assert!(limit <= 400, "limit {limit}");
}

/// Upstream starts failing fast. Old successes must not keep authorising raises: failures are
/// not a hungry bottleneck.
#[test]
fn does_not_grow_through_fast_failures() {
    let pipeline = Pipeline {
        latency: 0.5..1.0,
        failing: Some(100.0..160.0),
        secs: 200.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy());

    let before = run.limit_at(100.0);
    let peak = run.peak(100.0..160.0);
    assert!(
        peak <= before * 2,
        "grew from {before} to {peak} while failing"
    );
}

/// One item leaves every 20 seconds, fewer than a window's worth. The policy must still learn
/// the pace, so that when the bottleneck slows further the limit comes down.
#[test]
fn learns_the_pace_from_sparse_departures() {
    let pipeline = Pipeline {
        latency: 1.0..1.0,
        bottleneck_rate: vec![(0.0, 0.05), (2000.0, 0.02)],
        secs: 6000.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy_from(64));

    assert!(run.policy.fastest().is_some(), "never learned the pace");
    // steady at about 64 (a few departures can trim it slightly while the window refills)
    let before = run.limit_at(2000.0);
    assert!(before >= 58, "limit {before} before the bottleneck slowed");
    let after = run.limit_at(6000.0);
    assert!(
        after <= before * 3 / 4,
        "limit {after} after the bottleneck slowed"
    );
}

/// Issue #4: the items queue after the bottleneck, where the probe can't see them. The
/// bottleneck keeps waiting, but more items only lengthen the queue: the limit must come back
/// to what drains within the bound, or the floor if even that takes longer. (A raise made before
/// the pace is known can't be checked, so the bound may include one raise's worth of queue.)
#[test]
fn does_not_grow_into_a_hidden_queue() {
    for sink_rate in [0.1, 0.15, 0.2, 0.5, 1.0, 5.0, 20.0] {
        let pipeline = Pipeline {
            latency: 1.0..2.0,
            sink_rate: vec![(0.0, sink_rate)],
            secs: 3000.0,
            ..Pipeline::default()
        };
        let run = run(&pipeline, policy());

        let end = run.limit_at(3000.0);
        let last = run.last();
        assert!(
            last.residence <= last.bound || end == FLOOR,
            "sink rate {sink_rate}: limit {end}, {last:?}"
        );
        // one raise past it to find out that more doesn't help
        let peak = run.peak(..);
        assert!(
            peak <= end * 2,
            "sink rate {sink_rate}: grew to {peak}, ended at {end}"
        );
    }
}

/// Issue #4 as observed: every item waits ~30s upstream, so about 0.13 items/s leave at the
/// floor. Raising does help here, but only once the pace is known: until then the limit must
/// not grow past `drain_target`, and afterwards residence stays within the bound, so a shutdown
/// drains in about the bound rather than minutes.
#[test]
fn learns_the_bound_at_low_rates() {
    let pipeline = Pipeline {
        latency: 28.0..32.0,
        secs: 1200.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, policy());

    let learned = run
        .steps
        .iter()
        .find(|s| s.bound.is_finite())
        .expect("bound never learned");
    assert!(learned.t <= 300.0, "bound learned only at {}s", learned.t);
    // no raise before it: 4 items for 30s is already past the 10s target
    let mut before = run.steps.iter().filter(|s| s.t < learned.t);
    assert!(
        before.all(|s| s.target == FLOOR),
        "grew before the bound was known"
    );
    let fastest = run.policy.fastest().unwrap().as_secs_f64();
    assert!((25.0..=35.0).contains(&fastest), "fastest {fastest}");
    let last = run.last();
    assert!(last.residence <= last.bound, "{last:?}");
    assert_eq!(run.limit_at(1200.0), MAX);
}

/// Items take anywhere from 1 to 120 seconds, and few leave in any window. A short residence
/// reading doesn't mean a pass is short: until the pace is known, raises must not be taken back,
/// or the policy keeps resetting and never learns it.
#[test]
fn learns_the_pace_through_widely_varying_latency() {
    let pipeline = Pipeline {
        latency: 1.0..120.0,
        secs: 1800.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, DrainBounded::builder().max(MAX).build());

    assert!(run.policy.fastest().is_some(), "never learned the pace");
    assert_eq!(run.limit_at(1800.0), MAX);
}

/// After a raise into a hidden queue is taken back, the departures of the taken-back items must
/// not read as a change of pace and let the same raise in again. A real change must, though:
/// here the stage behind the bottleneck gets four times faster.
#[test]
fn useless_raise_waits_until_the_pace_speeds_up() {
    let pipeline = Pipeline {
        latency: 8.0..8.0,
        sink_rate: vec![(0.0, 0.15), (2000.0, 0.6)],
        secs: 4000.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, DrainBounded::builder().max(MAX).build());

    assert!(run.policy.fastest().is_some(), "never learned the pace");
    // once settled, the limit holds until the sink speeds up
    let settled = run.steps.iter().filter(|s| (600.0..2000.0).contains(&s.t));
    let changes: Vec<_> = settled.filter(|s| s.target != s.limit).collect();
    assert!(changes.is_empty(), "{changes:?}");
    let before = run.limit_at(2000.0);
    let after = run.limit_at(4000.0);
    assert!(after > before, "stayed at {after} after the sink sped up");
}

/// The same, but the stage behind the bottleneck gets four times slower. That changes the pace
/// too, so the raise may be tried again. It is just as useless, though: each retry must be taken
/// back, and they must stay rare.
#[test]
fn useless_raise_is_taken_back_after_the_pace_slows_down() {
    let pipeline = Pipeline {
        latency: 8.0..8.0,
        sink_rate: vec![(0.0, 0.6), (2000.0, 0.15)],
        secs: 20_000.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, DrainBounded::builder().max(MAX).build());

    let settled = run.limit_at(1500.0);
    let retries = run.raises(2000.0..);
    assert!(retries <= 3, "{retries} retries after the sink slowed down");
    let above = run.share_above(settled, 2000.0..);
    assert!(
        above < 0.05,
        "above {settled} {:.0}% of the time",
        above * 100.0
    );
}

/// Behind the bottleneck sits a stage whose pace varies by chance. Chance alone must rarely end
/// the wait after a useless raise, or the policy would try the same raise again and again: the
/// retries must stay about as rare as the doubling waits make them, and each be taken back.
#[test]
fn useless_raise_is_not_retried_by_chance() {
    let pipeline = Pipeline {
        latency: 2.0..4.0,
        sink_rate: vec![(0.0, 0.5)],
        is_sink_random: true,
        secs: 20_000.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, DrainBounded::builder().max(MAX).build());

    // the first useless raise is taken back by 300s. The waits alone, doubling from about 600s,
    // end five times in 20 000s
    let retries = run.raises(300.0..);
    assert!(retries <= 8, "{retries} retries");
    let settled = run.limit_at(300.0);
    let above = run.share_above(settled, 300.0..);
    assert!(
        above < 0.05,
        "above {settled} {:.0}% of the time",
        above * 100.0
    );
}

/// After a useless raise, upstream hangs and the limit drops to the floor. There the limit sets
/// the pace, so the pace never changes: waiting for it to change would keep the limit at the
/// floor for good.
#[test]
fn useless_raise_is_forgotten_after_a_hang() {
    let pipeline = Pipeline {
        latency: 8.0..8.0,
        sink_rate: vec![(0.0, 0.5)],
        hanging: Some(2000.0..2100.0),
        secs: 4000.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, DrainBounded::builder().max(MAX).build());

    // the sink takes 0.5 items/s and an item needs 8s to reach it: 4 items keep it busy
    let after = run.limit_at(4000.0);
    assert!(after >= 4, "stayed at {after} after the hang");
}

/// Behind the bottleneck sits a stage that works like clockwork, and it gets 30% faster. Its
/// pace hardly varies by chance, so the policy must learn that and notice the change: the
/// useless raise is tried again right away, not only once the wait runs out.
#[test]
fn useless_raise_notices_a_small_change_of_a_steady_pace() {
    let pipeline = Pipeline {
        latency: 8.0..8.0,
        sink_rate: vec![(0.0, 0.5), (3000.0, 0.65)],
        secs: 6000.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, DrainBounded::builder().max(MAX).build());

    // the waits run out at about 900s and 2200s, then not before about 4800s
    assert_eq!(run.raises(2300.0..3000.0), 0, "retried while the pace held");
    assert!(
        run.raises(3000.0..3400.0) > 0,
        "never noticed the pace change"
    );
}

/// Behind the bottleneck sits a stage that sends items out in batches of 10, at random times, so
/// its pace varies far more by chance than if items left one by one. The policy must learn that:
/// rarely take chance for a change of pace, and not let the limit creep up into the queue
/// behind the bottleneck, which only makes shutdown longer.
#[test]
fn useless_raise_waits_through_chance_in_batches() {
    let seeds = [
        0x9e37_79b9_7f4a_7c15,
        0xdead_beef_cafe_f00d,
        0x0bad_c0de_1234_4321,
        0x0f0f_f0f0_1357_2468,
    ];
    for seed in seeds {
        let pipeline = Pipeline {
            latency: 2.0..4.0,
            sink_rate: vec![(0.0, 0.5)],
            sink_batch: 10,
            is_sink_random: true,
            secs: 20_000.0,
            seed,
            ..Pipeline::default()
        };
        let run = run(&pipeline, DrainBounded::builder().max(MAX).build());

        // the pace never changes here: retries come from the waits running out, which takes
        // longer the more the pace varies by chance, and rarely from chance itself
        let retries = run.raises(1000.0..);
        assert!(retries <= 8, "seed {seed:x}: {retries} retries");
        // a retry kept by chance now and then is one step up at most; it used to climb to 8 to 64
        // times the settled limit
        let settled = run.limit_at(1000.0);
        let late = run.peak(10_000.0..);
        assert!(
            late <= settled * 2,
            "seed {seed:x}: from {settled} to {late}"
        );
        // the sink takes 0.5 items/s: 10 000 in all, less the start
        assert!(
            run.completed > 9_000,
            "seed {seed:x}: only {} completed",
            run.completed
        );
    }
}

/// Upstream hangs for a moment while a raise is on trial. The hang, not the raise, keeps
/// departures down, so the raise must not be judged useless for good: the limit must still
/// climb all the way once the hang is over.
#[test]
fn hang_during_a_raise_trial_does_not_pin_the_limit() {
    for start in [100.0, 120.0, 140.0, 160.0] {
        let pipeline = Pipeline {
            latency: 15.0..25.0,
            hanging: Some(start..start + 24.0),
            secs: 4000.0,
            ..Pipeline::default()
        };
        let run = run(&pipeline, policy());

        let end = run.limit_at(4000.0);
        assert_eq!(end, MAX, "hang at {start}s: ended at {end}");
    }
}

/// Behind the bottleneck sits a stage that works like clockwork: its pace never changes, so
/// a useless raise stays useless. The wait after it still ends now and then, in case the raise
/// was judged wrongly, but each retry that turns out useless doubles the next wait.
#[test]
fn useless_raise_is_retried_less_and_less() {
    let pipeline = Pipeline {
        latency: 8.0..8.0,
        sink_rate: vec![(0.0, 0.5)],
        secs: 40_000.0,
        ..Pipeline::default()
    };
    let run = run(&pipeline, DrainBounded::builder().max(MAX).build());

    // the first useless raise is taken back by 300s
    let retries = run
        .steps
        .iter()
        .filter(|s| s.t > 300.0 && s.target > s.limit);
    let retries: Vec<f64> = retries.map(|s| s.t).collect();
    assert!(retries.len() >= 3, "retried only at {retries:?}");
    for gaps in retries.windows(3) {
        let (first, second) = (gaps[1] - gaps[0], gaps[2] - gaps[1]);
        assert!(second >= first * 1.8, "retried at {retries:?}");
    }
    // and each retry was taken back
    assert_eq!(run.limit_at(40_000.0), run.limit_at(300.0));
}
