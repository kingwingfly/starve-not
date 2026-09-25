//! `DrainBounded` driven by a simulated pipeline: items are admitted up to the limit, spend a
//! random time upstream, then queue for a bottleneck that works in batches. Time is simulated,
//! so each run takes milliseconds and always gives the same result.
//!
//! Set `SIM_TRACE=1` to print every decision.

use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    ops::Range,
    time::{Duration, Instant},
};

use starve_not::{DrainBounded, Policy, Sample};

const TICK: f64 = 2.0;
const BATCH: usize = 2;

/// Deterministic xorshift, so every run sees the same latencies.
struct Rng(u64);

impl Rng {
    fn unit(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

struct Pipeline {
    /// seconds an item spends upstream, drawn uniformly
    latency: Range<f64>,
    /// items per second the bottleneck handles, from the given time on
    rate: Vec<(f64, f64)>,
    /// items admitted in this time range fail upstream after 0.2s
    failing: Option<Range<f64>>,
    /// upstream hangs in this time range: nothing gets through until it ends
    hanging: Option<Range<f64>>,
    /// items per second a stage after the bottleneck handles, one at a time; its queue is
    /// unbounded and the probe doesn't see it
    sink: Option<f64>,
    secs: f64,
}

impl Pipeline {
    fn new(latency: Range<f64>, rate: f64, secs: f64) -> Self {
        Self {
            latency,
            rate: vec![(0.0, rate)],
            failing: None,
            hanging: None,
            sink: None,
            secs,
        }
    }

    fn rate_at(&self, t: f64) -> f64 {
        self.rate
            .iter()
            .rev()
            .find(|(from, _)| t >= *from)
            .unwrap()
            .1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Event {
    Arrive,
    Fail,
}

#[derive(Debug)]
struct Step {
    t: f64,
    limit: usize,
    target: usize,
    residence: f64,
    bound: f64,
}

struct Run {
    steps: Vec<Step>,
    completed: u64,
    policy: DrainBounded,
}

impl Run {
    fn shrinks(&self, during: Range<f64>) -> Vec<&Step> {
        let steps = self.steps.iter().filter(|s| during.contains(&s.t));
        steps.filter(|s| s.target < s.limit).collect()
    }

    fn limit_at(&self, t: f64) -> usize {
        self.steps
            .iter()
            .rfind(|s| s.t <= t)
            .map_or(0, |s| s.target)
    }
}

fn micros(t: f64) -> u64 {
    (t * 1e6).round() as u64
}

fn run(pipeline: &Pipeline, mut policy: DrainBounded) -> Run {
    let trace = std::env::var_os("SIM_TRACE").is_some();
    let origin = Instant::now();
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut limit = policy.initial();
    let mut in_flight = 0usize;
    let mut upstream = BinaryHeap::<Reverse<(u64, Event)>>::new();
    let mut queue = 0usize;
    // the batch on the bottleneck and when it finishes
    let mut busy: Option<(f64, usize)> = None;
    // items waiting for the sink, and when the one on it finishes
    let mut sink_queue = 0usize;
    let mut sink_busy: Option<f64> = None;
    let (mut completed, mut released, mut idle) = (0u64, 0u64, 0.0f64);
    let mut total = 0u64;
    let (mut t, mut last_tick) = (0.0f64, 0.0f64);
    let mut steps = Vec::new();
    while t < pipeline.secs {
        while in_flight < limit {
            in_flight += 1;
            let failing = pipeline.failing.as_ref().is_some_and(|f| f.contains(&t));
            let (after, event) = match failing {
                true => (0.2, Event::Fail),
                false => {
                    let span = pipeline.latency.end - pipeline.latency.start;
                    let hang = match &pipeline.hanging {
                        Some(h) if h.contains(&t) => h.end - t,
                        _ => 0.0,
                    };
                    (
                        hang + pipeline.latency.start + rng.unit() * span,
                        Event::Arrive,
                    )
                }
            };
            upstream.push(Reverse((micros(t + after), event)));
        }
        // a hang also holds back what was already on its way
        if let Some(h) = &pipeline.hanging
            && h.contains(&t)
        {
            let held = std::mem::take(&mut upstream)
                .into_iter()
                .map(|Reverse((at, e))| {
                    let at = match e {
                        Event::Arrive => at.max(micros(h.end)),
                        Event::Fail => at,
                    };
                    Reverse((at, e))
                });
            upstream = held.collect();
        }
        if busy.is_none() && queue > 0 {
            let n = queue.min(BATCH);
            queue -= n;
            busy = Some((t + n as f64 / pipeline.rate_at(t), n));
        }
        if let Some(rate) = pipeline.sink
            && sink_busy.is_none()
            && sink_queue > 0
        {
            sink_queue -= 1;
            sink_busy = Some(t + 1.0 / rate);
        }
        let next_tick = last_tick + TICK;
        let next_upstream = upstream
            .peek()
            .map_or(f64::INFINITY, |Reverse((at, _))| *at as f64 / 1e6);
        let next_done = busy.map_or(f64::INFINITY, |(at, _)| at);
        let next_sink = sink_busy.unwrap_or(f64::INFINITY);
        let next = next_tick.min(next_upstream).min(next_done).min(next_sink);
        if busy.is_none() {
            idle += next - t;
        }
        t = next;
        if next == next_done {
            let (_, n) = busy.take().unwrap();
            match pipeline.sink {
                Some(_) => sink_queue += n,
                None => {
                    completed += n as u64;
                    total += n as u64;
                    in_flight -= n;
                }
            }
        } else if next == next_sink {
            sink_busy = None;
            completed += 1;
            total += 1;
            in_flight -= 1;
        } else if next == next_upstream {
            let Reverse((_, event)) = upstream.pop().unwrap();
            match event {
                Event::Arrive => queue += 1,
                Event::Fail => {
                    released += 1;
                    in_flight -= 1;
                }
            }
        } else {
            let elapsed = Duration::from_secs_f64(t - last_tick);
            let at = origin + Duration::from_secs_f64(t);
            let mut sample = Sample::new(at, elapsed, in_flight, limit);
            sample.completed = completed;
            sample.released = released;
            sample.idle.push(Duration::from_secs_f64(idle));
            let target = policy.decide(&sample);
            let d = policy.diagnostics();
            let get = |name| {
                d.iter()
                    .find(|(n, _)| *n == name)
                    .map_or(f64::INFINITY, |(_, v)| v)
            };
            if trace {
                println!(
                    "{t:6.1} limit={limit:<5} target={target:<5} in_flight={in_flight:<5} {d}"
                );
            }
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
        completed: total,
        policy,
    }
}

fn policy() -> DrainBounded {
    DrainBounded::builder()
        .floor(4)
        .drain_target(Duration::from_secs(10))
        .build()
}

/// The pipeline from issue #1: items spend 8 to 16 seconds upstream, far longer than the
/// bottleneck needs. The limit has to climb all the way instead of shrinking after each growth.
#[test]
fn grows_through_long_upstream_latency() {
    let run = run(&Pipeline::new(8.0..16.0, 120.0, 300.0), policy());
    assert_eq!(
        run.shrinks(0.0..300.0).len(),
        0,
        "{:?}",
        run.shrinks(0.0..300.0)
    );
    assert_eq!(run.limit_at(300.0), 1024);
    assert!(run.completed > 10_000, "only {} completed", run.completed);
}

/// Items take longer than `drain_target` even at their fastest, and longer than `max_window`:
/// the policy must learn their pace instead of squeezing them, and must not mistake the gap
/// between two waves of departures for a stall.
#[test]
fn slow_by_nature_is_not_squeezed() {
    for latency in [15.0..25.0, 35.0..45.0] {
        let run = run(&Pipeline::new(latency.clone(), 120.0, 900.0), policy());
        let shrinks = run.shrinks(0.0..900.0);
        assert_eq!(shrinks.len(), 0, "{latency:?}: {shrinks:?}");
        assert_eq!(run.limit_at(900.0), 1024, "{latency:?}");
    }
}

/// When the bottleneck slows down, the queue in front of it grows, and the limit must come down
/// to what drains within the bound.
#[test]
fn shrinks_when_the_bottleneck_slows() {
    let mut pipeline = Pipeline::new(5.0..10.0, 120.0, 400.0);
    pipeline.rate.push((200.0, 20.0));
    let run = run(&pipeline, policy());
    assert_eq!(run.limit_at(200.0), 1024);
    let limit = run.limit_at(400.0);
    // 20 items/s for at most 1.5 × the ~8s it takes when fed, plus some slack
    assert!((100..=400).contains(&limit), "limit {limit}");
    let last = run.steps.last().unwrap();
    assert!(last.residence <= last.bound, "{last:?}");
}

/// A fast pipeline that becomes bottleneck-bound must end up draining within `drain_target`.
#[test]
fn fast_pipeline_drains_in_time() {
    let mut pipeline = Pipeline::new(0.2..0.3, 120.0, 120.0);
    pipeline.rate.push((60.0, 20.0));
    let run = run(&pipeline, policy());
    let last = run.steps.last().unwrap();
    assert!(last.residence <= 10.0, "{last:?}");
}

/// `initial` must survive the first decisions, before any item has had time to leave.
#[test]
fn initial_limit_is_kept_at_startup() {
    let policy = DrainBounded::builder()
        .floor(32)
        .initial(256)
        .drain_target(Duration::from_secs(10))
        .build();
    let run = run(&Pipeline::new(5.0..5.0, 120.0, 20.0), policy);
    assert!(run.steps.iter().all(|s| s.target >= 256), "{:?}", run.steps);
}

/// Items that fail fast say how long failing takes, not how long working does: an outage must
/// not teach the policy a fastest time that squeezes the pipeline once it recovers.
#[test]
fn fast_failures_do_not_set_the_fastest_time() {
    let mut pipeline = Pipeline::new(15.0..25.0, 120.0, 600.0);
    pipeline.failing = Some(300.0..360.0);
    let run = run(&pipeline, policy());
    let fastest = run.policy.fastest().unwrap();
    assert!(fastest >= Duration::from_secs(10), "fastest {fastest:?}");
    assert_eq!(run.limit_at(600.0), 1024);
}

/// While a raise ramps up the bound is widened, but by no more than one raise's growth.
#[test]
fn ramp_widening_does_not_compound() {
    let policy = DrainBounded::builder()
        .floor(4)
        .max(4096)
        .drain_target(Duration::from_secs(150))
        .build();
    let run = run(&Pipeline::new(60.0..120.0, 120.0, 1200.0), policy);
    for step in &run.steps {
        // the plain bound is at least 150s; 2× that is the most one raise may add
        assert!(
            step.bound.is_infinite() || step.bound <= 2.0 * 1.5 * 120.0 * 1.2,
            "{step:?}"
        );
    }
    assert_eq!(run.shrinks(0.0..1200.0).len(), 0);
}

/// When upstream hangs, the bottleneck starves while the gate stays full. That must not read as
/// a hungry bottleneck: at most one raise before nothing has left for long enough, then the
/// limit comes down instead of up.
#[test]
fn does_not_grow_through_a_hang() {
    let mut pipeline = Pipeline::new(0.5..1.0, 120.0, 200.0);
    pipeline.hanging = Some(100.0..160.0);
    let run = run(&pipeline, policy());
    let before = run.limit_at(100.0);
    let peak = run.steps.iter().filter(|s| (100.0..160.0).contains(&s.t));
    let peak = peak.map(|s| s.target).max().unwrap();
    assert!(
        peak <= before * 2,
        "grew from {before} to {peak} while hung"
    );
    assert!(
        run.limit_at(160.0) <= before,
        "{} after the hang",
        run.limit_at(160.0)
    );
}

/// Upstream stalls after a single item made it through, before any latency could be learned.
/// That one old completion must not keep authorising raises, and the stall must bring the limit
/// back down.
#[test]
fn early_stall_does_not_keep_growing() {
    let mut policy = policy();
    let origin = Instant::now();
    let mut limit = policy.initial();
    let tick = Duration::from_secs(2);
    let mut peak = limit;
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
    assert!(peak <= 8, "grew to {peak} on one completion");
    assert_eq!(limit, 4);
}

/// Starting high on a pipeline slower than `drain_target`, the fill transient (everything
/// inside, nothing out yet) must not be taken for a pipeline that can't drain.
#[test]
fn startup_fill_is_not_taken_for_overload() {
    let policy = DrainBounded::builder()
        .floor(4)
        .initial(512)
        .drain_target(Duration::from_secs(10))
        .build();
    let run = run(&Pipeline::new(15.0..25.0, 20.0, 1200.0), policy);
    // 20 items/s for 1200s, less the first pass
    assert!(run.completed > 20_000, "only {} completed", run.completed);
}

/// A fastest time inflated by startup must not stick once the bottleneck is busy: when the
/// bottleneck later slows, the limit still has to come down.
#[test]
fn startup_does_not_inflate_the_fastest_time() {
    let policy = DrainBounded::builder()
        .floor(4)
        .initial(512)
        .drain_target(Duration::from_secs(10))
        .build();
    let mut pipeline = Pipeline::new(35.0..45.0, 10.0, 1200.0);
    pipeline.rate.push((400.0, 1.0));
    let run = run(&pipeline, policy);
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
    let mut pipeline = Pipeline::new(0.5..1.0, 120.0, 200.0);
    pipeline.failing = Some(100.0..160.0);
    let run = run(&pipeline, policy());
    let before = run.limit_at(100.0);
    let during = run.steps.iter().filter(|s| (100.0..160.0).contains(&s.t));
    let peak = during.map(|s| s.target).max().unwrap();
    assert!(
        peak <= before * 2,
        "grew from {before} to {peak} while failing"
    );
}

/// One item leaves every 20 seconds, fewer than a window's worth. The policy must still learn
/// the pace, so that when the bottleneck slows further the limit comes down.
#[test]
fn learns_the_pace_from_sparse_departures() {
    let policy = DrainBounded::builder()
        .floor(4)
        .initial(64)
        .drain_target(Duration::from_secs(10))
        .build();
    let mut pipeline = Pipeline::new(1.0..1.0, 0.05, 6000.0);
    pipeline.rate.push((2000.0, 0.02));
    let run = run(&pipeline, policy);
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
/// to what drains within `drain_target`, or the floor if even that takes longer.
#[test]
fn does_not_grow_into_a_hidden_queue() {
    for sink in [0.1, 0.15, 0.2, 0.5, 1.0, 5.0, 20.0] {
        let mut pipeline = Pipeline::new(1.0..2.0, 120.0, 3000.0);
        pipeline.sink = Some(sink);
        let run = run(&pipeline, policy());
        let end = run.limit_at(3000.0);
        let last = run.steps.last().unwrap();
        assert!(
            last.residence <= 10.0 || end == 4,
            "sink {sink}: limit {end}, {last:?}"
        );
        // one raise past it to find out that more doesn't help
        let peak = run.steps.iter().map(|s| s.target).max().unwrap();
        assert!(
            peak <= end * 2,
            "sink {sink}: grew to {peak}, ended at {end}"
        );
    }
}

/// Issue #4 as observed: every item waits ~30s upstream, so about 0.13 items/s leave at the
/// floor. Raising does help here, but only once the pace is known: until then the limit must
/// not grow past `drain_target`, and afterwards residence stays within the bound, so a shutdown
/// drains in about the bound rather than minutes.
#[test]
fn learns_the_bound_at_low_rates() {
    let run = run(&Pipeline::new(28.0..32.0, 120.0, 1200.0), policy());
    let learned = run
        .steps
        .iter()
        .find(|s| s.bound.is_finite())
        .expect("bound never learned");
    assert!(learned.t <= 300.0, "bound learned only at {}s", learned.t);
    // no raise before it: 4 items for 30s is already past the 10s target
    assert!(
        run.steps
            .iter()
            .filter(|s| s.t < learned.t)
            .all(|s| s.target == 4),
        "grew before the bound was known"
    );
    let fastest = run.policy.fastest().unwrap().as_secs_f64();
    assert!((25.0..=35.0).contains(&fastest), "fastest {fastest}");
    let last = run.steps.last().unwrap();
    assert!(last.residence <= last.bound, "{last:?}");
    assert_eq!(run.limit_at(1200.0), 1024);
}
