//! Under `tokio::time::pause`, probes and a spawned pacer share tokio's clock, so paused tests
//! see consistent idle times. Needs the `test-util` feature: `cargo test --features test-util`.

#![cfg(feature = "test-util")]

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use starve_not::{Aimd, Gate, IdleProbe, Pacer};
use tokio::time::{advance, sleep};

#[tokio::test(start_paused = true)]
async fn probe_follows_paused_time() {
    let probe = IdleProbe::new();
    {
        let _idle = probe.idle();
        advance(Duration::from_secs(10)).await;
    }
    assert_eq!(probe.total(), Duration::from_secs(10));
}

/// The bottleneck waits half of every tick. Paused time runs far faster than real time, so this
/// only reads as half idle if the probe and the pacer use the same clock.
#[tokio::test(start_paused = true)]
async fn spawned_pacer_sees_probe_on_the_same_clock() {
    let gate = Gate::new(1);
    let probe = IdleProbe::new();
    let shares = Arc::new(Mutex::new(Vec::new()));
    let record = shares.clone();
    let _pacer = Pacer::builder(&gate, Aimd::default())
        .probe(&probe)
        .tick(Duration::from_secs(2))
        .on_decision(move |decision, _| {
            record.lock().unwrap().extend(decision.sample.idle_shares())
        })
        .build()
        .spawn();

    // idle for the first second of every tick, busy for the second
    for _ in 0..10 {
        {
            let _idle = probe.idle();
            sleep(Duration::from_secs(1)).await;
        }
        sleep(Duration::from_secs(1)).await;
    }

    let shares = shares.lock().unwrap();
    assert!(shares.len() >= 5, "{shares:?}");
    assert!(shares.iter().all(|s| (s - 0.5).abs() < 1e-9), "{shares:?}");
}

/// The bottleneck usually runs on its own thread, outside the runtime. A probe made in the
/// runtime must still read the runtime's paused clock there.
#[tokio::test(start_paused = true)]
async fn probe_follows_paused_time_from_other_threads() {
    let probe = IdleProbe::new();
    let on_thread = |f: fn(&IdleProbe)| {
        let probe = probe.clone();
        std::thread::spawn(move || f(&probe)).join().unwrap();
    };
    on_thread(IdleProbe::start);
    advance(Duration::from_secs(10)).await;
    on_thread(IdleProbe::end);
    assert_eq!(probe.total(), Duration::from_secs(10));
}

/// A probe used in another runtime must still read the clock of the one it was made in, or
/// the two paused clocks would mix.
#[tokio::test(start_paused = true)]
async fn probe_follows_its_own_runtime_from_another() {
    let probe = IdleProbe::new();
    probe.start();
    advance(Duration::from_secs(10)).await;
    let other = probe.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        runtime.block_on(async { other.end() });
    })
    .join()
    .unwrap();
    assert_eq!(probe.total(), Duration::from_secs(10));
}
