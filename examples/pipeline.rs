//! A simulated pipeline with three stages: pick items, download them, run them on a device.
//! Nothing real happens; each stage just sleeps.
//!
//! What to watch for in the output:
//!
//! - **Start:** downloads take 200ms and the device 5ms per item, in batches of 16.
//! - **At 20s** downloads slow to 800ms. The device starts waiting for input, so the limit
//!   doubles until the device is busy again, then settles back a bit.
//! - **At 40s** the device slows to 20ms per item. Now the work inside would take too long to
//!   finish, so the limit comes down.
//! - **At 60s** the gate closes and the program waits for the items inside. That should take
//!   less than the policy's `drain_target`.
//!
//! ```sh
//! cargo run --example pipeline
//! ```

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
    time::{Duration, Instant},
};

use starve_not::{DrainBounded, Gate, IdleProbe, Pacer, Ticket};
use tokio::sync::mpsc;

const BATCH: usize = 16;
const RUN: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() {
    let start = Instant::now();
    let latency_ms = Arc::new(AtomicU64::new(200));
    let per_item_ms = Arc::new(AtomicU64::new(5));

    let gate = Gate::new(1);
    let device = IdleProbe::new();
    let policy = DrainBounded::builder()
        .floor(2 * BATCH)
        .drain_target(Duration::from_secs(2))
        .build();
    let _pacer = Pacer::builder(&gate, policy)
        .probe(&device)
        .tick(Duration::from_secs(1))
        .on_decision(move |d, p| {
            println!(
                "{:>5.1}s  limit {:>4} -> {:<4} in flight {:>4}  {:>6.1} items/s  device idle {:>3.0}%  {}",
                start.elapsed().as_secs_f64(),
                d.limit,
                d.target,
                d.sample.in_flight,
                p.throughput(),
                d.sample.idle_shares().next().unwrap_or(0.0) * 100.0,
                d.diagnostics,
            )
        })
        .build()
        .spawn();

    let (to_device, mut device_rx) = mpsc::channel::<(u64, Ticket)>(1024);

    // claim → download: claim batches as the gate allows, download each item concurrently
    let claim = tokio::spawn({
        let (gate, latency_ms) = (gate.clone(), latency_ms.clone());
        async move {
            let mut next_id = 0u64;
            while let Ok(mut tickets) = gate.acquire_up_to(BATCH).await {
                for _ in 0..tickets.len() {
                    let (id, ticket, to_device) = (next_id, tickets.split(1), to_device.clone());
                    next_id += 1;
                    let latency = Duration::from_millis(latency_ms.load(Relaxed));
                    tokio::spawn(async move {
                        tokio::time::sleep(latency).await;
                        let _ = to_device.send((id, ticket)).await;
                    });
                }
            }
        }
    });

    // device: blocking, batches whatever has arrived, measures its own starvation
    let device_thread = std::thread::spawn({
        let per_item_ms = per_item_ms.clone();
        move || {
            loop {
                let first = {
                    let _idle = device.idle();
                    device_rx.blocking_recv()
                };
                let Some(first) = first else { break };
                let mut batch = vec![first];
                while batch.len() < BATCH {
                    match device_rx.try_recv() {
                        Ok(item) => batch.push(item),
                        Err(_) => break,
                    }
                }
                let per_item = Duration::from_millis(per_item_ms.load(Relaxed));
                std::thread::sleep(per_item * batch.len() as u32);
                for (id, ticket) in batch {
                    // every 20th item fails: returned, but not counted as throughput
                    match id % 20 {
                        0 => ticket.release(),
                        _ => ticket.complete(),
                    }
                }
            }
        }
    });

    println!("--- downloads now take 200ms --- device now takes 5ms per item");
    tokio::time::sleep_until((start + Duration::from_secs(20)).into()).await;
    latency_ms.store(800, Relaxed);
    println!("--- downloads now take 800ms");
    tokio::time::sleep_until((start + Duration::from_secs(40)).into()).await;
    per_item_ms.store(20, Relaxed);
    println!("--- device now takes 20ms per item");
    tokio::time::sleep_until((start + RUN).into()).await;

    println!("--- closing with {} in flight", gate.in_flight());
    let closing = Instant::now();
    gate.close();
    gate.drained().await;
    println!("--- drained in {:.2}s", closing.elapsed().as_secs_f64());
    claim.await.unwrap();
    device_thread.join().unwrap();
}
