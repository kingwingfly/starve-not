//! Keep the slowest stage of a pipeline busy without letting work pile up.
//!
//! Picture a pipeline where items are downloaded, then processed on a GPU. The GPU is the
//! *bottleneck*, and the whole pipeline goes only as fast as it does. You want two things:
//!
//! 1. **The GPU never starves.** Enough items must be on their way to cover the time downloads
//!    take, even when downloads suddenly get slower.
//! 2. **Not too much is on its way.** Every item admitted into the pipeline uses memory, and on
//!    shutdown it costs time for all of them to finish. That should stay short.
//!
//! No single fixed number gets both right, because the right number depends on download speed,
//! GPU speed and batch size, and those change while the program runs. `starve-not` adjusts the
//! number for you.
//!
//! # How it fits together
//!
//! - A [`Gate`] sits at the start of the pipeline and lets items in. Each admitted item holds a
//!   *permit*, and the gate has a *limit* on how many permits are out at once. The permits come
//!   as a [`Ticket`], which travels with the item until it is done. Items of very different
//!   cost can take more than one permit each (see [Weighted items](Gate#weighted-items)).
//! - An [`IdleProbe`] on the bottleneck measures how long it sits waiting for input.
//! - A [`Pacer`] looks at the gate and the probes every couple of seconds, asks a [`Policy`]
//!   what the limit should be, and changes the gate's limit.
//!
//! Three policies come with the crate:
//!
//! - [`DrainBounded`] (the one to start with) raises the limit while the bottleneck starves and
//!   the gate is nearly full, provided items are succeeding. It checks whether raises improve
//!   throughput and uses estimated average residence to limit queued work, allowing longer for
//!   naturally slow pipelines.
//! - [`Aimd`] raises the limit slowly and cuts it when items fail or get slow. It needs no probe,
//!   so it suits pipelines without one clear bottleneck.
//! - [`Fixed`] never changes the limit. It is useful as a baseline when measuring the others.
//!
//! # Example
//!
//! ```no_run
//! use starve_not::{DrainBounded, Gate, IdleProbe, Pacer};
//! # async fn download(_: u64) -> Vec<u8> { vec![] }
//! # fn infer(_: &[u8]) -> bool { true }
//!
//! # #[cfg(feature = "rt")]
//! # async fn run() {
//! let gate = Gate::new(1);
//! let device = IdleProbe::new();
//! let policy = DrainBounded::builder().floor(16).build();
//! let _pacer = Pacer::builder(&gate, policy).probe(&device).build().spawn();
//!
//! let (tx, mut rx) = tokio::sync::mpsc::channel(64);
//! tokio::spawn({
//!     let gate = gate.clone();
//!     async move {
//!         for id in 0.. {
//!             // wait until the gate lets one more item in
//!             let Ok(ticket) = gate.acquire().await else { break };
//!             let bytes = download(id).await;
//!             if tx.send((bytes, ticket)).await.is_err() {
//!                 break;
//!             }
//!         }
//!     }
//! });
//! std::thread::spawn(move || {
//!     loop {
//!         let next = {
//!             let _idle = device.idle(); // counts as idle until the next item arrives
//!             rx.blocking_recv()
//!         };
//!         let Some((bytes, ticket)) = next else { break };
//!         match infer(&bytes) {
//!             true => ticket.complete(), // done: the permit goes back to the gate
//!             false => ticket.release(), // failed: also goes back, but isn't counted as done
//!         }
//!     }
//! });
//!
//! // on shutdown: let nothing new in, then wait for the items already inside
//! gate.close();
//! gate.drained().await;
//! # }
//! ```
//!
//! # Several gates
//!
//! One gate limits the whole pipeline, but a single stage can have its own limit too. Say
//! downloads should be limited on their own, because the storage server slows down when
//! asked for too many bytes at once. Create one gate for downloads and one for the whole
//! pipeline, each with its own pacer and policy. An item holds a ticket from each: the download
//! ticket goes back as soon as the download is done, the other one when the item is finished.
//!
//! Always take the tickets in the same order, outer gate first. Each gate can count in its own
//! unit. Here images are resized to the same size once downloaded, so the pipeline counts items,
//! but downloads differ in size, so the download gate weighs them in megabytes (see
//! [Weighted items](Gate#weighted-items)).
//!
//! ```no_run
//! use starve_not::{Aimd, DrainBounded, Gate, IdleProbe, Pacer};
//! # struct Item { id: u64, megabytes: usize }
//! # async fn download(_: u64) -> Result<Vec<u8>, ()> { Ok(vec![]) }
//!
//! # #[cfg(feature = "rt")]
//! # async fn run(items: Vec<Item>, device: IdleProbe) {
//! // the whole pipeline, in items: grows while the device waits for input
//! let pipeline = Gate::new(1);
//! let policy = DrainBounded::builder().floor(8).build();
//! let _pacer = Pacer::builder(&pipeline, policy).probe(&device).build().spawn();
//!
//! // downloads at once, in megabytes: backs off when downloads fail or get slow
//! let downloads = Gate::new(1);
//! let policy = Aimd::builder().floor(64).increase(16).build();
//! let _download_pacer = Pacer::builder(&downloads, policy).build().spawn();
//!
//! for item in items {
//!     let Ok(ticket) = pipeline.acquire().await else { break };
//!     let Ok(download_ticket) = downloads.acquire_weighted(item.megabytes).await else { break };
//!     match download(item.id).await {
//!         Ok(bytes) => {
//!             download_ticket.complete();
//!             // ...send `bytes` and `ticket` on to the device, which completes `ticket`
//!         }
//!         // both go back without counting as done
//!         Err(_) => drop((download_ticket, ticket)),
//!     }
//! }
//! # }
//! ```
//!
//! # Features
//!
//! - `rt` (on by default): adds `Pacer::spawn`, which runs the pacer as a tokio task. Without
//!   it, call [`Pacer::step`] yourself on a timer.
//! - `diagnostics`: adds `Policy::diagnostics`, the values a policy decided from, for logs and
//!   metrics. Without it, policies keep nothing extra for them.
//! - `test-util`: for tests with paused tokio time. Probes then follow `tokio::time::pause`, like a
//!   spawned pacer. Turn it on in `[dev-dependencies]` only, since it costs a little on every
//!   probe call.

#![deny(missing_docs, rustdoc::broken_intra_doc_links)]
#![warn(missing_debug_implementations)]

mod gate;
mod pacer;
pub mod policy;
mod probe;
mod sample;

pub use gate::{Closed, Gate, Ticket};
#[cfg(feature = "rt")]
pub use pacer::PacerHandle;
pub use pacer::{Decision, Pacer, PacerBuilder};
#[cfg(feature = "diagnostics")]
pub use policy::Diagnostics;
pub use policy::{Aimd, DrainBounded, Fixed, Policy};
pub use probe::{IdleGuard, IdleProbe};
pub use sample::Sample;
