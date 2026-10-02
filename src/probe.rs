//! Measuring how long the bottleneck waits for input.

use std::{fmt, sync::Arc, time::Duration};

use parking_lot::Mutex;
// for tests, the clock of the runtime the probe was created in, so it follows
// `tokio::time::pause` like a spawned pacer
#[cfg(not(feature = "test-util"))]
use std::time::Instant;
#[cfg(feature = "test-util")]
use tokio::time::Instant;

/// Measures how long a stage spends waiting for input.
///
/// Put one on the bottleneck, the slowest stage (for example the loop feeding a GPU), and wrap
/// every wait for its next input in [`idle`](Self::idle):
///
/// ```
/// # use starve_not::IdleProbe;
/// # let (probe, rx) = (IdleProbe::new(), std::sync::mpsc::channel::<u32>().1);
/// let next = {
///     let _idle = probe.idle();
///     rx.recv()
/// };
/// ```
///
/// The [`Pacer`](crate::Pacer) reads the probe regularly. If the bottleneck waits for input
/// while the gate is full, the pipeline doesn't let enough work in, and the limit should go up.
///
/// The probe only sees the stage it is on. If a slower stage comes after it, with a buffer in
/// between that holds more items than the gate lets in, this stage waits while that queue
/// grows, and the probe reports hunger that more items won't fix. Put the probe on that slower
/// stage instead, or keep the buffers after the probed stage small, so a slow stage behind it
/// holds it up rather than leaving it idle.
///
/// When several workers run the same stage, give them clones of one probe. The stage counts as
/// idle whenever at least one of them is waiting, since one worker with nothing to do is already
/// wasted capacity.
///
/// Probes are cheap: each call takes a short lock, so they are fine in hot loops and on blocking
/// threads.
///
/// They measure time on the system clock. For tests with paused tokio time, turn on the
/// `test-util` feature in your dev-dependencies: probes then read the clock of the tokio runtime
/// they were created in, from any thread, so they follow `tokio::time::pause` like a spawned
/// pacer. A probe created outside any runtime then uses the clock of the runtime it is used in,
/// or the system clock outside one. The feature costs a little on every call, so leave it out of
/// normal builds.
#[derive(Clone)]
pub struct IdleProbe {
    /// one lock so a reading never counts a wait twice or goes backwards
    state: Arc<Mutex<State>>,
    /// the runtime the probe was created in, whose clock it reads
    #[cfg(feature = "test-util")]
    runtime: Option<tokio::runtime::Handle>,
}

#[derive(Debug, Default)]
struct State {
    /// finished idle time
    total: Duration,
    /// when the current idle period began, i.e. when `waiting` last rose from 0
    since: Option<Instant>,
    /// workers waiting for input
    waiting: usize,
}

impl Default for IdleProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl IdleProbe {
    /// Create a probe with no idle time recorded yet.
    pub fn new() -> Self {
        Self {
            state: Arc::default(),
            #[cfg(feature = "test-util")]
            runtime: tokio::runtime::Handle::try_current().ok(),
        }
    }

    /// Count the stage as idle until the returned guard is dropped.
    pub fn idle(&self) -> IdleGuard<'_> {
        self.start();
        IdleGuard { probe: self }
    }

    /// Mark that a worker started waiting for input.
    ///
    /// Every call needs a matching [`end`](Self::end). [`idle`](Self::idle) pairs them for you,
    /// so prefer it unless the wait starts and ends in different places.
    pub fn start(&self) {
        let state = &mut *self.state.lock();
        state.waiting += 1;
        state.since.get_or_insert_with(|| self.now());
    }

    /// Mark that a worker stopped waiting for input. Does nothing if no worker was waiting.
    pub fn end(&self) {
        let state = &mut *self.state.lock();
        state.waiting = state.waiting.saturating_sub(1);
        if let (0, Some(since)) = (state.waiting, state.since) {
            state.since = None;
            state.total += self.now().saturating_duration_since(since);
        }
    }

    /// Total time the stage has been idle so far, including a wait still going on.
    pub fn total(&self) -> Duration {
        let now = self.now();
        let state = self.state.lock();
        let current = state
            .since
            .map_or(Duration::ZERO, |since| now.saturating_duration_since(since));
        state.total + current
    }

    /// The time on the probe's clock.
    fn now(&self) -> Instant {
        #[cfg(feature = "test-util")]
        {
            // a thread that is shutting down can't ask tokio, which would panic: use the system
            // clock there
            let current = tokio::runtime::Handle::try_current();
            if current.is_err_and(|e| e.is_thread_local_destroyed()) {
                return Instant::from_std(std::time::Instant::now());
            }
            // read the clock of the runtime the probe was created in, from any thread, even one
            // in another runtime
            if let Some(runtime) = &self.runtime {
                let _entered = runtime.enter();
                return Instant::now();
            }
        }
        Instant::now()
    }
}

impl fmt::Debug for IdleProbe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock();
        f.debug_struct("IdleProbe")
            .field("total", &state.total)
            .field("waiting", &state.waiting)
            .finish()
    }
}

/// Keeps its [`IdleProbe`] counting as idle until dropped. Made by [`IdleProbe::idle`].
#[must_use = "the stage counts as idle only until the guard drops"]
#[derive(Debug)]
pub struct IdleGuard<'a> {
    probe: &'a IdleProbe,
}

impl Drop for IdleGuard<'_> {
    fn drop(&mut self) {
        self.probe.end();
    }
}
