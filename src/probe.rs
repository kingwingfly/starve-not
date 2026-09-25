//! Measuring how long the bottleneck waits for input.

use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;

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
/// threads. They measure time on the system clock.
#[derive(Clone, Default)]
pub struct IdleProbe {
    /// one lock so a reading never counts a wait twice or goes backwards
    state: Arc<Mutex<State>>,
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

impl IdleProbe {
    /// Create a probe with no idle time recorded yet.
    pub fn new() -> Self {
        Self::default()
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
        state.since.get_or_insert_with(Instant::now);
    }

    /// Mark that a worker stopped waiting for input. Does nothing if no worker was waiting.
    pub fn end(&self) {
        let state = &mut *self.state.lock();
        state.waiting = state.waiting.saturating_sub(1);
        if state.waiting == 0 {
            state.total += state
                .since
                .take()
                .map_or(Duration::ZERO, |since| since.elapsed());
        }
    }

    /// Total time the stage has been idle so far, including a wait still going on.
    pub fn total(&self) -> Duration {
        let now = Instant::now();
        let state = self.state.lock();
        let current = state
            .since
            .map_or(Duration::ZERO, |since| now.saturating_duration_since(since));
        state.total + current
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
