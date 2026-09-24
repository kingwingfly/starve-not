//! What a policy sees each time it decides.

use std::time::{Duration, Instant};

use smallvec::SmallVec;

use crate::{Gate, IdleProbe};

/// What happened in the pipeline since the pacer last looked.
///
/// The [`Pacer`](crate::Pacer) builds one on every tick and hands it to the
/// [`Policy`](crate::Policy). Counts such as [`completed`](Self::completed) cover only the time
/// since the previous sample. [`in_flight`](Self::in_flight) and [`limit`](Self::limit) are
/// the values at the moment the sample was taken.
///
/// To test a policy with made-up or recorded data, start from [`Sample::new`] and fill in the
/// other fields.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct Sample {
    /// When the sample was taken.
    pub at: Instant,
    /// Time since the previous sample. Never zero in samples from a [`Pacer`](crate::Pacer).
    pub elapsed: Duration,
    /// Items finished since the previous sample, that is, permits handed back with
    /// [`complete`](crate::Ticket::complete).
    pub completed: u64,
    /// Items that left without finishing since the previous sample: permits handed back with
    /// [`release`](crate::Ticket::release) or by dropping a ticket.
    pub released: u64,
    /// How long each probe was idle since the previous sample, in the order the probes were
    /// added to the pacer.
    ///
    /// Probes always use the system clock. If the pacer runs on a different clock (for example
    /// tokio's paused test clock), these times may not match [`elapsed`](Self::elapsed).
    pub idle: SmallVec<[Duration; 2]>,
    /// Items inside the pipeline when the sample was taken.
    pub in_flight: usize,
    /// The gate's limit when the sample was taken.
    pub limit: usize,
}

impl Sample {
    /// A sample with the given time and gate state, and nothing finished, released or idle.
    pub fn new(at: Instant, elapsed: Duration, in_flight: usize, limit: usize) -> Self {
        Self {
            at,
            elapsed,
            completed: 0,
            released: 0,
            idle: SmallVec::new(),
            in_flight,
            limit,
        }
    }

    /// For each probe, the fraction of the interval it spent idle: 0 means always busy, 1
    /// means always waiting.
    pub fn idle_shares(&self) -> impl Iterator<Item = f64> + '_ {
        let elapsed = self.elapsed.as_secs_f64();
        self.idle.iter().map(move |idle| match elapsed > 0.0 {
            true => (idle.as_secs_f64() / elapsed).min(1.0),
            false => 0.0,
        })
    }
}

/// Turns the gate's and probes' running totals into per-interval [`Sample`]s.
///
/// The first reading only sets the baseline, so the driver's clock (whatever it is) defines
/// every interval, and activity before the first reading never lands in one.
#[derive(Debug, Default)]
pub(crate) struct Sampler {
    last: Option<Reading>,
}

/// The running totals at one instant.
#[derive(Debug)]
struct Reading {
    at: Instant,
    completed: u64,
    released: u64,
    idle: SmallVec<[Duration; 2]>,
}

impl Sampler {
    /// The sample for the interval since the previous reading, or `None` on the first reading
    /// and when no time has passed since the previous one (its deltas then carry over).
    pub(crate) fn sample(
        &mut self,
        gate: &Gate,
        probes: &[IdleProbe],
        now: Instant,
    ) -> Option<Sample> {
        let reading = Reading {
            at: now,
            completed: gate.completed(),
            released: gate.released(),
            // probes run on the system clock whatever clock `now` comes from; totals from the
            // same clock keep their deltas consistent
            idle: probes.iter().map(IdleProbe::total).collect(),
        };
        let Some(last) = &self.last else {
            self.last = Some(reading);
            return None;
        };
        let elapsed = now.saturating_duration_since(last.at);
        if elapsed.is_zero() {
            return None;
        }
        let sample = Sample {
            at: now,
            elapsed,
            completed: reading.completed - last.completed,
            released: reading.released - last.released,
            idle: reading
                .idle
                .iter()
                .zip(&last.idle)
                .map(|(now, last)| now.saturating_sub(*last))
                .collect(),
            in_flight: gate.in_flight(),
            limit: gate.limit(),
        };
        self.last = Some(reading);
        Some(sample)
    }
}
