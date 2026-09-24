//! A policy that never changes the limit.

use super::Policy;
use crate::Sample;

/// Keeps the limit at one number.
///
/// Useful as a baseline when you want to know whether an adaptive policy actually helps. It also
/// lets you watch a pipeline's samples through
/// [`on_decision`](crate::PacerBuilder::on_decision) without the limit changing.
///
/// ```
/// # use starve_not::{Fixed, Gate, Pacer};
/// let gate = Gate::new(1);
/// let pacer = Pacer::builder(&gate, Fixed::new(64)).build();
/// assert_eq!(gate.limit(), 64);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fixed {
    limit: usize,
}

impl Fixed {
    /// Create a policy that keeps the limit at `limit`.
    ///
    /// # Panics
    ///
    /// If `limit` is 0, since the gate would then let nothing in.
    pub fn new(limit: usize) -> Self {
        assert!(limit > 0, "limit must be at least 1");
        Self { limit }
    }

    /// The limit it keeps.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

impl Policy for Fixed {
    fn initial(&self) -> usize {
        self.limit
    }

    fn decide(&mut self, _: &Sample) -> usize {
        self.limit
    }
}
