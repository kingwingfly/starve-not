//! Policies: the rules that decide the gate's limit.
//!
//! Three come with the crate: [`DrainBounded`], [`Aimd`] and [`Fixed`]. To write your own,
//! implement [`Policy`].

mod aimd;
mod drain_bounded;
mod fixed;

use std::fmt;

use smallvec::SmallVec;

pub use aimd::{Aimd, AimdBuilder};
pub use drain_bounded::{DrainBounded, DrainBoundedBuilder};
pub use fixed::Fixed;

use crate::Sample;

/// Decides what the gate's limit should be.
///
/// On every tick the [`Pacer`](crate::Pacer) calls [`decide`](Self::decide) with a [`Sample`]
/// of what just happened, and sets the gate's limit to the answer.
///
/// A policy should work only from the samples it gets: no clocks (use [`Sample::at`] for the
/// time) and no I/O. That way, feeding it the same samples always gives the same answers, which
/// makes policies easy to test.
pub trait Policy: Send + 'static {
    /// The limit to start with, before any sample arrives.
    fn initial(&self) -> usize;

    /// The limit to use from now on, given the latest sample.
    ///
    /// The current limit is [`Sample::limit`]. Return it unchanged to keep it.
    fn decide(&mut self, sample: &Sample) -> usize;

    /// Values explaining the latest decision, for logs and metrics. Empty by default.
    fn diagnostics(&self) -> Diagnostics {
        Diagnostics::default()
    }
}

/// Named numbers a [`Policy`] reports about its latest decision, such as `throughput=12.5`.
///
/// Printing it gives `name=value` pairs separated by spaces.
#[derive(Clone, Default, PartialEq)]
pub struct Diagnostics(SmallVec<[(&'static str, f64); 6]>);

impl Diagnostics {
    /// Add a value.
    pub fn push(&mut self, name: &'static str, value: f64) {
        self.0.push((name, value));
    }

    /// The values, in the order they were added.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, f64)> + '_ {
        self.0.iter().copied()
    }
}

impl fmt::Display for Diagnostics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, (name, value)) in self.iter().enumerate() {
            if i > 0 {
                f.write_str(" ")?;
            }
            write!(f, "{name}={value:.3}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Diagnostics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
