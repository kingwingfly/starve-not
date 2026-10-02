//! Policies: the rules that decide the gate's limit.
//!
//! Three come with the crate. To write your own, implement [`Policy`].
//!
//! | Policy | Needs a probe | Raises the limit when | Lowers it when |
//! |---|---|---|---|
//! | [`DrainBounded`] | yes | the bottleneck waits for input | items would stay inside too long |
//! | [`Aimd`] | no | ticks go well | items fail or get slow |
//! | [`Fixed`] | no | never | never |

mod aimd;
#[cfg(feature = "diagnostics")]
mod diagnostics;
mod drain_bounded;
mod fixed;

pub use aimd::{Aimd, AimdBuilder};
#[cfg(feature = "diagnostics")]
pub use diagnostics::Diagnostics;
pub use drain_bounded::{DrainBounded, DrainBoundedBuilder};
pub use fixed::Fixed;

use crate::Sample;

/// Decides what the gate's limit should be.
///
/// On every tick the [`Pacer`](crate::Pacer) calls [`decide`](Self::decide) with a [`Sample`]
/// of what just happened, and sets the gate's limit to the answer:
///
/// ```text
/// gate + probes -> Sample -> Policy::decide -> new limit -> gate
///       ^                                                    |
///       +---------------- items go in and out ---------------+
/// ```
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
    ///
    /// Needs the `diagnostics` feature.
    #[cfg(feature = "diagnostics")]
    fn diagnostics(&self) -> Diagnostics {
        Diagnostics::default()
    }
}
