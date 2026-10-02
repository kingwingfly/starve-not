//! What a policy reports about its decisions, with the `diagnostics` feature.

use std::fmt;

use smallvec::SmallVec;

/// Named numbers a [`Policy`](crate::Policy) reports about its latest decision, such as
/// `completion_rate=12.5`. Needs the `diagnostics` feature.
///
/// Printing it gives `name=value` pairs separated by spaces.
#[derive(Clone, Default, PartialEq)]
// room for all of `DrainBounded`'s values without allocating
pub struct Diagnostics(SmallVec<[(&'static str, f64); 10]>);

impl Diagnostics {
    /// Add a value.
    pub fn push(&mut self, name: &'static str, value: f64) {
        self.0.push((name, value));
    }

    /// Add a flag, as 1 if `value` is true, else 0.
    pub fn push_flag(&mut self, name: &'static str, value: bool) {
        self.push(name, f64::from(u8::from(value)));
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
