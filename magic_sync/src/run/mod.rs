//! The engine, run for the host: the loops every host would otherwise write
//! the same way, and get wrong the same ways. A host builds its side's runner
//! once — the `Server` on the server, a `Replica` on a replica — and runs it
//! in a task of its own.

use crate::Error;
use std::time::Duration;

#[cfg(feature = "sqlite")]
pub mod replica;
#[cfg(feature = "postgres")]
pub mod server;

/// How long something that failed waits before it is tried again: `first`,
/// doubling with every failure in a row, up to `max`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Backoff {
    /// The wait after the first failure.
    pub first: Duration,
    /// The longest wait.
    pub max: Duration,
}

impl Backoff {
    /// Waits from `first`, doubling, up to `max`.
    #[must_use]
    pub const fn new(first: Duration, max: Duration) -> Self {
        Backoff { first, max }
    }

    /// Refuses a backoff that would not wait, or would shrink: `what` names
    /// the setting.
    pub(crate) fn check(&self, what: &str) -> Result<(), Error> {
        if self.first.is_zero() || self.first > self.max {
            return Err(Error::Misuse(format!(
                "`{what}` waits {:?} first and {:?} at most: the first wait is not zero, \
                 and not longer than the longest",
                self.first, self.max
            )));
        }
        Ok(())
    }

    /// The wait after `wait`, one failure later.
    pub(crate) fn after(&self, wait: Duration) -> Duration {
        wait.saturating_mul(2).min(self.max)
    }
}
