//! Process-local bootstrap readiness for background runtime work.
//!
//! A node binds its transport and starts Raft before [`crate::runtime::node::Node::bootstrap`]
//! completes. Background reconciliation may safely refresh addresses during that
//! interval, but it must not create new placement plans or reclaim local state
//! until bootstrap has established this process's initial groups.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Shared one-way bootstrap gate. A successful bootstrap opens it for the
/// lifetime of the process; a restart reconstructs readiness from durable Raft
/// state before opening it again.
#[derive(Debug, Clone, Default)]
pub struct RuntimeReadiness(Arc<AtomicBool>);

impl RuntimeReadiness {
    pub fn is_ready(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    pub fn mark_ready(&self) {
        self.0.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn readiness_is_closed_until_bootstrap_completes() {
        let gate = RuntimeReadiness::default();
        assert!(!gate.is_ready());
        gate.mark_ready();
        assert!(gate.is_ready());
        assert!(gate.clone().is_ready());
    }
}
