//! Process-local lifecycle and serving readiness.
//!
//! Bootstrap, admission, shutdown, and probes share this state. It is separate
//! from durable membership: changing process lifecycle never removes a voter or
//! revives a node that the replicated directory declared down.

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use serde::{Deserialize, Serialize};

/// Observable process lifecycle. Transitions are monotonic; clean teardown
/// moves `Draining` to `Stopped`, while failures remain `Failed` for diagnosis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[repr(u8)]
pub enum RuntimeLifecycle {
    Starting = 0,
    Running = 1,
    Draining = 2,
    Failed = 3,
    Stopped = 4,
}

impl RuntimeLifecycle {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Starting,
            1 => Self::Running,
            2 => Self::Draining,
            3 => Self::Failed,
            4 => Self::Stopped,
            _ => Self::Failed,
        }
    }

    pub fn is_live(self) -> bool {
        matches!(self, Self::Starting | Self::Running | Self::Draining)
    }

    pub fn is_ready(self) -> bool {
        self == Self::Running
    }
}

/// Shared lifecycle gate. The historical name is retained because background
/// drivers use `is_ready()` to wait for bootstrap completion.
#[derive(Debug, Clone)]
pub struct RuntimeReadiness(Arc<AtomicU8>);

impl Default for RuntimeReadiness {
    fn default() -> Self {
        Self(Arc::new(AtomicU8::new(RuntimeLifecycle::Starting as u8)))
    }
}

impl RuntimeReadiness {
    pub fn lifecycle(&self) -> RuntimeLifecycle {
        RuntimeLifecycle::from_u8(self.0.load(Ordering::Acquire))
    }

    pub fn is_ready(&self) -> bool {
        self.lifecycle().is_ready()
    }

    pub fn is_live(&self) -> bool {
        self.lifecycle().is_live()
    }

    /// Open client/background readiness after bootstrap. A concurrent drain or
    /// failure wins; bootstrap completion must never reopen a closing process.
    pub fn mark_ready(&self) -> bool {
        self.transition(RuntimeLifecycle::Starting, RuntimeLifecycle::Running)
            || self.lifecycle() == RuntimeLifecycle::Running
    }

    /// Close readiness before graceful shutdown begins.
    pub fn begin_draining(&self) -> bool {
        loop {
            let current = self.lifecycle();
            match current {
                RuntimeLifecycle::Starting | RuntimeLifecycle::Running => {
                    if self.transition(current, RuntimeLifecycle::Draining) {
                        return true;
                    }
                }
                RuntimeLifecycle::Draining => return true,
                RuntimeLifecycle::Failed | RuntimeLifecycle::Stopped => return false,
            }
        }
    }

    /// Record a fatal process-local failure. Stopped is terminal.
    pub fn mark_failed(&self) -> bool {
        loop {
            let current = self.lifecycle();
            if current == RuntimeLifecycle::Stopped {
                return false;
            }
            if current == RuntimeLifecycle::Failed {
                return true;
            }
            if self.transition(current, RuntimeLifecycle::Failed) {
                return true;
            }
        }
    }

    pub fn mark_stopped(&self) {
        self.0
            .store(RuntimeLifecycle::Stopped as u8, Ordering::Release);
    }

    fn transition(&self, from: RuntimeLifecycle, to: RuntimeLifecycle) -> bool {
        self.0
            .compare_exchange(from as u8, to as u8, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_closes_readiness_before_stop() {
        let gate = RuntimeReadiness::default();
        assert_eq!(gate.lifecycle(), RuntimeLifecycle::Starting);
        assert!(gate.is_live());
        assert!(!gate.is_ready());

        assert!(gate.mark_ready());
        assert!(gate.is_ready());
        assert!(gate.clone().begin_draining());
        assert_eq!(gate.lifecycle(), RuntimeLifecycle::Draining);
        assert!(!gate.is_ready());
        assert!(!gate.mark_ready());

        gate.mark_stopped();
        assert_eq!(gate.lifecycle(), RuntimeLifecycle::Stopped);
        assert!(!gate.is_live());
    }

    #[test]
    fn failure_cannot_be_reopened() {
        let gate = RuntimeReadiness::default();
        assert!(gate.mark_failed());
        assert_eq!(gate.lifecycle(), RuntimeLifecycle::Failed);
        assert!(!gate.mark_ready());
        assert!(!gate.begin_draining());
        gate.mark_stopped();
        assert!(!gate.mark_failed());
    }
}
