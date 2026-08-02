//! Per-group materialized-state visibility and durability tracking.
//!
//! Raft applies committed entries serially, but an asynchronous materialized
//! state may return after its RocksDB batch is visible and before the shared WAL
//! is durable. This module tracks those two boundaries independently so log
//! purge, snapshots, reads after restart, and CF reclamation can fence on the
//! boundary they actually require.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use openraft::LogId;
use tokio::sync::Notify;

use crate::error::{Error, Result};
use crate::types::GroupId;

type RaftLogId = LogId<u64>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ApplyDurabilitySnapshot {
    pub(crate) visible: Option<RaftLogId>,
    pub(crate) durable: Option<RaftLogId>,
    pub(crate) pending_entries: usize,
    pub(crate) pending_bytes: usize,
    pub(crate) recovery_ready: bool,
    pub(crate) failed: Option<String>,
}

#[derive(Debug, Default)]
struct GroupState {
    visible: Option<RaftLogId>,
    durable: Option<RaftLogId>,
    startup_applied: Option<RaftLogId>,
    pending_entries: usize,
    pending_bytes: usize,
    recovery_ready: bool,
    closing: bool,
    failed: Option<String>,
}

#[derive(Debug)]
struct GroupTracker {
    state: Mutex<GroupState>,
    changed: Notify,
}

impl GroupTracker {
    fn new(recovered: Option<RaftLogId>, recovery_ready: bool) -> Self {
        Self {
            state: Mutex::new(GroupState {
                visible: recovered,
                durable: recovered,
                startup_applied: recovered,
                recovery_ready,
                ..GroupState::default()
            }),
            changed: Notify::new(),
        }
    }

    fn snapshot(&self) -> ApplyDurabilitySnapshot {
        let state = self.state.lock().unwrap();
        ApplyDurabilitySnapshot {
            visible: state.visible,
            durable: state.durable,
            pending_entries: state.pending_entries,
            pending_bytes: state.pending_bytes,
            recovery_ready: state.recovery_ready,
            failed: state.failed.clone(),
        }
    }

    fn begin(&self, entries: usize, bytes: usize) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if let Some(error) = &state.failed {
            return Err(Error::Io(std::io::Error::other(error.clone())));
        }
        if state.closing {
            return Err(Error::Raft(
                "materialized-state durability tracker is closing".into(),
            ));
        }
        state.pending_entries = state.pending_entries.saturating_add(entries);
        state.pending_bytes = state.pending_bytes.saturating_add(bytes);
        Ok(())
    }

    fn visible(&self, log_id: RaftLogId) {
        let mut state = self.state.lock().unwrap();
        if let Err(error) = advance(&mut state.visible, &log_id)
            && state.failed.is_none()
        {
            state.failed = Some(error.to_string());
        }
        if index_of(&state.visible) > index_of(&state.startup_applied) {
            // Any post-start apply was delivered as committed by the current
            // Raft runtime. It is sufficient to prevent serving the reverted
            // startup prefix. A leader ReadIndex may also open this explicitly.
            state.recovery_ready = true;
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn durable(&self, log_id: RaftLogId, entries: usize, bytes: usize) {
        let mut state = self.state.lock().unwrap();
        if let Err(error) = advance(&mut state.durable, &log_id)
            && state.failed.is_none()
        {
            state.failed = Some(error.to_string());
        }
        state.pending_entries = state.pending_entries.saturating_sub(entries);
        state.pending_bytes = state.pending_bytes.saturating_sub(bytes);
        drop(state);
        self.changed.notify_waiters();
    }

    fn cancel(&self, entries: usize, bytes: usize, error: String) {
        let mut state = self.state.lock().unwrap();
        state.pending_entries = state.pending_entries.saturating_sub(entries);
        state.pending_bytes = state.pending_bytes.saturating_sub(bytes);
        if state.failed.is_none() {
            state.failed = Some(error);
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn fail(&self, error: String) {
        let mut state = self.state.lock().unwrap();
        if state.failed.is_none() {
            state.failed = Some(error);
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn installed(&self, log_id: Option<RaftLogId>) {
        let mut state = self.state.lock().unwrap();
        if let Some(log_id) = log_id {
            let visible_result = advance(&mut state.visible, &log_id);
            let durable_result = advance(&mut state.durable, &log_id);
            if state.failed.is_none()
                && let Some(error) = visible_result.err().or_else(|| durable_result.err())
            {
                state.failed = Some(error.to_string());
            }
        }
        state.recovery_ready = true;
        drop(state);
        self.changed.notify_waiters();
    }

    fn mark_recovery_ready(&self) {
        self.state.lock().unwrap().recovery_ready = true;
        self.changed.notify_waiters();
    }

    fn validate_install(&self, target: Option<&RaftLogId>) -> Result<()> {
        let state = self.state.lock().unwrap();
        if let Some(error) = &state.failed {
            return Err(Error::Io(std::io::Error::other(error.clone())));
        }
        for (name, current) in [("visible", &state.visible), ("durable", &state.durable)] {
            match (current, target) {
                (Some(current), None) => {
                    return Err(Error::Corrupt(
                        "snapshot install".into(),
                        format!("empty snapshot would move {name} state behind {current}"),
                    ));
                }
                (Some(current), Some(target)) if target.index < current.index => {
                    return Err(Error::Corrupt(
                        "snapshot install".into(),
                        format!("snapshot {target} would move {name} state behind {current}"),
                    ));
                }
                (Some(current), Some(target))
                    if target.index == current.index && target != current =>
                {
                    return Err(Error::Corrupt(
                        "snapshot install".into(),
                        format!("snapshot log id {target} conflicts with {name} state {current}"),
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn begin_close(&self) {
        self.state.lock().unwrap().closing = true;
        self.changed.notify_waiters();
    }

    async fn wait_durable(&self, target: &RaftLogId) -> Result<()> {
        loop {
            let notified = self.changed.notified();
            {
                let state = self.state.lock().unwrap();
                if let Some(error) = &state.failed {
                    return Err(Error::Io(std::io::Error::other(error.clone())));
                }
                if reaches(&state.durable, target)? {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    async fn drain(&self) -> Result<()> {
        loop {
            let notified = self.changed.notified();
            {
                let state = self.state.lock().unwrap();
                if let Some(error) = &state.failed {
                    return Err(Error::Io(std::io::Error::other(error.clone())));
                }
                if state.pending_entries == 0 && state.pending_bytes == 0 {
                    return Ok(());
                }
            }
            notified.await;
        }
    }
}

/// Database-scoped registry. The shared WAL worker can fail every group at
/// once, while purge and snapshots wait on one group's durable prefix.
#[derive(Debug)]
pub(crate) struct ApplyDurabilityRegistry {
    groups: Mutex<HashMap<GroupId, Arc<GroupTracker>>>,
    failed: Mutex<Option<String>>,
    async_data_state: bool,
}

impl ApplyDurabilityRegistry {
    pub(crate) fn new(async_data_state: bool) -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
            failed: Mutex::new(None),
            async_data_state,
        }
    }

    pub(crate) fn async_for(&self, group: GroupId) -> bool {
        self.async_data_state && matches!(group, GroupId::Data(_))
    }

    pub(crate) fn register(&self, group: GroupId, recovered: Option<RaftLogId>) {
        let recovery_ready = !self.async_for(group);
        let failure = self.failed.lock().unwrap().clone();
        let tracker = self
            .groups
            .lock()
            .unwrap()
            .entry(group)
            .or_insert_with(|| Arc::new(GroupTracker::new(recovered, recovery_ready)))
            .clone();
        if let Some(error) = failure {
            tracker.fail(error);
        }
    }

    fn tracker(&self, group: GroupId) -> Arc<GroupTracker> {
        let recovery_ready = !self.async_for(group);
        let failure = self.failed.lock().unwrap().clone();
        let tracker = self
            .groups
            .lock()
            .unwrap()
            .entry(group)
            .or_insert_with(|| Arc::new(GroupTracker::new(None, recovery_ready)))
            .clone();
        if let Some(error) = failure {
            tracker.fail(error);
        }
        tracker
    }

    pub(crate) fn snapshot(&self, group: GroupId) -> ApplyDurabilitySnapshot {
        self.tracker(group).snapshot()
    }

    pub(crate) fn begin(&self, group: GroupId, entries: usize, bytes: usize) -> Result<()> {
        self.tracker(group).begin(entries, bytes)
    }

    pub(crate) fn visible(&self, group: GroupId, log_id: RaftLogId) {
        self.tracker(group).visible(log_id);
    }

    pub(crate) fn durable(&self, group: GroupId, log_id: RaftLogId, entries: usize, bytes: usize) {
        self.tracker(group).durable(log_id, entries, bytes);
    }

    pub(crate) fn cancel(&self, group: GroupId, entries: usize, bytes: usize, error: String) {
        self.tracker(group).cancel(entries, bytes, error.clone());
        self.fail_all(error);
    }

    pub(crate) fn fail_all(&self, error: String) {
        let mut failed = self.failed.lock().unwrap();
        if failed.is_none() {
            *failed = Some(error.clone());
        }
        drop(failed);
        let trackers: Vec<_> = self.groups.lock().unwrap().values().cloned().collect();
        for tracker in trackers {
            tracker.fail(error.clone());
        }
    }

    pub(crate) async fn wait_durable(&self, group: GroupId, target: &RaftLogId) -> Result<()> {
        self.tracker(group).wait_durable(target).await
    }

    pub(crate) async fn drain(&self, group: GroupId) -> Result<()> {
        self.tracker(group).drain().await
    }

    pub(crate) fn begin_close(&self, group: GroupId) {
        self.tracker(group).begin_close();
    }

    pub(crate) fn installed(&self, group: GroupId, log_id: Option<RaftLogId>) {
        self.tracker(group).installed(log_id);
    }

    pub(crate) fn validate_install(
        &self,
        group: GroupId,
        log_id: Option<&RaftLogId>,
    ) -> Result<()> {
        self.tracker(group).validate_install(log_id)
    }

    pub(crate) fn recovery_ready(&self, group: GroupId) -> bool {
        self.tracker(group).snapshot().recovery_ready
    }

    pub(crate) fn mark_recovery_ready(&self, group: GroupId) {
        self.tracker(group).mark_recovery_ready();
    }

    pub(crate) fn ensure_drained(&self, group: GroupId) -> Result<()> {
        let groups = self.groups.lock().unwrap();
        if let Some(tracker) = groups.get(&group) {
            let (pending_entries, pending_bytes) = {
                let state = tracker.state.lock().unwrap();
                (state.pending_entries, state.pending_bytes)
            };
            if pending_entries != 0 || pending_bytes != 0 {
                return Err(Error::Raft(format!(
                    "refusing to drop {group:?} with {} entries / {} bytes awaiting durability",
                    pending_entries, pending_bytes
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn remove(&self, group: GroupId) {
        self.groups.lock().unwrap().remove(&group);
    }
}

fn index_of(log_id: &Option<RaftLogId>) -> Option<u64> {
    log_id.as_ref().map(|log_id| log_id.index)
}

fn advance(current: &mut Option<RaftLogId>, next: &RaftLogId) -> Result<()> {
    match current {
        Some(previous) if previous.index == next.index && previous != next => Err(Error::Corrupt(
            "state durability watermark".into(),
            format!(
                "log id mismatch at index {}: {previous} != {next}",
                next.index
            ),
        )),
        Some(previous) if previous.index > next.index => Ok(()),
        Some(previous) if previous == next => Ok(()),
        _ => {
            *current = Some(*next);
            Ok(())
        }
    }
}

fn reaches(current: &Option<RaftLogId>, target: &RaftLogId) -> Result<bool> {
    let Some(current) = current else {
        return Ok(false);
    };
    if current.index < target.index {
        return Ok(false);
    }
    if current.index == target.index && current != target {
        return Err(Error::Corrupt(
            "state durability watermark".into(),
            format!(
                "log id mismatch at index {}: {current} != {target}",
                target.index
            ),
        ));
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::CommittedLeaderId;

    fn log_id(term: u64, index: u64) -> RaftLogId {
        RaftLogId::new(CommittedLeaderId::new(term, 1), index)
    }

    #[tokio::test]
    async fn visible_and_durable_are_independent_and_monotonic() {
        let registry = ApplyDurabilityRegistry::new(true);
        let group = GroupId::Data(1);
        registry.register(group, Some(log_id(1, 3)));

        registry.begin(group, 2, 100).unwrap();
        registry.visible(group, log_id(1, 5));
        let visible = registry.snapshot(group);
        assert_eq!(visible.visible, Some(log_id(1, 5)));
        assert_eq!(visible.durable, Some(log_id(1, 3)));
        assert!(visible.recovery_ready);

        registry.durable(group, log_id(1, 5), 2, 100);
        registry.wait_durable(group, &log_id(1, 5)).await.unwrap();
        let durable = registry.snapshot(group);
        assert_eq!(durable.durable, Some(log_id(1, 5)));
        assert_eq!(durable.pending_entries, 0);
        assert_eq!(durable.pending_bytes, 0);

        registry.installed(group, Some(log_id(2, 8)));
        registry.durable(group, log_id(1, 6), 0, 0);
        assert_eq!(registry.snapshot(group).durable, Some(log_id(2, 8)));
    }

    #[tokio::test]
    async fn mismatch_at_same_index_is_corruption() {
        let registry = ApplyDurabilityRegistry::new(true);
        let group = GroupId::Data(1);
        registry.register(group, Some(log_id(1, 3)));
        assert!(registry.wait_durable(group, &log_id(2, 3)).await.is_err());
    }

    #[test]
    fn global_failure_poison_applies_to_groups_registered_later() {
        let registry = ApplyDurabilityRegistry::new(true);
        registry.fail_all("flush failed".into());
        let group = GroupId::Data(2);
        registry.register(group, Some(log_id(1, 1)));

        let state = registry.snapshot(group);
        assert_eq!(state.failed.as_deref(), Some("flush failed"));
        assert!(registry.begin(group, 1, 10).is_err());
    }

    #[tokio::test]
    async fn closing_rejects_new_applies_and_allows_existing_ones_to_drain() {
        let registry = ApplyDurabilityRegistry::new(true);
        let group = GroupId::Data(3);
        registry.register(group, None);
        registry.begin(group, 1, 10).unwrap();
        registry.begin_close(group);

        assert!(registry.begin(group, 1, 10).is_err());
        assert!(registry.ensure_drained(group).is_err());
        registry.visible(group, log_id(1, 1));
        registry.durable(group, log_id(1, 1), 1, 10);
        registry.drain(group).await.unwrap();
        registry.ensure_drained(group).unwrap();
    }

    #[test]
    fn snapshot_install_may_advance_but_never_replace_with_an_older_prefix() {
        let registry = ApplyDurabilityRegistry::new(true);
        let group = GroupId::Data(4);
        registry.register(group, Some(log_id(2, 8)));

        assert!(
            registry
                .validate_install(group, Some(&log_id(2, 8)))
                .is_ok()
        );
        assert!(
            registry
                .validate_install(group, Some(&log_id(3, 9)))
                .is_ok()
        );
        assert!(
            registry
                .validate_install(group, Some(&log_id(1, 7)))
                .is_err()
        );
        assert!(
            registry
                .validate_install(group, Some(&log_id(3, 8)))
                .is_err()
        );
        assert!(registry.validate_install(group, None).is_err());
    }
}
