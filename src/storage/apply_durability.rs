//! Per-group materialized-state visibility and durability tracking.
//!
//! Raft applies committed entries serially, but an asynchronous materialized
//! state may return after its RocksDB batch is visible and before the shared WAL
//! is durable. This module tracks those two boundaries independently so log
//! purge, snapshots, reads after restart, and CF reclamation can fence on the
//! boundary they actually require.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use openraft::LogId;
use tokio::sync::Notify;

use crate::error::{Error, Result};
use crate::types::GroupId;

type RaftLogId = LogId<u64>;
type FailureHook = Arc<dyn Fn(String) + Send + Sync>;

static NEXT_RECOVERY_EPOCH: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy)]
pub(crate) struct ApplyDurabilityLimits {
    pub(crate) max_pending_entries: usize,
    pub(crate) max_pending_bytes: usize,
    pub(crate) max_dirty_age: Duration,
}

impl Default for ApplyDurabilityLimits {
    fn default() -> Self {
        Self {
            max_pending_entries: super::MAX_PENDING_STATE_ENTRIES,
            max_pending_bytes: super::MAX_PENDING_STATE_BYTES,
            max_dirty_age: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum DurabilityWaitKind {
    #[cfg(test)]
    General,
    Purge,
    Snapshot,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ApplyDurabilitySnapshot {
    pub(crate) visible: Option<RaftLogId>,
    pub(crate) durable: Option<RaftLogId>,
    pub(crate) pending_entries: usize,
    pub(crate) pending_bytes: usize,
    pub(crate) oldest_pending_ms: Option<u64>,
    pub(crate) last_flush_latency_ms: Option<u64>,
    pub(crate) max_flush_latency_ms: u64,
    pub(crate) flush_failures: u64,
    pub(crate) recovery_ready: bool,
    pub(crate) recovery_epoch: u64,
    pub(crate) recovery_target: Option<RaftLogId>,
    pub(crate) recovery_attempts: u64,
    pub(crate) recovery_duration_ms: Option<u64>,
    pub(crate) replayed_entries: u64,
    pub(crate) purge_waits: u64,
    pub(crate) purge_wait_ms: u64,
    pub(crate) snapshot_waits: u64,
    pub(crate) snapshot_wait_ms: u64,
    pub(crate) failed: Option<String>,
}

#[derive(Debug)]
struct PendingApply {
    log_id: RaftLogId,
    entries: usize,
    bytes: usize,
    started: Instant,
}

#[derive(Debug, Default)]
struct GroupState {
    visible: Option<RaftLogId>,
    durable: Option<RaftLogId>,
    startup_applied: Option<RaftLogId>,
    pending_entries: usize,
    pending_bytes: usize,
    pending: VecDeque<PendingApply>,
    last_flush_latency_ms: Option<u64>,
    max_flush_latency_ms: u64,
    flush_failures: u64,
    recovery_ready: bool,
    recovery_epoch: u64,
    recovery_target: Option<RaftLogId>,
    recovery_attempts: u64,
    recovery_started: Option<Instant>,
    recovery_duration_ms: Option<u64>,
    replayed_entries: u64,
    purge_waits: u64,
    purge_wait_ms: u64,
    snapshot_waits: u64,
    snapshot_wait_ms: u64,
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
                recovery_epoch: NEXT_RECOVERY_EPOCH.fetch_add(1, Ordering::Relaxed),
                recovery_started: (!recovery_ready).then(Instant::now),
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
            oldest_pending_ms: state
                .pending
                .front()
                .map(|pending| millis(pending.started.elapsed())),
            last_flush_latency_ms: state.last_flush_latency_ms,
            max_flush_latency_ms: state.max_flush_latency_ms,
            flush_failures: state.flush_failures,
            recovery_ready: state.recovery_ready,
            recovery_epoch: state.recovery_epoch,
            recovery_target: state.recovery_target,
            recovery_attempts: state.recovery_attempts,
            recovery_duration_ms: state.recovery_duration_ms,
            replayed_entries: state.replayed_entries,
            purge_waits: state.purge_waits,
            purge_wait_ms: state.purge_wait_ms,
            snapshot_waits: state.snapshot_waits,
            snapshot_wait_ms: state.snapshot_wait_ms,
            failed: state.failed.clone(),
        }
    }

    fn begin(
        &self,
        log_id: RaftLogId,
        entries: usize,
        bytes: usize,
        limits: ApplyDurabilityLimits,
    ) -> Result<bool> {
        let mut state = self.state.lock().unwrap();
        if let Some(error) = &state.failed {
            return Err(Error::Io(std::io::Error::other(error.clone())));
        }
        if state.closing {
            return Err(Error::Raft(
                "materialized-state durability tracker is closing".into(),
            ));
        }
        if entries > limits.max_pending_entries || bytes > limits.max_pending_bytes {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "materialized-state batch of {entries} entries / {bytes} bytes exceeds limits of {} entries / {} bytes",
                    limits.max_pending_entries, limits.max_pending_bytes
                ),
            )));
        }
        let entries_fit =
            state.pending_entries.saturating_add(entries) <= limits.max_pending_entries;
        let bytes_fit = state.pending_bytes.saturating_add(bytes) <= limits.max_pending_bytes;
        if !entries_fit || !bytes_fit {
            return Ok(false);
        }
        state.pending_entries = state.pending_entries.saturating_add(entries);
        state.pending_bytes = state.pending_bytes.saturating_add(bytes);
        state.pending.push_back(PendingApply {
            log_id,
            entries,
            bytes,
            started: Instant::now(),
        });
        Ok(true)
    }

    fn visible(&self, log_id: RaftLogId) {
        let mut state = self.state.lock().unwrap();
        let previous_visible = state.visible;
        if let Err(error) = advance(&mut state.visible, &log_id)
            && state.failed.is_none()
        {
            state.failed = Some(error.to_string());
        }
        if !state.recovery_ready {
            let previous = index_of(&previous_visible).unwrap_or(0);
            let startup = index_of(&state.startup_applied).unwrap_or(0);
            if log_id.index > previous.max(startup) {
                state.replayed_entries = state
                    .replayed_entries
                    .saturating_add(log_id.index - previous.max(startup));
            }
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn durable(&self, log_id: RaftLogId) {
        let mut state = self.state.lock().unwrap();
        if let Err(error) = advance(&mut state.durable, &log_id)
            && state.failed.is_none()
        {
            state.failed = Some(error.to_string());
        }
        if let Some(position) = state
            .pending
            .iter()
            .position(|pending| pending.log_id == log_id)
            && let Some(pending) = state.pending.remove(position)
        {
            state.pending_entries = state.pending_entries.saturating_sub(pending.entries);
            state.pending_bytes = state.pending_bytes.saturating_sub(pending.bytes);
            let latency = millis(pending.started.elapsed());
            state.last_flush_latency_ms = Some(latency);
            state.max_flush_latency_ms = state.max_flush_latency_ms.max(latency);
        }
        drop(state);
        self.changed.notify_waiters();
    }

    fn cancel(&self, log_id: RaftLogId, error: String) {
        let mut state = self.state.lock().unwrap();
        if let Some(position) = state
            .pending
            .iter()
            .position(|pending| pending.log_id == log_id)
            && let Some(pending) = state.pending.remove(position)
        {
            state.pending_entries = state.pending_entries.saturating_sub(pending.entries);
            state.pending_bytes = state.pending_bytes.saturating_sub(pending.bytes);
        }
        state.flush_failures = state.flush_failures.saturating_add(1);
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

    fn installed(&self, log_id: Option<RaftLogId>, requires_fence: bool) {
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
        state.startup_applied = log_id;
        state.recovery_epoch = NEXT_RECOVERY_EPOCH.fetch_add(1, Ordering::Relaxed);
        state.recovery_ready = !requires_fence;
        state.recovery_target = None;
        state.recovery_started = requires_fence.then(Instant::now);
        state.recovery_duration_ms = None;
        drop(state);
        self.changed.notify_waiters();
    }

    fn begin_recovery_attempt(&self, target: Option<RaftLogId>) -> u64 {
        let mut state = self.state.lock().unwrap();
        state.recovery_attempts = state.recovery_attempts.saturating_add(1);
        state.recovery_target = target;
        state.recovery_epoch
    }

    fn mark_recovery_ready(&self, epoch: u64, target: Option<&RaftLogId>) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        if state.recovery_epoch != epoch || state.closing {
            return Err(Error::Raft(
                "stale materialized-state recovery fence".into(),
            ));
        }
        if let Some(error) = &state.failed {
            return Err(Error::Io(std::io::Error::other(error.clone())));
        }
        if let Some(target) = target
            && !reaches(&state.visible, target)?
        {
            return Err(Error::Raft(format!(
                "materialized state has not reached recovery target {target}"
            )));
        }
        state.recovery_ready = true;
        state.recovery_target = target.copied();
        state.recovery_duration_ms = state.recovery_started.map(|at| millis(at.elapsed()));
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn dirty_expired(&self, max_age: Duration) -> bool {
        self.state
            .lock()
            .unwrap()
            .pending
            .front()
            .is_some_and(|pending| pending.started.elapsed() >= max_age)
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

    async fn wait_visible(&self, target: &RaftLogId) -> Result<()> {
        loop {
            let notified = self.changed.notified();
            {
                let state = self.state.lock().unwrap();
                if let Some(error) = &state.failed {
                    return Err(Error::Io(std::io::Error::other(error.clone())));
                }
                if reaches(&state.visible, target)? {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    fn record_wait(&self, kind: DurabilityWaitKind, elapsed: Duration) {
        let mut state = self.state.lock().unwrap();
        let elapsed = millis(elapsed);
        match kind {
            #[cfg(test)]
            DurabilityWaitKind::General => {}
            DurabilityWaitKind::Purge => {
                state.purge_waits = state.purge_waits.saturating_add(1);
                state.purge_wait_ms = state.purge_wait_ms.saturating_add(elapsed);
            }
            DurabilityWaitKind::Snapshot => {
                state.snapshot_waits = state.snapshot_waits.saturating_add(1);
                state.snapshot_wait_ms = state.snapshot_wait_ms.saturating_add(elapsed);
            }
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
pub(crate) struct ApplyDurabilityRegistry {
    groups: Mutex<HashMap<GroupId, Arc<GroupTracker>>>,
    failed: Mutex<Option<String>>,
    failure_hook: Mutex<Option<FailureHook>>,
    failure_changed: Notify,
    limits: ApplyDurabilityLimits,
}

impl ApplyDurabilityRegistry {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::with_limits(ApplyDurabilityLimits::default())
    }

    pub(crate) fn with_limits(limits: ApplyDurabilityLimits) -> Self {
        Self {
            groups: Mutex::new(HashMap::new()),
            failed: Mutex::new(None),
            failure_hook: Mutex::new(None),
            failure_changed: Notify::new(),
            limits,
        }
    }

    /// Couple registry failures to the database WAL worker. Installing the
    /// hook while holding the failure lock prevents a concurrent first failure
    /// from being missed.
    pub(crate) fn set_failure_hook(&self, hook: FailureHook) {
        let failed = self.failed.lock().unwrap();
        *self.failure_hook.lock().unwrap() = Some(hook.clone());
        let existing = failed.clone();
        drop(failed);
        if let Some(error) = existing {
            hook(error);
        }
    }

    pub(crate) fn spawn_watchdog(registry: &Arc<Self>) -> std::io::Result<()> {
        let weak: Weak<Self> = Arc::downgrade(registry);
        let interval = registry
            .limits
            .max_dirty_age
            .checked_div(4)
            .unwrap_or(Duration::from_millis(10))
            .clamp(Duration::from_millis(10), Duration::from_millis(100));
        std::thread::Builder::new()
            .name("dal-state-lag-watchdog".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    let Some(registry) = weak.upgrade() else {
                        return;
                    };
                    registry.expire_dirty();
                }
            })
            .map(|_| ())
    }

    pub(crate) fn register(&self, group: GroupId, recovered: Option<RaftLogId>) {
        let recovery_ready = matches!(group, GroupId::Meta);
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
        let recovery_ready = matches!(group, GroupId::Meta);
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
        self.expire_dirty();
        self.tracker(group).snapshot()
    }

    pub(crate) async fn begin(
        &self,
        group: GroupId,
        log_id: RaftLogId,
        entries: usize,
        bytes: usize,
    ) -> Result<()> {
        let tracker = self.tracker(group);
        loop {
            self.expire_dirty();
            let notified = tracker.changed.notified();
            if tracker.begin(log_id, entries, bytes, self.limits)? {
                return Ok(());
            }
            notified.await;
        }
    }

    pub(crate) fn visible(&self, group: GroupId, log_id: RaftLogId) {
        let tracker = self.tracker(group);
        tracker.visible(log_id);
        if let Some(error) = tracker.snapshot().failed {
            self.fail_all(error);
        }
    }

    pub(crate) fn durable(&self, group: GroupId, log_id: RaftLogId) {
        let tracker = self.tracker(group);
        tracker.durable(log_id);
        if let Some(error) = tracker.snapshot().failed {
            self.fail_all(error);
        }
    }

    pub(crate) fn cancel(&self, group: GroupId, log_id: RaftLogId, error: String) {
        self.tracker(group).cancel(log_id, error.clone());
        self.fail_all(error);
    }

    pub(crate) fn fail_all(&self, error: String) {
        let mut failed = self.failed.lock().unwrap();
        let first_failure = failed.is_none();
        if failed.is_none() {
            *failed = Some(error.clone());
        }
        let failure_hook = first_failure
            .then(|| self.failure_hook.lock().unwrap().clone())
            .flatten();
        drop(failed);
        if first_failure {
            self.failure_changed.notify_waiters();
        }
        if let Some(hook) = failure_hook {
            hook(error.clone());
        }
        let trackers: Vec<_> = self.groups.lock().unwrap().values().cloned().collect();
        for tracker in trackers {
            tracker.fail(error.clone());
        }
    }

    fn fail_on_corruption<T>(&self, result: Result<T>) -> Result<T> {
        if let Err(error @ Error::Corrupt(_, _)) = &result {
            self.fail_all(error.to_string());
        }
        result
    }

    pub(crate) fn ensure_healthy(&self) -> Result<()> {
        self.expire_dirty();
        match self.failed.lock().unwrap().as_ref() {
            Some(error) => Err(Error::Io(std::io::Error::other(error.clone()))),
            None => Ok(()),
        }
    }

    pub(crate) fn failure(&self) -> Option<String> {
        self.expire_dirty();
        self.failed.lock().unwrap().clone()
    }

    pub(crate) async fn wait_for_failure(&self) -> String {
        loop {
            // Register before checking the predicate so a concurrent failure
            // cannot be lost between the check and the await.
            let notified = self.failure_changed.notified();
            if let Some(error) = self.failure() {
                return error;
            }
            notified.await;
        }
    }

    pub(crate) async fn wait_durable(
        &self,
        group: GroupId,
        target: &RaftLogId,
        kind: DurabilityWaitKind,
    ) -> Result<()> {
        let tracker = self.tracker(group);
        let started = Instant::now();
        let result = tracker.wait_durable(target).await;
        tracker.record_wait(kind, started.elapsed());
        self.fail_on_corruption(result)
    }

    pub(crate) async fn wait_visible(&self, group: GroupId, target: &RaftLogId) -> Result<()> {
        let result = self.tracker(group).wait_visible(target).await;
        self.fail_on_corruption(result)
    }

    pub(crate) async fn drain(&self, group: GroupId) -> Result<()> {
        self.tracker(group).drain().await
    }

    pub(crate) fn begin_close(&self, group: GroupId) {
        self.tracker(group).begin_close();
    }

    pub(crate) fn installed(&self, group: GroupId, log_id: Option<RaftLogId>) {
        let tracker = self.tracker(group);
        tracker.installed(log_id, matches!(group, GroupId::Data(_)));
        if let Some(error) = tracker.snapshot().failed {
            self.fail_all(error);
        }
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

    pub(crate) fn begin_recovery_attempt(&self, group: GroupId, target: Option<RaftLogId>) -> u64 {
        self.tracker(group).begin_recovery_attempt(target)
    }

    pub(crate) fn mark_recovery_ready(
        &self,
        group: GroupId,
        epoch: u64,
        target: Option<&RaftLogId>,
    ) -> Result<()> {
        let result = self.tracker(group).mark_recovery_ready(epoch, target);
        self.fail_on_corruption(result)
    }

    fn expire_dirty(&self) {
        if self.failed.lock().unwrap().is_some() {
            return;
        }
        let expired = self
            .groups
            .lock()
            .unwrap()
            .iter()
            .find_map(|(group, tracker)| {
                tracker
                    .dirty_expired(self.limits.max_dirty_age)
                    .then_some(*group)
            });
        if let Some(group) = expired {
            self.fail_all(format!(
                "materialized state for {group:?} exceeded dirty-age limit of {} ms",
                self.limits.max_dirty_age.as_millis()
            ));
        }
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

fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
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
    use proptest::prelude::*;

    fn log_id(term: u64, index: u64) -> RaftLogId {
        RaftLogId::new(CommittedLeaderId::new(term, 1), index)
    }

    #[tokio::test]
    async fn visible_and_durable_are_independent_and_monotonic() {
        let registry = ApplyDurabilityRegistry::new();
        let group = GroupId::Data(1);
        registry.register(group, Some(log_id(1, 3)));

        registry.begin(group, log_id(1, 5), 2, 100).await.unwrap();
        registry.visible(group, log_id(1, 5));
        let visible = registry.snapshot(group);
        assert_eq!(visible.visible, Some(log_id(1, 5)));
        assert_eq!(visible.durable, Some(log_id(1, 3)));
        assert!(!visible.recovery_ready);
        let epoch = registry.begin_recovery_attempt(group, Some(log_id(1, 5)));
        registry
            .mark_recovery_ready(group, epoch, Some(&log_id(1, 5)))
            .unwrap();

        registry.durable(group, log_id(1, 5));
        registry
            .wait_durable(group, &log_id(1, 5), DurabilityWaitKind::General)
            .await
            .unwrap();
        let durable = registry.snapshot(group);
        assert_eq!(durable.durable, Some(log_id(1, 5)));
        assert_eq!(durable.pending_entries, 0);
        assert_eq!(durable.pending_bytes, 0);

        registry.installed(group, Some(log_id(2, 8)));
        registry.durable(group, log_id(1, 6));
        assert_eq!(registry.snapshot(group).durable, Some(log_id(2, 8)));
    }

    #[tokio::test]
    async fn mismatch_at_same_index_is_corruption() {
        let registry = ApplyDurabilityRegistry::new();
        let group = GroupId::Data(1);
        registry.register(group, Some(log_id(1, 3)));
        assert!(
            registry
                .wait_durable(group, &log_id(2, 3), DurabilityWaitKind::General)
                .await
                .is_err()
        );
        assert!(registry.ensure_healthy().is_err());
        assert!(
            registry
                .failure()
                .is_some_and(|error| error.contains("log id mismatch at index 3"))
        );

        let sibling = GroupId::Data(2);
        registry.register(sibling, None);
        assert!(
            registry.begin(sibling, log_id(1, 4), 1, 10).await.is_err(),
            "watermark corruption must fence every group sharing the database"
        );
    }

    #[tokio::test]
    async fn global_failure_poison_applies_to_groups_registered_later() {
        let registry = ApplyDurabilityRegistry::new();
        registry.fail_all("flush failed".into());
        let group = GroupId::Data(2);
        registry.register(group, Some(log_id(1, 1)));

        let state = registry.snapshot(group);
        assert_eq!(state.failed.as_deref(), Some("flush failed"));
        assert!(registry.begin(group, log_id(1, 2), 1, 10).await.is_err());
    }

    #[tokio::test]
    async fn closing_rejects_new_applies_and_allows_existing_ones_to_drain() {
        let registry = ApplyDurabilityRegistry::new();
        let group = GroupId::Data(3);
        registry.register(group, None);
        registry.begin(group, log_id(1, 1), 1, 10).await.unwrap();
        registry.begin_close(group);

        assert!(registry.begin(group, log_id(1, 2), 1, 10).await.is_err());
        assert!(registry.ensure_drained(group).is_err());
        registry.visible(group, log_id(1, 1));
        registry.durable(group, log_id(1, 1));
        registry.drain(group).await.unwrap();
        registry.ensure_drained(group).unwrap();
    }

    #[tokio::test]
    async fn per_group_byte_limit_blocks_until_the_oldest_batch_is_durable() {
        let registry = Arc::new(ApplyDurabilityRegistry::with_limits(
            ApplyDurabilityLimits {
                max_pending_entries: 10,
                max_pending_bytes: 100,
                max_dirty_age: Duration::from_secs(2),
            },
        ));
        let group = GroupId::Data(6);
        registry.register(group, None);
        registry.begin(group, log_id(1, 1), 1, 80).await.unwrap();

        let blocked_registry = registry.clone();
        let blocked =
            tokio::spawn(async move { blocked_registry.begin(group, log_id(1, 2), 1, 80).await });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!blocked.is_finished());
        registry.durable(group, log_id(1, 1));
        blocked.await.unwrap().unwrap();
        registry.durable(group, log_id(1, 2));
    }

    #[tokio::test]
    async fn a_single_batch_cannot_bypass_group_limits() {
        let registry = ApplyDurabilityRegistry::with_limits(ApplyDurabilityLimits {
            max_pending_entries: 2,
            max_pending_bytes: 100,
            max_dirty_age: Duration::from_secs(2),
        });
        let group = GroupId::Data(7);
        registry.register(group, None);

        assert!(registry.begin(group, log_id(1, 1), 3, 10).await.is_err());
        assert!(registry.begin(group, log_id(1, 1), 1, 101).await.is_err());
        let state = registry.snapshot(group);
        assert_eq!(state.pending_entries, 0);
        assert_eq!(state.pending_bytes, 0);
    }

    #[test]
    fn snapshot_install_may_advance_but_never_replace_with_an_older_prefix() {
        let registry = ApplyDurabilityRegistry::new();
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

    #[test]
    fn snapshot_install_invalidates_an_inflight_recovery_epoch() {
        let registry = ApplyDurabilityRegistry::new();
        let group = GroupId::Data(5);
        registry.register(group, Some(log_id(2, 8)));
        let old_epoch = registry.begin_recovery_attempt(group, Some(log_id(2, 8)));

        registry.installed(group, Some(log_id(3, 12)));
        assert!(
            registry
                .mark_recovery_ready(group, old_epoch, Some(&log_id(2, 8)))
                .is_err()
        );
        assert!(!registry.snapshot(group).recovery_ready);

        let current_epoch = registry.begin_recovery_attempt(group, Some(log_id(3, 12)));
        registry
            .mark_recovery_ready(group, current_epoch, Some(&log_id(3, 12)))
            .unwrap();
        assert!(registry.snapshot(group).recovery_ready);
    }

    proptest! {
        #[test]
        fn capd_crash_model_preserves_prefix_and_replay_invariants(
            transitions in prop::collection::vec(0u8..10, 1..512)
        ) {
            let mut l = 0u64;
            let mut c = 0u64;
            let mut a = 0u64;
            let mut d = 0u64;
            let mut p = 0u64;
            let mut snapshot = 0u64;
            let mut visible_state = 0u64;
            let mut durable_state = 0u64;

            // Entry i deterministically contributes i to the reference state.
            let prefix = |index: u64| index.saturating_mul(index + 1) / 2;

            for transition in transitions {
                match transition {
                    // Append a locally durable Raft entry.
                    0 => l = l.saturating_add(1),
                    // Learn one more committed entry already present locally.
                    1 if c < l => c += 1,
                    // Apply one committed entry to visible materialized state.
                    2 if a < c => {
                        a += 1;
                        visible_state = prefix(a);
                    }
                    // Complete one more state-WAL durability boundary.
                    3 if d < a => {
                        d += 1;
                        durable_state = prefix(d);
                    }
                    // Purge is admitted only through the durable prefix.
                    4 if p < d => p += 1,
                    // Snapshot publication first drains through captured A.
                    5 => {
                        d = a;
                        durable_state = visible_state;
                        snapshot = a;
                    }
                    // Power loss discards (D,A]. The retained log suffix must
                    // reproduce the exact reference state through true C.
                    6 => {
                        a = d;
                        visible_state = durable_state;
                        prop_assert_eq!(visible_state, prefix(d));
                        prop_assert!(p <= d, "replay source was purged above D");
                        let mut replayed = durable_state;
                        for index in (d + 1)..=c {
                            replayed = replayed.saturating_add(index);
                        }
                        prop_assert_eq!(replayed, prefix(c));
                    }
                    // A current leader re-establishes C and drives replay.
                    7 => {
                        a = c;
                        visible_state = prefix(c);
                    }
                    // An attempted visible-only purge must be refused.
                    8 => {
                        let requested = a;
                        if requested <= d {
                            p = p.max(requested);
                        }
                    }
                    // A compact steady-state transition.
                    9 => {
                        l = l.saturating_add(1);
                        c = l;
                        a = c;
                        visible_state = prefix(a);
                    }
                    _ => {}
                }

                prop_assert!(p <= d, "P={p} exceeded D={d}");
                prop_assert!(d <= a, "D={d} exceeded A={a}");
                prop_assert!(a <= c, "A={a} exceeded C={c}");
                prop_assert!(c <= l, "C={c} exceeded L={l}");
                prop_assert!(snapshot <= d);
                prop_assert_eq!(visible_state, prefix(a));
                prop_assert_eq!(durable_state, prefix(d));
            }
        }
    }
}
