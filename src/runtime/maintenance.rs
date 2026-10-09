//! Durable node-local client admission for a replicated maintenance operation.
//!
//! The gate starts closed on every process start. Only a ReadIndex-confirmed
//! meta view may reconcile it, including when no operation has ever existed.
//! A read permit covers the full accepted operation; installing a new fence
//! waits for those permits before its fsync-durable record can be acknowledged.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::stream::{FuturesUnordered, StreamExt};
use tokio::sync::{Mutex, OwnedRwLockReadGuard, RwLock};

use crate::codec;
use crate::error::{Error, Result};
use crate::keyspace;
use crate::storage::Storage;
use crate::transport::Transport;
use crate::transport::codec::{Envelope, MsgType};
use crate::transport::raft_wire::MaintenanceQueryReply;
use crate::types::{ClusterId, GroupId, MaintenanceRecord, MaintenanceStage};

#[derive(Clone)]
pub struct MaintenanceAdmission {
    storage: Arc<Storage>,
    record: Arc<RwLock<Option<MaintenanceRecord>>>,
    open: Arc<AtomicBool>,
    reconcile_lock: Arc<Mutex<()>>,
}

impl MaintenanceAdmission {
    pub fn new(storage: Arc<Storage>) -> Result<Self> {
        let record = storage.get_local(&keyspace::maintenance_fence_key())?;
        Ok(Self {
            storage,
            record: Arc::new(RwLock::new(record)),
            open: Arc::new(AtomicBool::new(false)),
            reconcile_lock: Arc::new(Mutex::new(())),
        })
    }

    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
    }

    /// Hold this permit through a client operation's Raft result. A pending
    /// fence blocks new readers and waits for all existing permits to drain.
    pub async fn admit(&self) -> Option<OwnedRwLockReadGuard<Option<MaintenanceRecord>>> {
        let permit = self.record.clone().read_owned().await;
        self.is_open().then_some(permit)
    }

    /// Apply a ReadIndex-confirmed meta view. Returning from a Preparing view
    /// means the local record is durable and every earlier admitted operation
    /// has completed. A release is persisted before admission reopens. Keep the
    /// reconcile lock through the drain: a hung accepted Raft proposal must
    /// hold preparation closed instead of letting a later release overtake it.
    pub async fn reconcile(&self, authoritative: Option<MaintenanceRecord>) -> Result<()> {
        let _reconcile = self.reconcile_lock.lock().await;
        {
            let current = self.record.read().await;
            if *current == authoritative {
                // A fresh ReadIndex-confirmed exact match with the durable
                // local record resolves any earlier conflicting observation.
                self.open.store(
                    authoritative
                        .as_ref()
                        .is_none_or(|record| record.stage == MaintenanceStage::Released),
                    Ordering::Release,
                );
                return Ok(());
            }
            match validate_transition(current.as_ref(), authoritative.as_ref()) {
                Ok(()) => {}
                Err(TransitionIssue::Stale) => {
                    // A delayed confirmed read cannot undo a newer installed
                    // release or generation.
                    return Err(Error::Raft("stale maintenance generation".into()));
                }
                Err(TransitionIssue::Conflict(reason)) => {
                    self.open.store(false, Ordering::Release);
                    return Err(Error::Raft(reason.into()));
                }
            }
        }

        self.open.store(false, Ordering::Release);
        let mut current = self.record.write().await;
        if let Some(record) = &authoritative {
            self.storage
                .put_local(&keyspace::maintenance_fence_key(), record)?;
        }
        *current = authoritative;
        self.open.store(
            current
                .as_ref()
                .is_none_or(|record| record.stage == MaintenanceStage::Released),
            Ordering::Release,
        );
        Ok(())
    }
}

enum TransitionIssue {
    Stale,
    Conflict(&'static str),
}

fn validate_transition(
    current: Option<&MaintenanceRecord>,
    next: Option<&MaintenanceRecord>,
) -> std::result::Result<(), TransitionIssue> {
    let Some(current) = current else {
        return Ok(());
    };
    let Some(next) = next else {
        // Meta retains released generations. A missing record beside a local
        // fence indicates a different or damaged cluster state; keep closed.
        return Err(TransitionIssue::Conflict(
            "authoritative maintenance record disappeared",
        ));
    };
    if next.generation < current.generation {
        return Err(TransitionIssue::Stale);
    }
    if next.generation == current.generation {
        if next.operation_id != current.operation_id || next.participants != current.participants {
            return Err(TransitionIssue::Conflict(
                "conflicting maintenance generation",
            ));
        }
        if current.stage == MaintenanceStage::Released && next.stage != MaintenanceStage::Released {
            return Err(TransitionIssue::Stale);
        }
    }
    Ok(())
}

/// Read the maintenance record from the current meta leader. Follower and
/// unavailable replies never authorize admission, including a missing record.
pub async fn fetch_authoritative_maintenance<T: Transport>(
    transport: &T,
    cluster_id: ClusterId,
    addresses: impl IntoIterator<Item = String>,
    timeout: Duration,
) -> Result<Option<MaintenanceRecord>> {
    let read = async {
        let mut pending = FuturesUnordered::new();
        for address in addresses {
            let request = Envelope::new(
                cluster_id,
                MsgType::MaintenanceQuery,
                GroupId::Meta,
                0,
                Vec::new(),
            );
            pending.push(async move { transport.call(&address, request).await });
        }
        while let Some(reply) = pending.next().await {
            let Ok(reply) = reply else { continue };
            if reply.cluster_id != cluster_id
                || reply.msg_type != MsgType::MaintenanceQuery
                || reply.group_id != GroupId::Meta
            {
                continue;
            }
            if let Ok(MaintenanceQueryReply::Value(record)) = codec::decode(&reply.payload) {
                return Ok(record);
            }
        }
        Err(Error::Raft(
            "no meta leader returned authoritative maintenance state".into(),
        ))
    };
    tokio::time::timeout(timeout, read)
        .await
        .map_err(|_| Error::Raft("maintenance query timed out".into()))?
}
