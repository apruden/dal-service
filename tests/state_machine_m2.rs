//! M2 gate: idempotency, CAS algebra, sequence discipline, and prefix recovery.

use std::sync::Mutex;

use dal::partition::{ApplyResult, DataStateMachine, RejectReason};
use dal::storage::Storage;
use dal::types::{DataOp, DataRequest, GroupId, IfVersion, KeyPresence, MutationResult};

static SERIAL: Mutex<()> = Mutex::new(());
const G: GroupId = GroupId::Data(0);

fn sm_storage() -> (DataStateMachine, tempfile::TempDir, Storage) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    storage.ensure_group(G).unwrap();
    (DataStateMachine::new(G), dir, storage)
}

fn put(client: u128, seq: u64, key: &[u8], val: &[u8], iv: Option<IfVersion>) -> DataRequest {
    DataRequest {
        client_id: client,
        sequence: seq,
        op: DataOp::Put {
            key: key.to_vec(),
            value: val.to_vec(),
            if_version: iv,
        },
    }
}

fn del(client: u128, seq: u64, key: &[u8], iv: Option<IfVersion>) -> DataRequest {
    DataRequest {
        client_id: client,
        sequence: seq,
        op: DataOp::Delete {
            key: key.to_vec(),
            if_version: iv,
        },
    }
}

#[test]
fn basic_put_get_delete() {
    let (sm, _d, s) = sm_storage();
    let r = sm.apply(&s, &put(1, 1, b"k", b"v", None), 10).unwrap();
    assert_eq!(r, ApplyResult::Decided(MutationResult::Applied { version: 10 }));
    assert_eq!(sm.get(&s, b"k").unwrap(), Some((10, b"v".to_vec())));

    let r = sm.apply(&s, &del(1, 2, b"k", None), 11).unwrap();
    assert_eq!(r, ApplyResult::Decided(MutationResult::Applied { version: 11 }));
    assert_eq!(sm.get(&s, b"k").unwrap(), None);
}

#[test]
fn retry_returns_stored_result_without_reapplying() {
    let (sm, _d, s) = sm_storage();
    sm.apply(&s, &put(1, 1, b"k", b"v", None), 10).unwrap();
    // Same (client, sequence, op) replayed at a later index: stored result,
    // and crucially the key version is NOT bumped to the new index.
    let r = sm.apply(&s, &put(1, 1, b"k", b"v", None), 50).unwrap();
    assert_eq!(r, ApplyResult::Replayed(MutationResult::Applied { version: 10 }));
    assert_eq!(sm.get(&s, b"k").unwrap(), Some((10, b"v".to_vec())));
}

#[test]
fn same_sequence_different_command_is_rejected() {
    let (sm, _d, s) = sm_storage();
    sm.apply(&s, &put(1, 1, b"k", b"v", None), 10).unwrap();
    let r = sm.apply(&s, &put(1, 1, b"k", b"DIFFERENT", None), 11).unwrap();
    assert_eq!(r, ApplyResult::Rejected(RejectReason::SequenceMismatch));
    // Original value untouched.
    assert_eq!(sm.get(&s, b"k").unwrap(), Some((10, b"v".to_vec())));
}

#[test]
fn failed_cas_advances_highest_avoiding_wedge() {
    // The §8.4 wedge case: a failed CAS must still advance `highest`, else the
    // client's next sequence is seen as a gap and its stream wedges forever.
    let (sm, _d, s) = sm_storage();
    sm.apply(&s, &put(1, 1, b"k", b"v", None), 10).unwrap(); // version 10
    let r = sm
        .apply(&s, &put(1, 2, b"k", b"x", Some(IfVersion::Number(999))), 11)
        .unwrap();
    assert_eq!(
        r,
        ApplyResult::Decided(MutationResult::ConditionFailed {
            current: KeyPresence::Present { version: 10 }
        })
    );
    // Next sequence proceeds (not treated as a gap).
    let r = sm.apply(&s, &put(1, 3, b"k", b"w", None), 12).unwrap();
    assert_eq!(r, ApplyResult::Decided(MutationResult::Applied { version: 12 }));
}

#[test]
fn numeric_cas_against_absent_fails() {
    let (sm, _d, s) = sm_storage();
    let r = sm
        .apply(&s, &put(1, 1, b"k", b"v", Some(IfVersion::Number(0))), 10)
        .unwrap();
    assert_eq!(
        r,
        ApplyResult::Decided(MutationResult::ConditionFailed {
            current: KeyPresence::Absent
        })
    );
    assert_eq!(sm.get(&s, b"k").unwrap(), None);
}

#[test]
fn put_absent_is_create_only() {
    let (sm, _d, s) = sm_storage();
    let r = sm
        .apply(&s, &put(1, 1, b"k", b"v", Some(IfVersion::Absent)), 10)
        .unwrap();
    assert_eq!(r, ApplyResult::Decided(MutationResult::Applied { version: 10 }));
    // Second create-only fails: key now present.
    let r = sm
        .apply(&s, &put(1, 2, b"k", b"v2", Some(IfVersion::Absent)), 11)
        .unwrap();
    assert_eq!(
        r,
        ApplyResult::Decided(MutationResult::ConditionFailed {
            current: KeyPresence::Present { version: 10 }
        })
    );
}

#[test]
fn delete_recreate_keeps_versions_strictly_increasing() {
    let (sm, _d, s) = sm_storage();
    sm.apply(&s, &put(1, 1, b"k", b"a", None), 10).unwrap();
    sm.apply(&s, &del(1, 2, b"k", None), 20).unwrap();
    sm.apply(&s, &put(1, 3, b"k", b"b", None), 30).unwrap();
    // No ABA: the recreated key has a strictly larger version than before.
    assert_eq!(sm.get(&s, b"k").unwrap(), Some((30, b"b".to_vec())));
}

#[test]
fn unconditional_delete_of_absent_is_applied() {
    let (sm, _d, s) = sm_storage();
    let r = sm.apply(&s, &del(1, 1, b"missing", None), 10).unwrap();
    assert_eq!(r, ApplyResult::Decided(MutationResult::Applied { version: 10 }));
    assert_eq!(sm.get(&s, b"missing").unwrap(), None);
}

#[test]
fn delete_with_absent_sentinel_is_malformed() {
    let (sm, _d, s) = sm_storage();
    let r = sm
        .apply(&s, &del(1, 1, b"k", Some(IfVersion::Absent)), 10)
        .unwrap();
    assert_eq!(r, ApplyResult::Rejected(RejectReason::Malformed));
    // Malformed advanced last_applied only; the sequence was not consumed.
    assert_eq!(s.last_applied(G).unwrap().unwrap().index, 10);
    // A real command at sequence 1 still works (sequence not burned).
    let r = sm.apply(&s, &put(1, 1, b"k", b"v", None), 11).unwrap();
    assert_eq!(r, ApplyResult::Decided(MutationResult::Applied { version: 11 }));
}

#[test]
fn gapped_and_stale_sequences_are_rejected() {
    let (sm, _d, s) = sm_storage();
    sm.apply(&s, &put(1, 1, b"k", b"v", None), 10).unwrap();
    // Gap: highest is 1, jump to 3.
    let r = sm.apply(&s, &put(1, 3, b"k", b"v", None), 11).unwrap();
    assert!(matches!(
        r,
        ApplyResult::Rejected(RejectReason::SequenceGap { expected: 2, got: 3 })
    ));
    // Stale: sequence below highest.
    sm.apply(&s, &put(1, 2, b"k", b"v2", None), 12).unwrap();
    let r = sm.apply(&s, &put(1, 1, b"k", b"v", None), 13).unwrap();
    assert!(matches!(
        r,
        ApplyResult::Rejected(RejectReason::StaleSequence { highest: 2, got: 1 })
    ));
}

#[test]
fn every_committed_entry_advances_last_applied() {
    // Decided, replayed, and rejected entries all move last_applied forward so
    // Raft can never wedge.
    let (sm, _d, s) = sm_storage();
    sm.apply(&s, &put(1, 1, b"k", b"v", None), 10).unwrap();
    sm.apply(&s, &put(1, 1, b"k", b"v", None), 11).unwrap(); // replay
    sm.apply(&s, &put(1, 5, b"k", b"v", None), 12).unwrap(); // gap reject
    assert_eq!(s.last_applied(G).unwrap().unwrap().index, 12);
}

#[test]
fn independent_clients_have_independent_sequences() {
    let (sm, _d, s) = sm_storage();
    sm.apply(&s, &put(1, 1, b"a", b"1", None), 10).unwrap();
    // Different client also starts at sequence 1.
    let r = sm.apply(&s, &put(2, 1, b"b", b"2", None), 11).unwrap();
    assert_eq!(r, ApplyResult::Decided(MutationResult::Applied { version: 11 }));
}

#[test]
fn crash_between_applies_recovers_to_prefix() {
    let _guard = SERIAL.lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    {
        let s = Storage::open(dir.path()).unwrap();
        s.ensure_group(G).unwrap();
        let sm = DataStateMachine::new(G);
        sm.apply(&s, &put(1, 1, b"k", b"a", None), 10).unwrap();
        sm.apply(&s, &put(1, 2, b"k", b"b", None), 11).unwrap();

        // Crash while applying the third entry.
        fail::cfg("apply_state::before_write", "return").unwrap();
        let r = sm.apply(&s, &put(1, 3, b"k", b"c", None), 12);
        fail::remove("apply_state::before_write");
        assert!(r.is_err());
    }
    // Reopen and confirm we recovered to the committed prefix (index 11), then
    // re-apply entry 12 successfully — the client stream is not wedged.
    let s = Storage::open(dir.path()).unwrap();
    let sm = DataStateMachine::new(G);
    assert_eq!(s.last_applied(G).unwrap().unwrap().index, 11);
    assert_eq!(sm.get(&s, b"k").unwrap(), Some((11, b"b".to_vec())));
    let r = sm.apply(&s, &put(1, 3, b"k", b"c", None), 12).unwrap();
    assert_eq!(r, ApplyResult::Decided(MutationResult::Applied { version: 12 }));
}
