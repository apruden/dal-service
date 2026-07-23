//! M1 gate: identity-mismatch rejection and crash-point recovery.
//!
//! Failpoints are process-global, so every test here holds `SERIAL` while a
//! failpoint may be active and resets it before releasing. This keeps the
//! suite correct even under cargo's default parallel test runner.

use std::sync::Mutex;

use dal::error::Error;
use dal::keyspace;
use dal::meta::bootstrap::ensure_bootstrap_group;
use dal::storage::{StateMutation, Storage};
use dal::types::{BootstrapGroup, GroupId, LogId, ServingState};

static SERIAL: Mutex<()> = Mutex::new(());

const G: GroupId = GroupId::Data(7);

fn put(key: &[u8], value: &[u8]) -> StateMutation {
    StateMutation::Put {
        key: keyspace::user_key(key),
        value: value.to_vec(),
    }
}

fn read(storage: &Storage, key: &[u8]) -> Option<Vec<u8>> {
    storage.get_state(G, &keyspace::user_key(key)).unwrap()
}

// ---- identity --------------------------------------------------------------

#[test]
fn fresh_open_persists_identity_and_matches_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let s = Storage::open_checked(dir.path(), 0xABCD, 42).unwrap();
        assert_eq!(s.identity().unwrap().unwrap().cluster_id, 0xABCD);
    }
    // Reopen with the same identity: fine.
    Storage::open_checked(dir.path(), 0xABCD, 42).unwrap();
}

#[test]
fn wrong_cluster_id_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    Storage::open_checked(dir.path(), 0xABCD, 42).unwrap();
    let r = Storage::open_checked(dir.path(), 0x1234, 42);
    assert!(matches!(r, Err(Error::IdentityMismatch { .. })));
}

#[test]
fn wrong_node_id_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    Storage::open_checked(dir.path(), 0xABCD, 42).unwrap();
    let r = Storage::open_checked(dir.path(), 0xABCD, 99);
    assert!(matches!(r, Err(Error::IdentityMismatch { .. })));
}

// ---- CF lifecycle ----------------------------------------------------------

#[test]
fn group_cf_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let s = Storage::open(dir.path()).unwrap();
    assert!(!s.group_exists(G));
    s.ensure_group(G).unwrap();
    assert!(s.group_exists(G));
    s.ensure_group(G).unwrap(); // idempotent
    s.drop_group(G).unwrap();
    assert!(!s.group_exists(G));
    s.drop_group(G).unwrap(); // idempotent
}

#[test]
fn cfs_survive_reopen() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    {
        let s = Storage::open(dir.path()).unwrap();
        s.ensure_group(G).unwrap();
        s.apply_state(G, &[put(b"k", b"v")], LogId::new(1, 1))
            .unwrap();
    }
    let s = Storage::open(dir.path()).unwrap();
    assert!(s.group_exists(G));
    assert_eq!(read(&s, b"k"), Some(b"v".to_vec()));
    assert_eq!(s.last_applied(G).unwrap(), Some(LogId::new(1, 1)));
}

// ---- crash recovery --------------------------------------------------------

#[test]
fn sequential_applies_recover_to_last_prefix() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    {
        let s = Storage::open(dir.path()).unwrap();
        s.ensure_group(G).unwrap();
        s.apply_state(G, &[put(b"a", b"1")], LogId::new(1, 1))
            .unwrap();
        s.apply_state(G, &[put(b"b", b"2")], LogId::new(1, 2))
            .unwrap();
        s.apply_state(G, &[put(b"a", b"3")], LogId::new(1, 3))
            .unwrap();
    }
    let s = Storage::open(dir.path()).unwrap();
    assert_eq!(s.last_applied(G).unwrap(), Some(LogId::new(1, 3)));
    assert_eq!(read(&s, b"a"), Some(b"3".to_vec()));
    assert_eq!(read(&s, b"b"), Some(b"2".to_vec()));
}

// ---- serving-gate reclamation (DESIGN §7.4, M6) ----------------------------

#[test]
fn reclaim_records_non_serving_before_dropping_cfs() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    {
        let s = Storage::open(dir.path()).unwrap();
        s.ensure_group(G).unwrap();
        s.apply_state(G, &[put(b"k", b"v")], LogId::new(1, 1))
            .unwrap();
        assert!(s.group_exists(G));
        assert_eq!(s.serving_state(G).unwrap(), None);

        s.reclaim_group(G).unwrap();
        // Non-serving is recorded and the CFs are gone.
        assert_eq!(s.serving_state(G).unwrap(), Some(ServingState::NonServing));
        assert!(!s.group_exists(G));
    }
    // The non-serving record survives reopen (default CF); the group stays gone,
    // so the amnesia rule keeps this node from silently re-participating.
    let s = Storage::open(dir.path()).unwrap();
    assert_eq!(s.serving_state(G).unwrap(), Some(ServingState::NonServing));
    assert!(!s.group_exists(G));
}

#[test]
fn group_start_requires_durable_admission_and_never_revives_non_serving() {
    let dir = tempfile::tempdir().unwrap();
    let s = Storage::open_checked(dir.path(), 0xABCD, 42).unwrap();
    assert!(s.authorize_group_start(G, 42).is_err());
    assert!(!s.group_exists(G));

    ensure_bootstrap_group(
        &s,
        &BootstrapGroup {
            cluster_id: 0xABCD,
            group: G,
            members: vec![42],
        },
    )
    .unwrap();
    s.authorize_group_start(G, 42).unwrap();
    assert!(s.group_exists(G));
    assert_eq!(s.serving_state(G).unwrap(), Some(ServingState::Serving));

    s.reclaim_group(G).unwrap();
    assert!(s.authorize_group_start(G, 42).is_err());
    assert!(s.require_serving(G).is_err());
    assert!(!s.group_exists(G));
}

#[test]
fn snapshot_replace_is_atomic_at_both_crash_boundaries() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let s = Storage::open(dir.path()).unwrap();
    s.ensure_group(G).unwrap();
    s.apply_state(G, &[put(b"old", b"v1")], LogId::new(1, 1))
        .unwrap();
    let pairs = vec![(keyspace::user_key(b"new"), b"v2".to_vec())];
    let applied = dal::codec::encode(&LogId::new(2, 4));

    fail::cfg("snapshot_install::before_write", "return").unwrap();
    assert!(s.install_state(G, &pairs, &applied).is_err());
    fail::remove("snapshot_install::before_write");
    assert_eq!(read(&s, b"old"), Some(b"v1".to_vec()));
    assert_eq!(read(&s, b"new"), None);

    fail::cfg("snapshot_install::after_write", "return").unwrap();
    assert!(s.install_state(G, &pairs, &applied).is_err());
    fail::remove("snapshot_install::after_write");
    assert_eq!(read(&s, b"old"), None);
    assert_eq!(read(&s, b"new"), Some(b"v2".to_vec()));
}

#[test]
fn crash_before_write_leaves_previous_state() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    {
        let s = Storage::open(dir.path()).unwrap();
        s.ensure_group(G).unwrap();
        s.apply_state(G, &[put(b"a", b"1")], LogId::new(1, 1))
            .unwrap();

        fail::cfg("apply_state::before_write", "return").unwrap();
        let r = s.apply_state(G, &[put(b"a", b"2"), put(b"c", b"9")], LogId::new(1, 2));
        fail::remove("apply_state::before_write");
        assert!(r.is_err(), "crash point must surface as an error");
    }
    // Reopen: the crashed batch left no trace; recovery is a clean prefix.
    let s = Storage::open(dir.path()).unwrap();
    assert_eq!(s.last_applied(G).unwrap(), Some(LogId::new(1, 1)));
    assert_eq!(read(&s, b"a"), Some(b"1".to_vec()));
    assert_eq!(read(&s, b"c"), None, "no partial batch may be visible");
}

#[test]
fn crash_after_write_is_durable() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    {
        let s = Storage::open(dir.path()).unwrap();
        s.ensure_group(G).unwrap();
        s.apply_state(G, &[put(b"a", b"1")], LogId::new(1, 1))
            .unwrap();

        fail::cfg("apply_state::after_write", "return").unwrap();
        let r = s.apply_state(G, &[put(b"a", b"2")], LogId::new(1, 2));
        fail::remove("apply_state::after_write");
        // The write completed before the post-write crash point fired.
        assert!(r.is_err());
    }
    let s = Storage::open(dir.path()).unwrap();
    assert_eq!(s.last_applied(G).unwrap(), Some(LogId::new(1, 2)));
    assert_eq!(read(&s, b"a"), Some(b"2".to_vec()));
}

#[test]
fn multi_mutation_batch_is_all_or_nothing() {
    let _guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    let dir = tempfile::tempdir().unwrap();
    {
        let s = Storage::open(dir.path()).unwrap();
        s.ensure_group(G).unwrap();
        s.apply_state(
            G,
            &[put(b"x", b"1"), put(b"y", b"2"), put(b"z", b"3")],
            LogId::new(2, 5),
        )
        .unwrap();

        fail::cfg("apply_state::before_write", "return").unwrap();
        let muts = vec![
            put(b"x", b"10"),
            StateMutation::Delete {
                key: keyspace::user_key(b"y"),
            },
            put(b"w", b"99"),
        ];
        let r = s.apply_state(G, &muts, LogId::new(2, 6));
        fail::remove("apply_state::before_write");
        assert!(r.is_err());
    }
    let s = Storage::open(dir.path()).unwrap();
    assert_eq!(s.last_applied(G).unwrap(), Some(LogId::new(2, 5)));
    assert_eq!(read(&s, b"x"), Some(b"1".to_vec()));
    assert_eq!(read(&s, b"y"), Some(b"2".to_vec()));
    assert_eq!(read(&s, b"w"), None);
}
