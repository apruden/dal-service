//! The meta group: authoritative cluster identity, node directory, and
//! placement map (DESIGN §3.1, §5). M5 delivers the state machine's in-apply
//! validation; the running meta Raft group and bootstrap driver build on it.

pub mod state_machine;
