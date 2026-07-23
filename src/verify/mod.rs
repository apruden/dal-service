//! System-verification toolkit (IMPLEMENTATION §M7): a deterministic PRNG, an
//! executable single-register linearizability checker, and whole-run oracles.
//! The fault-injection harness that drives these lives in `tests/verify_m7.rs`.

pub mod linearizability;
pub mod oracles;
pub mod rng;
