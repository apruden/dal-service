//! Production server runtime (M8): wires the library into the `dal` binary.

pub mod admin;
pub mod config_file;
pub mod directory;
pub mod dispatch;
pub mod http;
pub mod node;
pub mod readiness;
pub mod rebalance;
pub mod search;
