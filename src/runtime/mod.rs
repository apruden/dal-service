//! Production server runtime (M8): wires the library into the `dal` binary.

pub mod config_file;
pub mod dispatch;
pub mod http;
pub mod node;
pub mod rebalance;
