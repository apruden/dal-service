//! `dal` binary entry point.
//!
//! Subcommands (`run`, `init`, `join`, `leave`, `abort-plan`, `status`) are
//! wired up as their supporting milestones land. For now the binary reports
//! build identity so the crate has a runnable target.

fn main() {
    println!("dal-service {} (M1 foundation)", env!("CARGO_PKG_VERSION"));
}
