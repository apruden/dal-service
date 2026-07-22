//! `dal` binary entry point.
//!
//! Subcommands: `init` (create a cluster), `join` (register a node), `status`
//! (render the routing snapshot), plus `run`. The heavy lifting lives in the
//! library (`dal::meta::bootstrap`, `dal::api`); this is thin argument plumbing.

use std::process::ExitCode;

fn usage() -> &'static str {
    "usage: dal <command> [args]\n\
     \n\
     commands:\n\
       init    --config <path>     create a new cluster from a bootstrap descriptor\n\
       join    --seed <addr>       register this node with an existing cluster\n\
       status  --seed <addr>       print the cluster routing snapshot\n\
       run     --config <path>     run a node\n"
}

fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first() else {
        eprint!("{}", usage());
        return ExitCode::FAILURE;
    };
    let rest = &args[1..];

    match command.as_str() {
        "init" => match flag(rest, "--config") {
            Some(path) => {
                // The bootstrap driver (dal::meta::bootstrap) runs the resumable
                // protocol against the node runtime; wiring the runtime to real
                // sockets is the remaining M5/M6 integration.
                println!("init: would bootstrap cluster from {path}");
                ExitCode::SUCCESS
            }
            None => {
                eprintln!("init: --config <path> required");
                ExitCode::FAILURE
            }
        },
        "join" => match flag(rest, "--seed") {
            Some(seed) => {
                println!("join: would register with seed {seed}");
                ExitCode::SUCCESS
            }
            None => {
                eprintln!("join: --seed <addr> required");
                ExitCode::FAILURE
            }
        },
        "status" => match flag(rest, "--seed") {
            Some(seed) => {
                println!("status: would query routing from seed {seed}");
                ExitCode::SUCCESS
            }
            None => {
                eprintln!("status: --seed <addr> required");
                ExitCode::FAILURE
            }
        },
        "run" => {
            println!("dal-service {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        other => {
            eprintln!("unknown command: {other}\n");
            eprint!("{}", usage());
            ExitCode::FAILURE
        }
    }
}
