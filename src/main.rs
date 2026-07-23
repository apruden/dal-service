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
       init        --config <path>       create a new cluster from a bootstrap descriptor\n\
       join        --seed <addr>         register this node with an existing cluster\n\
       leave       --node <id>           gracefully decommission a node (drain)\n\
       abort-plan  --group <g> --plan <id>  mark a stuck move plan aborting\n\
       status      --seed <addr>         print the cluster routing snapshot\n\
       run         --config <path>       run a node\n"
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
            Some(path) => unavailable("init", path),
            None => {
                eprintln!("init: --config <path> required");
                ExitCode::FAILURE
            }
        },
        "join" => match flag(rest, "--seed") {
            Some(seed) => unavailable("join", seed),
            None => {
                eprintln!("join: --seed <addr> required");
                ExitCode::FAILURE
            }
        },
        "leave" => match flag(rest, "--node") {
            Some(node) => unavailable("leave", node),
            None => {
                eprintln!("leave: --node <id> required");
                ExitCode::FAILURE
            }
        },
        "abort-plan" => match (flag(rest, "--group"), flag(rest, "--plan")) {
            (Some(group), Some(plan)) => {
                unavailable("abort-plan", &format!("group={group} plan={plan}"))
            }
            _ => {
                eprintln!("abort-plan: --group <g> and --plan <id> required");
                ExitCode::FAILURE
            }
        },
        "status" => match flag(rest, "--seed") {
            Some(seed) => unavailable("status", seed),
            None => {
                eprintln!("status: --seed <addr> required");
                ExitCode::FAILURE
            }
        },
        "run" => {
            eprintln!(
                "run is unavailable: this build does not yet wire the library runtime to a process"
            );
            ExitCode::FAILURE
        }
        other => {
            eprintln!("unknown command: {other}\n");
            eprint!("{}", usage());
            ExitCode::FAILURE
        }
    }
}

fn unavailable(command: &str, detail: &str) -> ExitCode {
    eprintln!(
        "{command} is unavailable for {detail}: this binary does not yet wire the library runtime to a process"
    );
    ExitCode::FAILURE
}
