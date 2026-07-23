//! `dal` binary entry point.
//!
//! Subcommands: `init` (create a cluster), `join` (register a node), `status`
//! (render the routing snapshot), plus `run`. The heavy lifting lives in the
//! library (`dal::meta::bootstrap`, `dal::api`); this is thin argument plumbing.

use std::path::Path;
use std::process::ExitCode;

use dal::runtime::config_file::{load_init_descriptor, load_node_config};
use dal::runtime::node::Node;

fn usage() -> &'static str {
    "usage: dal <command> [args]\n\
     \n\
     commands:\n\
       init        --config <path>       create a new cluster from a bootstrap descriptor\n\
       join        --seed <addr>         register this node with an existing cluster\n\
       leave       --node <id>           gracefully decommission a node (drain)\n\
       abort-plan  --group <g> --plan <id>  mark a stuck move plan aborting\n\
       status      --seed <addr>         print the cluster routing snapshot\n\
       run         --config <path> --cluster <path>   run a node\n"
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
        "run" => match (flag(rest, "--config"), flag(rest, "--cluster")) {
            (Some(config), Some(cluster)) => run_node(config, cluster),
            _ => {
                eprintln!("run: --config <node.json> and --cluster <init.json> required");
                ExitCode::FAILURE
            }
        },
        other => {
            eprintln!("unknown command: {other}\n");
            eprint!("{}", usage());
            ExitCode::FAILURE
        }
    }
}

/// Load config + genesis descriptor, start the node, drive genesis, then serve
/// until SIGINT and shut down gracefully.
fn run_node(config: &str, cluster: &str) -> ExitCode {
    let node_cfg = match load_node_config(Path::new(config)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("run: config: {e}");
            return ExitCode::FAILURE;
        }
    };
    let desc = match load_init_descriptor(Path::new(cluster)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("run: cluster: {e}");
            return ExitCode::FAILURE;
        }
    };
    if node_cfg.cluster_id != desc.cluster_id {
        eprintln!("run: node config and cluster descriptor disagree on cluster_id");
        return ExitCode::FAILURE;
    }

    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("run: runtime: {e}");
            return ExitCode::FAILURE;
        }
    };

    rt.block_on(async move {
        let ctx = zmq::Context::new();
        let node = match Node::start(ctx, node_cfg, desc).await {
            Ok(n) => n,
            Err(e) => {
                eprintln!("run: start: {e}");
                return ExitCode::FAILURE;
            }
        };
        if let Err(e) = node.bootstrap().await {
            eprintln!("run: bootstrap: {e}");
            return ExitCode::FAILURE;
        }
        eprintln!("dal node {} running; Ctrl-C to stop", node.node_id());
        let _ = tokio::signal::ctrl_c().await;
        eprintln!("dal node {} shutting down", node.node_id());
        if let Err(e) = node.shutdown().await {
            eprintln!("run: shutdown: {e}");
            return ExitCode::FAILURE;
        }
        ExitCode::SUCCESS
    })
}

fn unavailable(command: &str, detail: &str) -> ExitCode {
    eprintln!(
        "{command} is unavailable for {detail}: this binary does not yet wire the library runtime to a process"
    );
    ExitCode::FAILURE
}
