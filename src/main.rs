//! `dal` binary entry point.
//!
//! Subcommands: `run` (start a node; it drives resumable genesis from the
//! cluster descriptor) plus the operator admin clients `join`, `leave`,
//! `abort-plan`, and `status`. There is no separate `init` — genesis happens
//! inside `run`. The heavy lifting lives in the library (`dal::runtime`);
//! this is thin argument plumbing.

use std::future::Future;
use std::path::Path;
use std::process::ExitCode;

use dal::api::ops::RoutingInfo;
use dal::meta::state_machine::MetaApplyResult;
use dal::runtime::admin;
use dal::runtime::config_file::{load_init_descriptor, load_node_config};
use dal::runtime::node::Node;
use dal::search::SearchIndexDefinition;
use dal::transport::raft_wire::SearchIndexAdminBody;
use dal::types::GroupId;

fn usage() -> &'static str {
    "usage: dal <command> [args]\n\
     \n\
     commands:\n\
       run         --config <node.json> --cluster <init.json>   run a node (drives genesis)\n\
       join        --config <node.json>                         register this node with its cluster\n\
       leave       --cluster <init.json> --node <id> [--seed <addr>]   gracefully drain a node\n\
       abort-plan  --cluster <init.json> --group <meta|N> --plan <id> [--seed <addr>]   abort a stuck plan\n\
       index-create  --cluster <init.json> --name <name> --definition <json> [--seed <addr>]\n\
       index-rebuild --cluster <init.json> --name <name> --definition <json> [--seed <addr>]\n\
       index-drop    --cluster <init.json> --name <name> [--seed <addr>]\n\
       status      --cluster <init.json> [--seed <addr>]        print the cluster routing snapshot\n"
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
        "run" => match (flag(rest, "--config"), flag(rest, "--cluster")) {
            (Some(config), Some(cluster)) => run_node(config, cluster),
            _ => {
                eprintln!("run: --config <node.json> and --cluster <init.json> required");
                ExitCode::FAILURE
            }
        },
        "join" => match flag(rest, "--config") {
            Some(config) => join(config),
            None => {
                eprintln!("join: --config <node.json> required");
                ExitCode::FAILURE
            }
        },
        "leave" => match (flag(rest, "--cluster"), flag(rest, "--node")) {
            (Some(cluster), Some(node)) => leave(cluster, node, flag(rest, "--seed")),
            _ => {
                eprintln!("leave: --cluster <init.json> and --node <id> required");
                ExitCode::FAILURE
            }
        },
        "abort-plan" => match (
            flag(rest, "--cluster"),
            flag(rest, "--group"),
            flag(rest, "--plan"),
        ) {
            (Some(cluster), Some(group), Some(plan)) => {
                abort_plan(cluster, group, plan, flag(rest, "--seed"))
            }
            _ => {
                eprintln!(
                    "abort-plan: --cluster <init.json>, --group <meta|N>, and --plan <id> required"
                );
                ExitCode::FAILURE
            }
        },
        "index-create" | "index-rebuild" => match (
            flag(rest, "--cluster"),
            flag(rest, "--name"),
            flag(rest, "--definition"),
        ) {
            (Some(cluster), Some(name), Some(definition)) => search_index_definition_command(
                command,
                cluster,
                name,
                definition,
                flag(rest, "--seed"),
            ),
            _ => {
                eprintln!(
                    "{command}: --cluster <init.json>, --name <name>, and --definition <json> required"
                );
                ExitCode::FAILURE
            }
        },
        "index-drop" => match (flag(rest, "--cluster"), flag(rest, "--name")) {
            (Some(cluster), Some(name)) => search_index_drop(cluster, name, flag(rest, "--seed")),
            _ => {
                eprintln!("index-drop: --cluster <init.json> and --name <name> required");
                ExitCode::FAILURE
            }
        },
        "status" => match flag(rest, "--cluster") {
            Some(cluster) => status(cluster, flag(rest, "--seed")),
            None => {
                eprintln!("status: --cluster <init.json> required");
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

    with_runtime(async move {
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

fn join(config: &str) -> ExitCode {
    let cfg = match load_node_config(Path::new(config)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("join: config: {e}");
            return ExitCode::FAILURE;
        }
    };
    with_runtime(async move { report("join", admin::join(zmq::Context::new(), &cfg).await) })
}

fn leave(cluster: &str, node: &str, seed: Option<&str>) -> ExitCode {
    let desc = match load_init_descriptor(Path::new(cluster)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("leave: cluster: {e}");
            return ExitCode::FAILURE;
        }
    };
    let Ok(node_id) = node.parse::<u64>() else {
        eprintln!("leave: --node must be a node id (u64)");
        return ExitCode::FAILURE;
    };
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        report(
            "leave",
            admin::leave(zmq::Context::new(), &desc, node_id, seed.as_deref()).await,
        )
    })
}

fn abort_plan(cluster: &str, group: &str, plan: &str, seed: Option<&str>) -> ExitCode {
    let desc = match load_init_descriptor(Path::new(cluster)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("abort-plan: cluster: {e}");
            return ExitCode::FAILURE;
        }
    };
    let group = match parse_group(group) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("abort-plan: {e}");
            return ExitCode::FAILURE;
        }
    };
    let Ok(plan_id) = plan.parse::<u64>() else {
        eprintln!("abort-plan: --plan must be a plan id (u64)");
        return ExitCode::FAILURE;
    };
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        report(
            "abort-plan",
            admin::abort_plan(zmq::Context::new(), &desc, group, plan_id, seed.as_deref()).await,
        )
    })
}

fn search_index_definition_command(
    command: &str,
    cluster: &str,
    name: &str,
    definition_path: &str,
    seed: Option<&str>,
) -> ExitCode {
    let desc = match load_init_descriptor(Path::new(cluster)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{command}: cluster: {e}");
            return ExitCode::FAILURE;
        }
    };
    let definition = match std::fs::read_to_string(definition_path)
        .map_err(|e| e.to_string())
        .and_then(|json| {
            serde_json::from_str::<SearchIndexDefinition>(&json).map_err(|e| e.to_string())
        }) {
        Ok(definition) => definition,
        Err(e) => {
            eprintln!("{command}: definition: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = definition.validate() {
        eprintln!("{command}: definition: {e}");
        return ExitCode::FAILURE;
    }
    let body = if command == "index-create" {
        SearchIndexAdminBody::Create {
            name: name.to_string(),
            definition,
        }
    } else {
        SearchIndexAdminBody::Rebuild {
            name: name.to_string(),
            definition,
        }
    };
    let label = command.to_string();
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        report(
            &label,
            admin::search_index_admin(zmq::Context::new(), &desc, body, seed.as_deref()).await,
        )
    })
}

fn search_index_drop(cluster: &str, name: &str, seed: Option<&str>) -> ExitCode {
    let desc = match load_init_descriptor(Path::new(cluster)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("index-drop: cluster: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = dal::search::validate_index_name(name) {
        eprintln!("index-drop: {e}");
        return ExitCode::FAILURE;
    }
    let body = SearchIndexAdminBody::Drop {
        name: name.to_string(),
    };
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        report(
            "index-drop",
            admin::search_index_admin(zmq::Context::new(), &desc, body, seed.as_deref()).await,
        )
    })
}

fn status(cluster: &str, seed: Option<&str>) -> ExitCode {
    let desc = match load_init_descriptor(Path::new(cluster)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("status: cluster: {e}");
            return ExitCode::FAILURE;
        }
    };
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        match admin::status(zmq::Context::new(), &desc, seed.as_deref()).await {
            Ok(info) => {
                print_routing(&info);
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("status: {e}");
                ExitCode::FAILURE
            }
        }
    })
}

/// A partition group is `meta` or a partition number.
fn parse_group(s: &str) -> Result<GroupId, String> {
    if s.eq_ignore_ascii_case("meta") {
        return Ok(GroupId::Meta);
    }
    s.parse::<u16>()
        .map(GroupId::Data)
        .map_err(|e| format!("--group must be 'meta' or a partition number: {e}"))
}

/// Render a meta apply outcome and map it to a process exit code.
fn report(command: &str, result: dal::error::Result<MetaApplyResult>) -> ExitCode {
    match result {
        Ok(MetaApplyResult::Applied) => {
            println!("{command}: applied");
            ExitCode::SUCCESS
        }
        Ok(MetaApplyResult::NoOp) => {
            println!("{command}: no change (already applied)");
            ExitCode::SUCCESS
        }
        Ok(MetaApplyResult::Rejected(why)) => {
            eprintln!("{command}: rejected: {why:?}");
            ExitCode::FAILURE
        }
        Err(e) => {
            eprintln!("{command}: {e}");
            ExitCode::FAILURE
        }
    }
}

fn print_routing(info: &RoutingInfo) {
    println!("cluster_id: {:#x}", info.cluster_id);
    println!("partitions (p): {}", info.p);
    println!("nodes:");
    for e in &info.directory {
        println!(
            "  {:>4}  {:<28}  {:?}  incarnation={}",
            e.node_id, e.control_addr, e.state, e.incarnation
        );
    }
    println!("placements:");
    let mut placements = info.placements.clone();
    placements.sort_by_key(|(p, _)| *p);
    for (p, placement) in &placements {
        let mv = match &placement.r#move {
            Some(m) => format!("  (move plan {} -> {:?})", m.plan_id, m.target_voters),
            None => String::new(),
        };
        println!("  partition {p:>4}: voters {:?}{mv}", placement.voters);
    }
}

/// Build a multi-thread runtime and drive `fut` to completion. Every subcommand
/// is a short-lived async client, so one runtime per invocation is fine.
fn with_runtime<F: Future<Output = ExitCode>>(fut: F) -> ExitCode {
    match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt.block_on(fut),
        Err(e) => {
            eprintln!("runtime: {e}");
            ExitCode::FAILURE
        }
    }
}
