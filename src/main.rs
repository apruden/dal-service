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
use std::time::Duration;

use dal::api::ops::RoutingInfo;
use dal::meta::state_machine::MetaApplyResult;
use dal::runtime::admin;
use dal::runtime::config_file::{
    load_init_descriptor, load_node_config, load_node_security, load_operator_security,
    validate_node_security,
};
use dal::runtime::node::Node;
use dal::search::SearchIndexDefinition;
use dal::transport::raft_wire::SearchIndexAdminBody;
use dal::transport::security::CurveClientConfig;
use dal::types::GroupId;

fn usage() -> &'static str {
    "usage: dal <command> [args]\n\
     \n\
     commands:\n\
       check-config --config <node.json> --cluster <init.json>   validate configuration without starting\n\
       run         --config <node.json> --cluster <init.json>   run a node (drives genesis)\n\
       join        --config <node.json> --auth <operator.json>   register this node with its cluster\n\
       leave       --auth <operator.json> --cluster <init.json> --node <id> [--seed <addr>]   gracefully drain a node\n\
       abort-plan  --auth <operator.json> --cluster <init.json> --group <meta|N> --plan <id> [--seed <addr>]   abort a stuck plan\n\
       index-create  --auth <operator.json> --cluster <init.json> --name <name> --definition <json> [--seed <addr>]\n\
       index-rebuild --auth <operator.json> --cluster <init.json> --name <name> --definition <json> [--seed <addr>]\n\
       index-drop    --auth <operator.json> --cluster <init.json> --name <name> [--seed <addr>]\n\
       status      --auth <operator.json> --cluster <init.json> [--seed <addr>]        print the cluster routing snapshot\n\
       maintenance-status --auth <operator.json> --cluster <init.json> [--seed <addr>]   print maintenance progress\n\
       maintenance-release --auth <operator.json> --cluster <init.json> --operation <hex> --generation <n> [--seed <addr>] [--wait-seconds <n>]   release verified maintenance\n"
}

fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .map(String::as_str)
}

fn main() -> ExitCode {
    let _tracing_guard = init_tracing();
    tracing::info!(version = env!("CARGO_PKG_VERSION"), "process started");
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(command) = args.first() else {
        eprint!("{}", usage());
        return ExitCode::FAILURE;
    };
    let rest = &args[1..];

    match command.as_str() {
        "check-config" => match (flag(rest, "--config"), flag(rest, "--cluster")) {
            (Some(config), Some(cluster)) => check_config(config, cluster),
            _ => {
                eprintln!("check-config: --config <node.json> and --cluster <init.json> required");
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
        "join" => match (flag(rest, "--config"), flag(rest, "--auth")) {
            (Some(config), Some(auth)) => join(config, auth),
            _ => {
                eprintln!("join: --config <node.json> and --auth <operator.json> required");
                ExitCode::FAILURE
            }
        },
        "leave" => match (
            flag(rest, "--auth"),
            flag(rest, "--cluster"),
            flag(rest, "--node"),
        ) {
            (Some(auth), Some(cluster), Some(node)) => {
                leave(auth, cluster, node, flag(rest, "--seed"))
            }
            _ => {
                eprintln!(
                    "leave: --auth <operator.json>, --cluster <init.json>, and --node <id> required"
                );
                ExitCode::FAILURE
            }
        },
        "abort-plan" => match (
            flag(rest, "--auth"),
            flag(rest, "--cluster"),
            flag(rest, "--group"),
            flag(rest, "--plan"),
        ) {
            (Some(auth), Some(cluster), Some(group), Some(plan)) => {
                abort_plan(auth, cluster, group, plan, flag(rest, "--seed"))
            }
            _ => {
                eprintln!(
                    "abort-plan: --auth <operator.json>, --cluster <init.json>, --group <meta|N>, and --plan <id> required"
                );
                ExitCode::FAILURE
            }
        },
        "index-create" | "index-rebuild" => match (
            flag(rest, "--auth"),
            flag(rest, "--cluster"),
            flag(rest, "--name"),
            flag(rest, "--definition"),
        ) {
            (Some(auth), Some(cluster), Some(name), Some(definition)) => {
                search_index_definition_command(
                    command,
                    auth,
                    cluster,
                    name,
                    definition,
                    flag(rest, "--seed"),
                )
            }
            _ => {
                eprintln!(
                    "{command}: --auth <operator.json>, --cluster <init.json>, --name <name>, and --definition <json> required"
                );
                ExitCode::FAILURE
            }
        },
        "index-drop" => match (
            flag(rest, "--auth"),
            flag(rest, "--cluster"),
            flag(rest, "--name"),
        ) {
            (Some(auth), Some(cluster), Some(name)) => {
                search_index_drop(auth, cluster, name, flag(rest, "--seed"))
            }
            _ => {
                eprintln!(
                    "index-drop: --auth <operator.json>, --cluster <init.json>, and --name <name> required"
                );
                ExitCode::FAILURE
            }
        },
        "status" => match (flag(rest, "--auth"), flag(rest, "--cluster")) {
            (Some(auth), Some(cluster)) => status(auth, cluster, flag(rest, "--seed")),
            _ => {
                eprintln!("status: --auth <operator.json> and --cluster <init.json> required");
                ExitCode::FAILURE
            }
        },
        "maintenance-status" => match (flag(rest, "--auth"), flag(rest, "--cluster")) {
            (Some(auth), Some(cluster)) => maintenance_status(auth, cluster, flag(rest, "--seed")),
            _ => {
                eprintln!(
                    "maintenance-status: --auth <operator.json> and --cluster <init.json> required"
                );
                ExitCode::FAILURE
            }
        },
        "maintenance-release" => match (
            flag(rest, "--auth"),
            flag(rest, "--cluster"),
            flag(rest, "--operation"),
            flag(rest, "--generation"),
        ) {
            (Some(auth), Some(cluster), Some(operation), Some(generation)) => maintenance_release(
                auth,
                cluster,
                operation,
                generation,
                flag(rest, "--seed"),
                flag(rest, "--wait-seconds"),
            ),
            _ => {
                eprintln!(
                    "maintenance-release: --auth, --cluster, --operation, and --generation required"
                );
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

/// Validate the node, cluster descriptor, credentials, trust policy, and pins
/// without opening storage, binding sockets, or advancing trust generations.
fn check_config(config: &str, cluster: &str) -> ExitCode {
    if zmq::has("curve") != Some(true) {
        eprintln!("check-config: this libzmq build does not support CURVE security");
        return ExitCode::FAILURE;
    }
    let cfg = match load_node_config(Path::new(config)) {
        Ok(cfg) => cfg,
        Err(error) => {
            eprintln!("check-config: config: {error}");
            return ExitCode::FAILURE;
        }
    };
    let descriptor = match load_init_descriptor(Path::new(cluster)) {
        Ok(descriptor) => descriptor,
        Err(error) => {
            eprintln!("check-config: cluster: {error}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(error) = dal::runtime::node::validate_node_descriptor(&cfg, &descriptor) {
        eprintln!("check-config: descriptor: {error}");
        return ExitCode::FAILURE;
    }
    if let Err(error) = validate_node_security(Path::new(config), &cfg) {
        eprintln!("check-config: security: {error}");
        return ExitCode::FAILURE;
    }
    if let Err(error) = dal::storage::rocks::check_storage_format(&cfg.data_dir) {
        eprintln!("check-config: storage: {error}");
        return ExitCode::FAILURE;
    }
    println!("configuration valid");
    ExitCode::SUCCESS
}

/// Install bounded, structured logs before parsing configuration so startup
/// failures are visible through the same production logging path.
fn init_tracing() -> Option<tracing_appender::non_blocking::WorkerGuard> {
    use tracing_subscriber::EnvFilter;

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let (writer, guard) = tracing_appender::non_blocking::NonBlockingBuilder::default()
        .buffered_lines_limit(1024)
        .lossy(true)
        .finish(std::io::stderr());
    let drop_counter = writer.error_counter();
    match tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .with_span_list(false)
        .with_writer(writer)
        .try_init()
    {
        Ok(()) => {
            dal::perf::install_log_drop_counter(drop_counter);
            Some(guard)
        }
        Err(error) => {
            eprintln!("failed to install structured logger: {error}");
            None
        }
    }
}

/// Load config + genesis descriptor, start the node, drive genesis, then serve
/// until SIGINT and shut down gracefully.
fn run_node(config: &str, cluster: &str) -> ExitCode {
    let node_cfg = match load_node_config(Path::new(config)) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(%e, "node config load failed");
            eprintln!("run: config: {e}");
            return ExitCode::FAILURE;
        }
    };
    let desc = match load_init_descriptor(Path::new(cluster)) {
        Ok(d) => d,
        Err(e) => {
            tracing::error!(%e, "cluster descriptor load failed");
            eprintln!("run: cluster: {e}");
            return ExitCode::FAILURE;
        }
    };
    let security = match load_node_security(Path::new(config), &node_cfg) {
        Ok(security) => security,
        Err(e) => {
            tracing::error!(%e, "node security load failed");
            eprintln!("run: security: {e}");
            return ExitCode::FAILURE;
        }
    };
    if node_cfg.cluster_id != desc.cluster_id {
        tracing::error!("node config and cluster descriptor disagree on cluster_id");
        eprintln!("run: node config and cluster descriptor disagree on cluster_id");
        return ExitCode::FAILURE;
    }

    with_runtime(async move {
        let mut signals = match ShutdownSignals::register() {
            Ok(signals) => signals,
            Err(error) => {
                tracing::error!(%error, "signal registration failed");
                eprintln!("run: signal registration: {error}");
                return ExitCode::FAILURE;
            }
        };
        let shutdown_timeout = node_cfg.timeouts.shutdown;
        let ctx = zmq::Context::new();
        let mut start = Box::pin(Node::start_secure(ctx, node_cfg, desc, security));
        let node = tokio::select! {
            result = &mut start => match result {
                Ok(node) => node,
                Err(error) => {
                    tracing::error!(%error, "node startup failed");
                    eprintln!("run: start: {error}");
                    return ExitCode::FAILURE;
                }
            },
            signal = signals.recv() => {
                tracing::info!(signal, "shutdown requested during startup");
                eprintln!("dal received {signal} during startup; stopping");
                return match tokio::time::timeout(shutdown_timeout, &mut start).await {
                    Ok(Ok(node)) => shutdown_node(node).await,
                    Ok(Err(error)) => {
                        tracing::error!(%error, "node startup failed while shutting down");
                        eprintln!("run: start while stopping: {error}");
                        ExitCode::FAILURE
                    }
                    Err(_) => {
                        tracing::error!(timeout_ms = shutdown_timeout.as_millis(), "forced stop after startup timeout");
                        eprintln!(
                            "run: forced stop after startup exceeded {} ms",
                            shutdown_timeout.as_millis()
                        );
                        ExitCode::FAILURE
                    }
                };
            }
        };

        enum BootstrapEvent {
            Complete(dal::error::Result<()>),
            Signal(&'static str),
            Fatal,
        }
        let event = {
            let bootstrap = node.bootstrap();
            tokio::pin!(bootstrap);
            tokio::select! {
                result = &mut bootstrap => BootstrapEvent::Complete(result),
                signal = signals.recv() => BootstrapEvent::Signal(signal),
                () = node.wait_for_failure() => BootstrapEvent::Fatal,
            }
        };
        match event {
            BootstrapEvent::Complete(Ok(())) => {}
            BootstrapEvent::Complete(Err(error)) => {
                tracing::error!(node_id = node.node_id(), %error, "node bootstrap failed");
                eprintln!("run: bootstrap: {error}");
                node.mark_failed();
                let _ = shutdown_node(node).await;
                return ExitCode::FAILURE;
            }
            BootstrapEvent::Signal(signal) => {
                tracing::info!(
                    node_id = node.node_id(),
                    signal,
                    "shutdown requested during bootstrap"
                );
                eprintln!(
                    "dal node {} received {signal} during bootstrap",
                    node.node_id()
                );
                return shutdown_node(node).await;
            }
            BootstrapEvent::Fatal => {
                tracing::error!(
                    node_id = node.node_id(),
                    "fatal background failure during bootstrap"
                );
                eprintln!("dal node {} failed during bootstrap", node.node_id());
                let _ = shutdown_node(node).await;
                return ExitCode::FAILURE;
            }
        }

        tracing::info!(node_id = node.node_id(), "node ready");
        eprintln!("dal node {} running; Ctrl-C to stop", node.node_id());
        let failed = tokio::select! {
            signal = signals.recv() => {
                tracing::info!(node_id = node.node_id(), signal, "shutdown requested");
                eprintln!("dal node {} received {signal}; shutting down", node.node_id());
                false
            }
            () = node.wait_for_failure() => {
                tracing::error!(node_id = node.node_id(), "fatal background failure; shutting down");
                eprintln!("dal node {} has a fatal background failure; shutting down", node.node_id());
                true
            }
        };
        let result = shutdown_node(node).await;
        if failed { ExitCode::FAILURE } else { result }
    })
}

struct ShutdownSignals {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn register() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.interrupt.recv() => "SIGINT",
            _ = self.terminate.recv() => "SIGTERM",
        }
    }
}

async fn shutdown_node(node: Node) -> ExitCode {
    let node_id = node.node_id();
    match node.shutdown().await {
        Ok(()) => {
            tracing::info!(node_id, "node shutdown complete");
            ExitCode::SUCCESS
        }
        Err(error) => {
            tracing::error!(node_id, %error, "node shutdown failed");
            eprintln!("run: shutdown: {error}");
            ExitCode::FAILURE
        }
    }
}

fn join(config: &str, auth: &str) -> ExitCode {
    let cfg = match load_node_config(Path::new(config)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("join: config: {e}");
            return ExitCode::FAILURE;
        }
    };
    let candidate = match load_node_security(Path::new(config), &cfg) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("join: node security: {e}");
            return ExitCode::FAILURE;
        }
    };
    let security = match load_operator_security(Path::new(auth), cfg.cluster_id) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("join: security: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = security.validate_joining_node(&candidate, cfg.node_id) {
        eprintln!("join: enrollment: {e}");
        return ExitCode::FAILURE;
    }
    if cfg
        .seeds
        .iter()
        .any(|seed| !security.server_keys.contains_key(seed))
    {
        eprintln!("join: operator auth file is missing a pin for a configured seed");
        return ExitCode::FAILURE;
    }
    let curve = match curve_client_config(security) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("join: security: {e}");
            return ExitCode::FAILURE;
        }
    };
    let policy_generation = candidate.policy.generation();
    let candidate_public_key = candidate.public_key;
    with_runtime(async move {
        report(
            "join",
            admin::join_secure(
                zmq::Context::new(),
                &cfg,
                curve,
                policy_generation,
                candidate_public_key,
            )
            .await,
        )
    })
}

fn leave(auth: &str, cluster: &str, node: &str, seed: Option<&str>) -> ExitCode {
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
    let curve = match load_cli_curve(auth, &desc) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("leave: security: {e}");
            return ExitCode::FAILURE;
        }
    };
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        report(
            "leave",
            admin::leave_secure(zmq::Context::new(), &desc, node_id, seed.as_deref(), curve).await,
        )
    })
}

fn abort_plan(auth: &str, cluster: &str, group: &str, plan: &str, seed: Option<&str>) -> ExitCode {
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
    let curve = match load_cli_curve(auth, &desc) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("abort-plan: security: {e}");
            return ExitCode::FAILURE;
        }
    };
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        report(
            "abort-plan",
            admin::abort_plan_secure(
                zmq::Context::new(),
                &desc,
                group,
                plan_id,
                seed.as_deref(),
                curve,
            )
            .await,
        )
    })
}

fn search_index_definition_command(
    command: &str,
    auth: &str,
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
    let curve = match load_cli_curve(auth, &desc) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("{command}: security: {e}");
            return ExitCode::FAILURE;
        }
    };
    let label = command.to_string();
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        report(
            &label,
            admin::search_index_admin_secure(
                zmq::Context::new(),
                &desc,
                body,
                seed.as_deref(),
                curve,
            )
            .await,
        )
    })
}

fn search_index_drop(auth: &str, cluster: &str, name: &str, seed: Option<&str>) -> ExitCode {
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
    let curve = match load_cli_curve(auth, &desc) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("index-drop: security: {e}");
            return ExitCode::FAILURE;
        }
    };
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        report(
            "index-drop",
            admin::search_index_admin_secure(
                zmq::Context::new(),
                &desc,
                body,
                seed.as_deref(),
                curve,
            )
            .await,
        )
    })
}

fn status(auth: &str, cluster: &str, seed: Option<&str>) -> ExitCode {
    let desc = match load_init_descriptor(Path::new(cluster)) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("status: cluster: {e}");
            return ExitCode::FAILURE;
        }
    };
    let curve = match load_cli_curve(auth, &desc) {
        Ok(value) => value,
        Err(e) => {
            eprintln!("status: security: {e}");
            return ExitCode::FAILURE;
        }
    };
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        match admin::status_secure(zmq::Context::new(), &desc, seed.as_deref(), curve).await {
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

fn maintenance_status(auth: &str, cluster: &str, seed: Option<&str>) -> ExitCode {
    let desc = match load_init_descriptor(Path::new(cluster)) {
        Ok(desc) => desc,
        Err(error) => {
            eprintln!("maintenance-status: cluster: {error}");
            return ExitCode::FAILURE;
        }
    };
    let curve = match load_cli_curve(auth, &desc) {
        Ok(curve) => curve,
        Err(error) => {
            eprintln!("maintenance-status: security: {error}");
            return ExitCode::FAILURE;
        }
    };
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        match admin::maintenance_status_secure(zmq::Context::new(), &desc, seed.as_deref(), curve)
            .await
        {
            Ok(Some(record)) => {
                println!("operation: {:#x}", record.operation_id);
                println!("generation: {}", record.generation);
                println!("stage: {:?}", record.stage);
                println!("participants: {:?}", record.participants);
                println!("preparation acknowledged: {:?}", record.acknowledged);
                println!("recovery acknowledged: {:?}", record.recovered);
                let pending: Vec<_> = match record.stage {
                    dal::types::MaintenanceStage::Preparing => record
                        .participants
                        .iter()
                        .copied()
                        .filter(|node| record.acknowledged.binary_search(node).is_err())
                        .collect(),
                    dal::types::MaintenanceStage::Verifying => record
                        .participants
                        .iter()
                        .copied()
                        .filter(|node| record.recovered.binary_search(node).is_err())
                        .collect(),
                    _ => Vec::new(),
                };
                if !pending.is_empty() {
                    println!(
                        "participants awaiting {:?} acknowledgement: {:?}",
                        record.stage, pending
                    );
                }
                println!("recovery processes: {:?}", record.recovery_processes);
                for (node, digest) in &record.recovery_policy_digests {
                    let digest: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
                    println!("participant {node} trust policy: {digest}");
                }
                println!(
                    "verification trust policy: {}",
                    record
                        .verification_policy_digest
                        .map(|digest| digest
                            .iter()
                            .map(|byte| format!("{byte:02x}"))
                            .collect::<String>())
                        .unwrap_or_else(|| "pending".into())
                );
                if record.stage == dal::types::MaintenanceStage::Verifying {
                    if !pending.is_empty() {
                        println!(
                            "recovery action: restart missing participants with their durable state and recorded identity; if state is lost, keep maintenance fenced"
                        );
                    } else if record.verification_policy_digest.is_none() {
                        println!(
                            "recovery action: align participant trust policies and restart affected nodes; maintenance remains fenced"
                        );
                    } else {
                        println!(
                            "recovery proofs complete; maintenance-release checks the leader policy and fresh heartbeats before release"
                        );
                    }
                }
                ExitCode::SUCCESS
            }
            Ok(None) => {
                println!("no maintenance operation");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("maintenance-status: {error}");
                ExitCode::FAILURE
            }
        }
    })
}

fn maintenance_release(
    auth: &str,
    cluster: &str,
    operation: &str,
    generation: &str,
    seed: Option<&str>,
    wait_seconds: Option<&str>,
) -> ExitCode {
    let Some(operation_id) = operation
        .strip_prefix("0x")
        .and_then(|hex| u128::from_str_radix(hex, 16).ok())
        .filter(|value| *value != 0)
    else {
        eprintln!(
            "maintenance-release: --operation must be a nonzero 0x-prefixed hex ID from maintenance-status"
        );
        return ExitCode::FAILURE;
    };
    let Ok(generation) = generation.parse::<u64>() else {
        eprintln!("maintenance-release: --generation must be a positive integer");
        return ExitCode::FAILURE;
    };
    if generation == 0 {
        eprintln!("maintenance-release: --generation must be a positive integer");
        return ExitCode::FAILURE;
    }
    let wait_seconds = match wait_seconds.unwrap_or("60").parse::<u64>() {
        Ok(seconds) if (1..=3600).contains(&seconds) => seconds,
        _ => {
            eprintln!("maintenance-release: --wait-seconds must be between 1 and 3600");
            return ExitCode::FAILURE;
        }
    };
    let desc = match load_init_descriptor(Path::new(cluster)) {
        Ok(desc) => desc,
        Err(error) => {
            eprintln!("maintenance-release: cluster: {error}");
            return ExitCode::FAILURE;
        }
    };
    let curve = match load_cli_curve(auth, &desc) {
        Ok(curve) => curve,
        Err(error) => {
            eprintln!("maintenance-release: security: {error}");
            return ExitCode::FAILURE;
        }
    };
    let seed = seed.map(str::to_string);
    with_runtime(async move {
        match admin::maintenance_release_secure(
            zmq::Context::new(),
            &desc,
            operation_id,
            generation,
            seed.as_deref(),
            curve,
            Duration::from_secs(wait_seconds),
        )
        .await
        {
            Ok(outcome) => report("maintenance-release", Ok(outcome)),
            Err(error) => {
                let might_have_sent = !matches!(&error, dal::error::Error::Config(_));
                let code = report("maintenance-release", Err(error));
                if might_have_sent {
                    eprintln!(
                        "If the request may have reached the leader, check maintenance-status before retrying."
                    );
                }
                code
            }
        }
    })
}

fn load_cli_curve(
    auth: &str,
    desc: &dal::meta::bootstrap::BootstrapDescriptor,
) -> Result<CurveClientConfig, String> {
    let security = load_operator_security(Path::new(auth), desc.cluster_id)
        .map_err(|error| error.to_string())?;
    security
        .validate_descriptor(desc)
        .map_err(|error| error.to_string())?;
    curve_client_config(security).map_err(|error| error.to_string())
}

fn curve_client_config(
    security: dal::runtime::config_file::LoadedOperatorSecurity,
) -> dal::error::Result<CurveClientConfig> {
    CurveClientConfig::new(
        security.public_key,
        security.secret_key,
        security.server_keys,
    )
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
        Ok(rt) => {
            let result = rt.block_on(fut);
            // A timed-out graceful stop may leave non-cancellable blocking work
            // in Tokio's pool. Bound runtime destruction so the executable can
            // still exit through its explicit forced-stop path.
            rt.shutdown_timeout(Duration::from_secs(5));
            result
        }
        Err(e) => {
            eprintln!("runtime: {e}");
            ExitCode::FAILURE
        }
    }
}
