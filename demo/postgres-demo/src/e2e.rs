use std::{
    collections::BTreeMap,
    env,
    error::Error,
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Child, ChildStdout, Command, Stdio},
    time::Duration,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "web")]
use std::{
    net::{SocketAddr, TcpStream},
    thread,
};

use kneefinder::{
    adapter_session::SessionOptions,
    agent::{AgentCohort, AgentPlacement},
    config::{
        AdapterCommand, AgentEndpointConfig, AgentTransportConfig, LoadConfig, OperationSelection,
        PhaseConfig, Preset, RunConfig, Strategy, WeightedOperation, WorkloadConfig,
    },
    engine::{Engine, EngineCommand, EngineEvent},
    measurement::{MeasurementStage, RunClassification, RunState},
    protocol::{ArgumentValue, OperationId, PhaseId, RunId, ScheduledOperation},
    stats::{OperationVariant, StatsReport, summarize_results},
    strategy::{StrategyAction, StrategyDecision},
};

#[cfg(feature = "web")]
use kneefinder::frontends::{Frontend, web::WebFrontend};

const CONNECTIONS: usize = 4;
const LOCK_HOLD_MS: u64 = 10;

fn theoretical_knee() -> f64 {
    1_000.0 / LOCK_HOLD_MS as f64 / 0.16
}

pub fn run() -> Result<(), Box<dyn Error>> {
    let executable = env::current_exe()?;
    let endpoints = vec![AgentEndpointConfig {
        id: "colocated-0".into(),
        transport: AgentTransportConfig::Subprocess {
            command: AdapterCommand {
                program: executable.to_string_lossy().into_owned(),
                arguments: vec!["adapter".into()],
            },
        },
    }];

    let expected_knee = theoretical_knee();
    eprintln!("e2e topology: coordinator -> colocated agent -> PostgreSQL adapter -> PostgreSQL");
    eprintln!(
        "target: {CONNECTIONS} client connections; 80/20 lookups/transfers; hot-row lock held {LOCK_HOLD_MS} ms; expected knee near {expected_knee:.0} req/s\n"
    );

    let rates = [150.0, 300.0, 450.0, 600.0, 750.0, 1_000.0, 1_400.0];
    let engine = Engine::new();
    let handle = engine.handle();
    let events = handle.subscribe();
    let configured = handle.execute(EngineCommand::Configure {
        config: Box::new(demo_config(endpoints, &rates)),
    })?;
    let prepared = handle.execute(EngineCommand::PrepareAgents {
        run_id: configured.run_id,
    })?;
    let kneefinder::engine::AgentPreparation::Ready { catalog } = prepared.preparation else {
        return Err("engine did not retain the prepared colocated agent".into());
    };
    if catalog
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .eq(["lookup", "transfer"])
    {
        eprintln!("adapter advertised operations: lookup=4, transfer=1");
    } else {
        return Err(format!("unexpected advertised operations: {:?}", catalog.operations).into());
    }
    handle.execute(EngineCommand::Start {
        run_id: configured.run_id,
    })?;

    let mut rows = Vec::new();
    let outcome = loop {
        match events.recv()? {
            EngineEvent::PhaseStats { run_id, report, .. } if run_id == configured.run_id => {
                let row = DemoRow::from_report(report);
                eprintln!(
                    "measured {:.0} req/s: p95 {:.1}ms",
                    row.offered_per_second, row.p95_ms
                );
                rows.push(row);
            }
            EngineEvent::RunStateChanged { snapshot, .. }
                if snapshot.run_id == configured.run_id && snapshot.state.is_terminal() =>
            {
                match snapshot.state {
                    RunState::Completed { outcome } => break outcome,
                    state => return Err(format!("generic colocated run ended in {state:?}").into()),
                }
            }
            _ => {}
        }
    };

    if rows.len() != rates.len() {
        return Err(format!(
            "expected {} measured phases, got {}",
            rates.len(),
            rows.len()
        )
        .into());
    }
    let knee = outcome
        .knee
        .as_ref()
        .ok_or_else(|| format!("fixed sweep did not produce a knee: {outcome:?}"))?;
    if outcome.classification != RunClassification::TargetSaturated
        || !(450.0..=850.0).contains(&knee.offered_rate)
        || outcome.analysis.is_none()
    {
        return Err(format!("unexpected fixed-sweep knee result: {outcome:?}").into());
    }
    eprintln!(
        "fitted knee {:.1} req/s ({:.1}–{:.1}), recommended {:.1}\n",
        knee.offered_rate, knee.lower_bound, knee.upper_bound, knee.recommended_operating_rate
    );
    print_rows(&rows, expected_knee);
    print_variant_stats(&rows);
    Ok(())
}

pub fn run_adaptive() -> Result<(), Box<dyn Error>> {
    let executable = env::current_exe()?;
    let endpoints = vec![AgentEndpointConfig {
        id: "colocated-0".into(),
        transport: AgentTransportConfig::Subprocess {
            command: AdapterCommand {
                program: executable.to_string_lossy().into_owned(),
                arguments: vec!["adapter".into()],
            },
        },
    }];
    let mut config = demo_config(endpoints, &[150.0, 1_400.0]);
    config.strategy = Strategy::Adaptive;
    config.load.explicit_levels.clear();
    config.load.growth_factor = 2.0;
    config.phases.warmup_ms = 200;
    config.phases.measurement_ms = 1_000;
    config.phases.recovery_ms = 1_000;
    config.phases.repetitions = 3;

    let engine = Engine::new();
    let handle = engine.handle();
    let events = handle.subscribe();
    let configured = handle.execute(EngineCommand::Configure {
        config: Box::new(config),
    })?;
    handle.execute(EngineCommand::PrepareAgents {
        run_id: configured.run_id,
    })?;
    handle.execute(EngineCommand::Start {
        run_id: configured.run_id,
    })?;

    let mut reports = 0;
    let mut decisions = Vec::<StrategyDecision>::new();
    let outcome = loop {
        match events.recv()? {
            EngineEvent::PhaseStats { run_id, report, .. } if run_id == configured.run_id => {
                reports += 1;
                eprintln!(
                    "adaptive measured {:.1} req/s: {:.1} goodput, stationary={}",
                    report.offered_rate, report.goodput_rate, report.quality.stationary
                );
            }
            EngineEvent::StrategyDecision { run_id, decision } if run_id == configured.run_id => {
                decisions.push(decision);
            }
            EngineEvent::RunStateChanged { snapshot, .. }
                if snapshot.run_id == configured.run_id && snapshot.state.is_terminal() =>
            {
                match snapshot.state {
                    RunState::Completed { outcome } => break outcome,
                    state => return Err(format!("adaptive run ended in {state:?}").into()),
                }
            }
            _ => {}
        }
    };

    if outcome.classification != RunClassification::TargetSaturated {
        return Err(format!("adaptive run completed as {outcome:?}").into());
    }
    let knee = outcome
        .knee
        .as_ref()
        .ok_or("adaptive run did not produce a knee estimate")?;
    if !(450.0..=850.0).contains(&knee.offered_rate)
        || knee.lower_bound > knee.offered_rate
        || knee.upper_bound < knee.offered_rate
        || knee.recommended_operating_rate > knee.lower_bound
        || outcome.analysis.is_none()
    {
        return Err(format!("unexpected adaptive knee result: {outcome:?}").into());
    }
    if reports < 5
        || !decisions.iter().any(|decision| {
            decision.stage == MeasurementStage::Refinement
                && decision.action == StrategyAction::Select
        })
    {
        return Err(format!(
            "adaptive run did not exercise refinement: {reports} reports, decisions={decisions:?}"
        )
        .into());
    }
    println!(
        "adaptive E2E passed: {reports} phases, knee {:.1} req/s ({:.1}–{:.1}), recommended {:.1}",
        knee.offered_rate, knee.lower_bound, knee.upper_bound, knee.recommended_operating_rate
    );
    Ok(())
}

pub fn run_tcp_multi_client() -> Result<(), Box<dyn Error>> {
    let mut cohort = connect_tcp_cohort()?;
    cohort.initialize(RunId(2), 2)?;

    let offered_per_second = 150.0;
    let operations = (0..100)
        .map(|index| {
            let (operation, arguments) = variant_for_index(index);
            ScheduledOperation {
                id: OperationId(index),
                operation: operation.into(),
                start_offset_ns: (index as f64 * 1e9 / offered_per_second).round() as u64,
                arguments,
            }
        })
        .collect();
    let phase = cohort.agents.execute_schedule(
        PhaseId(1),
        unix_now_ns().saturating_add(150_000_000),
        operations,
        &std::sync::atomic::AtomicBool::new(false),
    )?;
    if phase.agents.len() != 2 {
        return Err(format!("expected two per-agent results, got {}", phase.agents.len()).into());
    }
    for result in &phase.agents {
        if result.operations.len() != 50 {
            return Err(format!(
                "agent {} executed {} operations instead of 50",
                result.agent.id,
                result.operations.len()
            )
            .into());
        }
        let agent_stats = summarize_results(&result.operations)?;
        let attempts = agent_stats
            .variants
            .iter()
            .map(|variant| variant.stats.attempts)
            .collect::<Vec<_>>();
        if attempts != [32, 8, 2, 8] {
            return Err(format!(
                "agent {} received an imbalanced variant mix: {attempts:?}",
                result.agent.id,
            )
            .into());
        }
    }
    let operations = phase.into_operations();
    let stats = summarize_results(&operations)?;
    if stats.overall.attempts != 100
        || stats.overall.successful != 100
        || stats.overall.failed != 0
        || stats.overall.timed_out != 0
    {
        return Err(format!(
            "unexpected aggregate multi-client stats: {:?}",
            stats.overall
        )
        .into());
    }

    cohort.shutdown()?;
    println!("multi-client TCP E2E passed: 2 clients, 50 operations each, 100 successful total");
    Ok(())
}

fn connect_tcp_cohort() -> Result<TcpDemoCohort, Box<dyn Error>> {
    let (processes, endpoints) = spawn_tcp_agents()?;
    let options = SessionOptions {
        connection_timeout: Duration::from_secs(2),
        handshake_timeout: Duration::from_secs(2),
        response_timeout: Duration::from_secs(5),
        shutdown_timeout: Duration::from_secs(2),
        ..SessionOptions::default()
    };
    let agents = AgentCohort::from_endpoints(&endpoints, options)?;
    Ok(TcpDemoCohort { agents, processes })
}

type SpawnedTcpAgents = (Vec<TcpAdapterProcess>, Vec<AgentEndpointConfig>);

fn spawn_tcp_agents() -> Result<SpawnedTcpAgents, Box<dyn Error>> {
    let executable = env::current_exe()?;
    let processes = vec![
        TcpAdapterProcess::spawn(&executable)?,
        TcpAdapterProcess::spawn(&executable)?,
    ];
    let endpoints = processes
        .iter()
        .enumerate()
        .map(|(index, process)| AgentEndpointConfig {
            id: format!("tcp-{index}"),
            transport: AgentTransportConfig::Tcp {
                address: process.endpoint().into(),
            },
        })
        .collect();
    Ok((processes, endpoints))
}

struct TcpDemoCohort {
    agents: AgentCohort,
    processes: Vec<TcpAdapterProcess>,
}

impl TcpDemoCohort {
    fn initialize(
        &mut self,
        run_id: RunId,
        connections_per_agent: usize,
    ) -> Result<(), Box<dyn Error>> {
        let ready = self.agents.initialize(
            run_id,
            serde_json::json!({
                "connections": connections_per_agent,
                "lock_hold_ms": LOCK_HOLD_MS
            }),
        )?;
        if ready.agents.len() != 2
            || ready
                .agents
                .iter()
                .any(|agent| agent.placement != AgentPlacement::Remote)
        {
            return Err(format!("unexpected remote agent descriptors: {:?}", ready.agents).into());
        }
        Ok(())
    }

    fn shutdown(&mut self) -> Result<(), Box<dyn Error>> {
        self.agents.shutdown()?;
        for process in &mut self.processes {
            process.wait()?;
        }
        Ok(())
    }
}

#[cfg(feature = "web")]
pub fn run_tcp_multi_client_web(bind: SocketAddr) -> Result<(), Box<dyn Error>> {
    let (processes, endpoints) = spawn_tcp_agents()?;
    run_tcp_multi_client_dashboard(bind, false, endpoints, processes)
}

#[cfg(feature = "web")]
pub fn run_tcp_multi_client_web_external(
    bind: SocketAddr,
    endpoints: Vec<AgentEndpointConfig>,
) -> Result<(), Box<dyn Error>> {
    run_tcp_multi_client_dashboard(bind, true, endpoints, Vec::new())
}

#[cfg(feature = "web")]
fn run_tcp_multi_client_dashboard(
    bind: SocketAddr,
    allow_remote: bool,
    endpoints: Vec<AgentEndpointConfig>,
    processes: Vec<TcpAdapterProcess>,
) -> Result<(), Box<dyn Error>> {
    let engine = Engine::new();
    let handle = engine.handle();
    let web_handle = handle.clone();
    thread::Builder::new()
        .name("kneefinder-postgres-demo-web".into())
        .spawn(move || {
            if let Err(error) = WebFrontend::new(bind, allow_remote).run(web_handle) {
                eprintln!("PostgreSQL demo web server failed: {error}");
            }
        })?;
    wait_for_web(bind)?;

    let rates = [150.0, 300.0, 450.0, 600.0, 750.0, 1_000.0, 1_400.0];
    let configured = handle.execute(EngineCommand::Configure {
        config: Box::new(demo_config(endpoints, &rates)),
    })?;
    let prepared = handle.execute(EngineCommand::PrepareAgents {
        run_id: configured.run_id,
    })?;
    let kneefinder::engine::AgentPreparation::Ready { catalog } = prepared.preparation else {
        return Err("engine did not retain the prepared agent catalog".into());
    };
    if catalog.agents.len() != 2
        || catalog
            .agents
            .iter()
            .any(|agent| agent.placement != AgentPlacement::Remote)
    {
        return Err(format!(
            "unexpected prepared agent descriptors: {:?}",
            catalog.agents
        )
        .into());
    }
    println!("multi-client dashboard ready: http://{bind}");
    let _processes = processes;
    loop {
        thread::park();
    }
}

fn demo_config(endpoints: Vec<AgentEndpointConfig>, rates: &[f64]) -> RunConfig {
    RunConfig {
        preset: Preset::Quick,
        strategy: Strategy::Sweep,
        phases: PhaseConfig {
            warmup_ms: 0,
            measurement_ms: 1_000,
            recovery_ms: 0,
            repetitions: 1,
        },
        load: LoadConfig {
            initial_rate: rates[0],
            maximum_rate: *rates.last().expect("dashboard rates are non-empty"),
            growth_factor: 1.5,
            explicit_levels: rates.to_vec(),
            cycles: 1,
        },
        analysis: Default::default(),
        workload: WorkloadConfig {
            operations: OperationSelection::Selected {
                operations: vec![
                    weighted_operation("lookup", "account", ArgumentValue::Integer(1), 32.0),
                    weighted_operation("lookup", "account", ArgumentValue::Integer(2), 8.0),
                    weighted_operation(
                        "transfer",
                        "route",
                        ArgumentValue::String("hot".into()),
                        8.0,
                    ),
                    weighted_operation(
                        "transfer",
                        "route",
                        ArgumentValue::String("cold".into()),
                        2.0,
                    ),
                ],
            },
        },
        output_directory: PathBuf::from("results/postgres-demo"),
        agents: endpoints,
    }
}

fn weighted_operation(
    name: &str,
    argument: &str,
    value: ArgumentValue,
    weight: f64,
) -> WeightedOperation {
    WeightedOperation {
        name: name.into(),
        weight,
        arguments: BTreeMap::from([(argument.into(), value)]),
    }
}

#[cfg(feature = "web")]
fn wait_for_web(bind: SocketAddr) -> Result<(), Box<dyn Error>> {
    let probe = if bind.ip().is_unspecified() {
        SocketAddr::from(([127, 0, 0, 1], bind.port()))
    } else {
        bind
    };
    for _ in 0..50 {
        if TcpStream::connect_timeout(&probe, Duration::from_millis(100)).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(format!("web server did not start at http://{bind}").into())
}

struct TcpAdapterProcess {
    child: Child,
    _stdout: BufReader<ChildStdout>,
    endpoint: String,
}

impl TcpAdapterProcess {
    fn spawn(executable: &std::path::Path) -> Result<Self, Box<dyn Error>> {
        let mut child = Command::new(executable)
            .args(["adapter-tcp", "127.0.0.1:0"])
            .env("KNEEFINDER_POSTGRES_CONNECTIONS", "2")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or("TCP adapter stdout unavailable")?;
        let mut stdout = BufReader::new(stdout);
        let mut endpoint = String::new();
        if stdout.read_line(&mut endpoint)? == 0 {
            return Err("TCP adapter exited before publishing its listen address".into());
        }
        let endpoint = endpoint
            .trim()
            .strip_prefix("tcp://")
            .ok_or("TCP adapter published a malformed listen address")?
            .to_owned();
        Ok(Self {
            child,
            _stdout: stdout,
            endpoint,
        })
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn wait(&mut self) -> Result<(), Box<dyn Error>> {
        let status = self.child.wait()?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("TCP adapter exited with {status}").into())
        }
    }
}

impl Drop for TcpAdapterProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// One interleaved 50-operation cycle: lookup(1)=32, lookup(2)=8,
/// transfer(hot)=8, transfer(cold)=2.
fn variant_for_index(index: u64) -> (&'static str, BTreeMap<String, ArgumentValue>) {
    match index % 50 {
        24 | 49 => (
            "transfer",
            BTreeMap::from([("route".into(), ArgumentValue::String("cold".into()))]),
        ),
        5 | 11 | 17 | 23 | 30 | 36 | 42 | 48 => (
            "transfer",
            BTreeMap::from([("route".into(), ArgumentValue::String("hot".into()))]),
        ),
        3 | 9 | 15 | 21 | 28 | 34 | 40 | 46 => (
            "lookup",
            BTreeMap::from([("account".into(), ArgumentValue::Integer(2))]),
        ),
        _ => (
            "lookup",
            BTreeMap::from([("account".into(), ArgumentValue::Integer(1))]),
        ),
    }
}

fn print_rows(rows: &[DemoRow], expected_knee: f64) {
    let maximum_p95 = rows.iter().map(|row| row.p95_ms).fold(0.0_f64, f64::max);
    println!("\nexpected knee: {expected_knee:.0} req/s");
    println!(
        "{:<9} {:<9} {:<9} {:<9} {:<9} {:<12} latency",
        "offered", "goodput", "bad %", "p50 ms", "p95 ms", "dispatch p99"
    );
    for row in rows {
        let bar_width = if maximum_p95 == 0.0 {
            0
        } else {
            (row.p95_ms / maximum_p95 * 32.0).round() as usize
        };
        println!(
            "{:<9.0} {:<9.1} {:<9.2} {:<9.1} {:<9.1} {:<12.2} {}",
            row.offered_per_second,
            row.goodput_per_second,
            row.stats.overall.unsuccessful_rate() * 100.0,
            row.p50_ms,
            row.p95_ms,
            row.dispatch_p99_ms,
            "█".repeat(bar_width.max(1))
        );
    }
}

#[derive(Debug)]
struct DemoRow {
    offered_per_second: f64,
    goodput_per_second: f64,
    p50_ms: f64,
    p95_ms: f64,
    dispatch_p99_ms: f64,
    stats: StatsReport,
}

impl DemoRow {
    fn from_report(report: kneefinder::stats::PhaseReport) -> Self {
        Self {
            offered_per_second: report.offered_rate,
            goodput_per_second: report.goodput_rate,
            p50_ms: ns_to_ms(report.stats.overall.client_latency_ns.p50),
            p95_ms: ns_to_ms(report.stats.overall.client_latency_ns.p95),
            dispatch_p99_ms: ns_to_ms(report.stats.overall.dispatch_lag_ns.p99),
            stats: report.stats,
        }
    }
}

fn print_variant_stats(rows: &[DemoRow]) {
    println!("\nper operation variant:");
    println!(
        "{:<8} {:<24} {:<8} {:<8} {:<8} {:<9} {:<9} {:<9} {:<9}",
        "offered", "variant", "attempts", "ok", "errors", "timeouts", "p50 ms", "p95 ms", "p99 ms"
    );
    for row in rows {
        for variant in &row.stats.variants {
            println!(
                "{:<8.0} {:<24} {:<8} {:<8} {:<8} {:<9} {:<9.1} {:<9.1} {:<9.1}",
                row.offered_per_second,
                format_variant(&variant.variant),
                variant.stats.attempts,
                variant.stats.successful,
                variant.stats.failed,
                variant.stats.timed_out,
                ns_to_ms(variant.stats.client_latency_ns.p50),
                ns_to_ms(variant.stats.client_latency_ns.p95),
                ns_to_ms(variant.stats.client_latency_ns.p99),
            );
        }
    }
}

fn format_variant(variant: &OperationVariant) -> String {
    let arguments = variant
        .arguments
        .iter()
        .map(|(name, value)| {
            let value = match value {
                ArgumentValue::Integer(value) => value.to_string(),
                ArgumentValue::String(value) => value.clone(),
            };
            format!("{name}={value}")
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("{}({arguments})", variant.operation)
}

fn ns_to_ms(value: Option<u64>) -> f64 {
    value.unwrap_or_default() as f64 / 1e6
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}
