use std::{
    collections::BTreeMap,
    env,
    error::Error,
    io::{BufRead, BufReader, Write},
    process::{Child, ChildStdout, Command, Stdio},
    time::Duration,
    time::{SystemTime, UNIX_EPOCH},
};

#[cfg(feature = "web")]
use std::{
    net::{SocketAddr, TcpStream},
    path::PathBuf,
    thread,
};

use kneefinder::{
    adapter_session::SessionOptions,
    agent::{AgentCohort, AgentPlacement},
    config::{AdapterCommand, AgentEndpointConfig, AgentTransportConfig},
    protocol::{ArgumentValue, OperationId, OperationStatus, PhaseId, RunId, ScheduledOperation},
    stats::{OperationVariant, StatsReport, summarize_results},
};

#[cfg(feature = "web")]
use kneefinder::{
    config::{
        LoadConfig, OperationSelection, PhaseConfig, Preset, RunConfig, Strategy,
        WeightedOperation, WorkloadConfig,
    },
    engine::{Engine, EngineCommand, EngineError, EngineHandle},
    frontends::{Frontend, web::WebFrontend},
    measurement::{KneeEstimate, RunClassification, RunEvent, RunOutcome, RunState},
    stats::PhaseReport,
};

const WORKERS: usize = 4;
const READ_SERVICE_MS: u64 = 10;
const WRITE_SERVICE_MS: u64 = 20;
const PHASE_DURATION_NS: u64 = 1_000_000_000;

fn theoretical_knee(workers: usize) -> f64 {
    let average_read_ms = READ_SERVICE_MS as f64 * 1.25;
    let average_write_ms = WRITE_SERVICE_MS as f64 * 1.25;
    let average_service_ms = (9.0 * average_read_ms + average_write_ms) / 10.0;
    workers as f64 * 1_000.0 / average_service_ms
}

pub fn run() -> Result<(), Box<dyn Error>> {
    let executable = env::current_exe()?;
    let command = AdapterCommand {
        program: executable.to_string_lossy().into_owned(),
        arguments: vec!["adapter".into()],
    };
    let mut agents = AgentCohort::from_endpoints(
        &[AgentEndpointConfig {
            id: "colocated-0".into(),
            transport: AgentTransportConfig::Subprocess { command },
        }],
        SessionOptions::default(),
    )?;
    let ready = agents.initialize(
        RunId(1),
        serde_json::json!({
            "workers": WORKERS,
            "read_service_ms": READ_SERVICE_MS,
            "write_service_ms": WRITE_SERVICE_MS,
            "queue_capacity": 4096
        }),
    )?;
    if ready
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .eq(["read", "write"])
    {
        eprintln!("adapter advertised operations: read=9, write=1");
    } else {
        return Err(format!("unexpected advertised operations: {:?}", ready.operations).into());
    }

    let expected_knee = theoretical_knee(WORKERS);
    eprintln!(
        "e2e topology: coordinator -> colocated agent -> external adapter -> internal queue service"
    );
    eprintln!(
        "target: {WORKERS} workers; 90/10 operations and 3/1 argument variants; theoretical knee {expected_knee:.0} req/s\n"
    );

    let rates = [100.0, 200.0, 250.0, 290.0, 325.0, 425.0, 550.0];
    let mut rows = Vec::new();
    for (index, rate) in rates.into_iter().enumerate() {
        eprint!("measuring {rate:.0} req/s... ");
        io_flush_stderr()?;
        let row = run_phase(&mut agents, PhaseId(index as u64 + 1), rate)?;
        eprintln!("p95 {:.1}ms", row.p95_ms);
        rows.push(row);
    }

    agents.shutdown()?;
    print_rows(&rows, expected_knee);
    print_variant_stats(&rows);
    Ok(())
}

pub fn run_tcp_multi_client() -> Result<(), Box<dyn Error>> {
    let mut cohort = connect_tcp_cohort()?;
    cohort.initialize(RunId(2), 2)?;

    let offered_per_second = 100.0;
    let operations = (0..80)
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
    )?;
    if phase.agents.len() != 2 {
        return Err(format!("expected two per-agent results, got {}", phase.agents.len()).into());
    }
    for result in &phase.agents {
        if result.operations.len() != 40 {
            return Err(format!(
                "agent {} executed {} operations instead of 40",
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
        if attempts != [27, 9, 1, 3] {
            return Err(format!(
                "agent {} received an imbalanced variant mix: {attempts:?}",
                result.agent.id,
            )
            .into());
        }
    }
    let operations = phase.into_operations();
    let stats = summarize_results(&operations)?;
    if stats.overall.attempts != 80
        || stats.overall.successful != 80
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
    println!("multi-client TCP E2E passed: 2 clients, 40 operations each, 80 successful total");
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
        workers_per_agent: usize,
    ) -> Result<(), Box<dyn Error>> {
        let ready = self.agents.initialize(
            run_id,
            serde_json::json!({
                "workers": workers_per_agent,
                "read_service_ms": READ_SERVICE_MS,
                "write_service_ms": WRITE_SERVICE_MS,
                "queue_capacity": 4096
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
    mut processes: Vec<TcpAdapterProcess>,
) -> Result<(), Box<dyn Error>> {
    let engine = Engine::new();
    let handle = engine.handle();
    let web_handle = handle.clone();
    thread::Builder::new()
        .name("kneefinder-queue-demo-web".into())
        .spawn(move || {
            if let Err(error) = WebFrontend::new(bind, allow_remote).run(web_handle) {
                eprintln!("queue demo web server failed: {error}");
            }
        })?;
    wait_for_web(bind)?;

    let rates = [100.0, 200.0, 250.0, 290.0, 325.0, 425.0, 550.0];
    let configured = handle.execute(EngineCommand::Configure {
        config: Box::new(dashboard_config(endpoints, &rates)),
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
    handle.execute(EngineCommand::Start {
        run_id: configured.run_id,
    })?;
    let mut agents = engine.take_prepared_cohort(configured.run_id)?;
    let mut stop_requested = matches!(
        advance_or_stop(&engine, configured.run_id, RunEvent::AdapterReady)?,
        DemoProgress::StopRequested
    );

    let mut rows = Vec::new();
    for (index, rate) in rates.into_iter().enumerate() {
        if stop_requested || run_is_stopping(&handle, configured.run_id)? {
            stop_requested = true;
            break;
        }

        eprint!("measuring {rate:.0} req/s across two TCP agents... ");
        io_flush_stderr()?;
        let phase_id = PhaseId(index as u64 + 1);
        let row = run_phase(&mut agents, phase_id, rate)?;
        eprintln!("p95 {:.1}ms", row.p95_ms);
        engine.record_phase_stats(
            configured.run_id,
            phase_id,
            PhaseReport {
                offered_rate: row.offered_per_second,
                goodput_rate: row.goodput_per_second,
                elapsed_ns: PHASE_DURATION_NS,
                stats: row.stats.clone(),
            },
        )?;
        rows.push(row);

        if run_is_stopping(&handle, configured.run_id)? {
            stop_requested = true;
            break;
        }

        let progress = match index {
            0 => Some(RunEvent::BaselineEstablished),
            4 => Some(RunEvent::SaturationBracketed),
            5 => Some(RunEvent::BracketRefined),
            _ => None,
        };
        if let Some(progress) = progress
            && matches!(
                advance_or_stop(&engine, configured.run_id, progress)?,
                DemoProgress::StopRequested
            )
        {
            stop_requested = true;
            break;
        }
    }

    agents.shutdown()?;
    for process in &mut processes {
        process.wait()?;
    }
    let expected_knee = theoretical_knee(WORKERS);
    let completed = if stop_requested {
        false
    } else {
        matches!(
            advance_or_stop(
                &engine,
                configured.run_id,
                RunEvent::CandidateValidated {
                    outcome: RunOutcome {
                        classification: RunClassification::TargetSaturated,
                        knee: Some(KneeEstimate {
                            offered_rate: expected_knee,
                            lower_bound: 275.0,
                            upper_bound: 310.0,
                            recommended_operating_rate: expected_knee * 0.8,
                        }),
                        slo_maximum_rate: None,
                        warnings: vec![
                            "queue demo uses two independent TCP workload agents".into(),
                        ],
                    },
                },
            )?,
            DemoProgress::Advanced
        )
    };
    if !completed {
        engine.record_run_event(configured.run_id, RunEvent::AdapterStopped)?;
    }
    print_rows(&rows, expected_knee);
    if completed {
        println!("\nmulti-client dashboard ready: http://{bind}");
    } else {
        println!("\nmulti-client run stopped; dashboard remains available at http://{bind}");
    }
    loop {
        thread::park();
    }
}

#[cfg(feature = "web")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DemoProgress {
    Advanced,
    StopRequested,
}

#[cfg(feature = "web")]
fn advance_or_stop(
    engine: &Engine,
    run_id: RunId,
    event: RunEvent,
) -> Result<DemoProgress, Box<dyn Error>> {
    match engine.record_run_event(run_id, event) {
        Ok(_) => Ok(DemoProgress::Advanced),
        Err(EngineError::InvalidTransition { source, .. })
            if matches!(*source.state, RunState::Stopping { .. }) =>
        {
            Ok(DemoProgress::StopRequested)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(feature = "web")]
fn run_is_stopping(handle: &EngineHandle, run_id: RunId) -> Result<bool, EngineError> {
    Ok(matches!(
        handle.snapshot(run_id)?.state,
        RunState::Stopping { .. }
    ))
}

#[cfg(all(test, feature = "web"))]
mod web_stop_tests {
    use super::*;

    #[test]
    fn validation_completion_yields_to_a_stop_request() {
        let engine = Engine::new();
        let handle = engine.handle();
        let configured = handle
            .execute(EngineCommand::Configure {
                config: Box::new(dashboard_config(Vec::new(), &[100.0, 200.0])),
            })
            .unwrap();
        handle
            .execute(EngineCommand::Start {
                run_id: configured.run_id,
            })
            .unwrap();
        for event in [
            RunEvent::AdapterReady,
            RunEvent::BaselineEstablished,
            RunEvent::SaturationBracketed,
            RunEvent::BracketRefined,
        ] {
            engine.record_run_event(configured.run_id, event).unwrap();
        }
        handle
            .execute(EngineCommand::Stop {
                run_id: configured.run_id,
            })
            .unwrap();

        let progress = advance_or_stop(
            &engine,
            configured.run_id,
            RunEvent::CandidateValidated {
                outcome: RunOutcome {
                    classification: RunClassification::TargetSaturated,
                    knee: None,
                    slo_maximum_rate: None,
                    warnings: Vec::new(),
                },
            },
        )
        .unwrap();

        assert_eq!(progress, DemoProgress::StopRequested);
        engine
            .record_run_event(configured.run_id, RunEvent::AdapterStopped)
            .unwrap();
        assert_eq!(
            handle.snapshot(configured.run_id).unwrap().state,
            RunState::Stopped
        );
    }
}

#[cfg(feature = "web")]
fn dashboard_config(endpoints: Vec<AgentEndpointConfig>, rates: &[f64]) -> RunConfig {
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
        workload: WorkloadConfig {
            operations: OperationSelection::Selected {
                operations: vec![
                    weighted_operation("read", "key", ArgumentValue::Integer(0), 27.0),
                    weighted_operation("read", "key", ArgumentValue::Integer(1), 9.0),
                    weighted_operation(
                        "write",
                        "value",
                        ArgumentValue::String("small".into()),
                        3.0,
                    ),
                    weighted_operation(
                        "write",
                        "value",
                        ArgumentValue::String("large".into()),
                        1.0,
                    ),
                ],
            },
        },
        output_directory: PathBuf::from("results/queue-demo-multi-client"),
        agents: endpoints,
    }
}

#[cfg(feature = "web")]
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
            .env("KNEEFINDER_QUEUE_DEMO_WORKERS", "2")
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

fn run_phase(
    agents: &mut AgentCohort,
    phase_id: PhaseId,
    offered_per_second: f64,
) -> Result<DemoRow, Box<dyn Error>> {
    let operation_count = (offered_per_second * PHASE_DURATION_NS as f64 / 1e9).floor() as u64;
    let phase_start_unix_ns = unix_now_ns().saturating_add(100_000_000);
    let operations = (0..operation_count)
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

    let results = agents
        .execute_schedule(phase_id, phase_start_unix_ns, operations)?
        .into_operations();

    let stats = summarize_results(&results)?;

    let completed_in_window = results
        .iter()
        .filter(|result| {
            matches!(result.status, OperationStatus::Ok)
                && result
                    .actual_start_offset_ns
                    .saturating_add(result.client_latency_ns)
                    <= PHASE_DURATION_NS
        })
        .count();

    Ok(DemoRow {
        offered_per_second,
        goodput_per_second: completed_in_window as f64 * 1e9 / PHASE_DURATION_NS as f64,
        p50_ms: ns_to_ms(stats.overall.client_latency_ns.p50),
        p95_ms: ns_to_ms(stats.overall.client_latency_ns.p95),
        dispatch_p99_ms: ns_to_ms(stats.overall.dispatch_lag_ns.p99),
        stats,
    })
}

/// One interleaved 40-operation cycle: read(0)=27, read(1)=9,
/// write(small)=3, write(large)=1.
fn variant_for_index(index: u64) -> (&'static str, BTreeMap<String, ArgumentValue>) {
    match index % 40 {
        39 => (
            "write",
            BTreeMap::from([("value".into(), ArgumentValue::String("large".into()))]),
        ),
        9 | 19 | 29 => (
            "write",
            BTreeMap::from([("value".into(), ArgumentValue::String("small".into()))]),
        ),
        3 | 7 | 11 | 15 | 23 | 27 | 31 | 35 | 37 => (
            "read",
            BTreeMap::from([("key".into(), ArgumentValue::Integer(1))]),
        ),
        _ => (
            "read",
            BTreeMap::from([("key".into(), ArgumentValue::Integer(0))]),
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

fn io_flush_stderr() -> Result<(), Box<dyn Error>> {
    std::io::stderr().flush()?;
    Ok(())
}
