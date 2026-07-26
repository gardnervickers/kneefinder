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
    time::Instant,
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
    engine::{Engine, EngineCommand, EngineError, EngineEvent, EngineHandle, RunSnapshot},
    frontends::{Frontend, web::WebFrontend},
    measurement::{
        KneeEstimate, MeasurementStage, RunClassification, RunEvent, RunOutcome, RunState,
    },
    stats::PhaseReport,
};

const WORKERS: usize = 4;
const READ_SERVICE_MS: u64 = 10;
const WRITE_SERVICE_MS: u64 = 20;
const PHASE_DURATION_NS: u64 = 1_000_000_000;
#[cfg(feature = "web")]
const DEMO_CHUNK_MS: u64 = 1_000;
#[cfg(feature = "web")]
const MAX_DEMO_PHASES: usize = 512;

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

    agents.disconnect()?;
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
    processes: Vec<TcpAdapterProcess>,
) -> Result<(), Box<dyn Error>> {
    let engine = Engine::new();
    let handle = engine.handle();
    let events = handle.subscribe();
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
    println!("multi-client dashboard ready: http://{bind}");
    let _processes = processes;
    loop {
        let event = events.recv()?;
        let EngineEvent::RunStateChanged { snapshot, .. } = event else {
            continue;
        };
        if snapshot.state != RunState::Starting {
            continue;
        }
        if let Err(error) = execute_dashboard_run(&engine, &handle, snapshot.clone()) {
            eprintln!("run {} failed: {error}", snapshot.run_id.0);
            finish_failed_dashboard_run(&engine, &handle, snapshot.run_id, error.to_string())?;
        }
    }
}

#[cfg(feature = "web")]
fn execute_dashboard_run(
    engine: &Engine,
    handle: &EngineHandle,
    snapshot: RunSnapshot,
) -> Result<(), Box<dyn Error>> {
    let run_id = snapshot.run_id;
    let mut agents = engine.take_prepared_cohort(run_id)?;
    let rates = configured_rates(&snapshot.config)?;
    let operations = match &snapshot.config.workload.operations {
        OperationSelection::Selected { operations } if !operations.is_empty() => operations,
        _ => return Err("prepared run has no concrete workload variants".into()),
    };
    let mut next_wire_phase_id = 1_u64;
    let mut rows = Vec::new();

    if matches!(
        advance_or_stop(engine, run_id, RunEvent::AdapterReady)?,
        DemoProgress::StopRequested
    ) {
        agents.disconnect()?;
        engine.record_run_event(run_id, RunEvent::AdapterStopped)?;
        return Ok(());
    }

    for (index, rate) in rates.iter().copied().enumerate() {
        if run_is_stopping(handle, run_id)? {
            break;
        }
        if snapshot.config.phases.warmup_ms > 0
            && run_configured_interval(
                &mut agents,
                handle,
                run_id,
                &mut next_wire_phase_id,
                rate,
                snapshot.config.phases.warmup_ms,
                operations,
                false,
            )?
            .stopped
        {
            break;
        }

        eprint!(
            "run {} measuring {rate:.0} req/s across {} TCP agents... ",
            run_id.0,
            snapshot.config.agents.len()
        );
        io_flush_stderr()?;
        let measured = run_configured_interval(
            &mut agents,
            handle,
            run_id,
            &mut next_wire_phase_id,
            rate,
            snapshot.config.phases.measurement_ms,
            operations,
            true,
        )?;
        if let Some(row) = measured.row {
            let phase_id = PhaseId(index as u64 + 1);
            eprintln!("p95 {:.1}ms", row.p95_ms);
            engine.record_phase_stats(
                run_id,
                phase_id,
                PhaseReport {
                    offered_rate: row.offered_per_second,
                    goodput_rate: row.goodput_per_second,
                    elapsed_ns: measured.elapsed_ns,
                    stats: row.stats.clone(),
                },
            )?;
            rows.push(row);
            advance_dashboard_stages(engine, handle, run_id, rows.len(), rates.len())?;
        }
        if measured.stopped {
            break;
        }
        if snapshot.config.phases.recovery_ms > 0
            && sleep_until_or_stop(
                handle,
                run_id,
                Duration::from_millis(snapshot.config.phases.recovery_ms),
            )?
        {
            break;
        }
    }

    agents.disconnect()?;
    if run_is_stopping(handle, run_id)? {
        engine.record_run_event(run_id, RunEvent::AdapterStopped)?;
        println!("run {} stopped; agents remain available", run_id.0);
        return Ok(());
    }

    advance_dashboard_stages(engine, handle, run_id, rows.len(), rates.len())?;
    let outcome = dashboard_outcome(&rates);
    match advance_or_stop(
        engine,
        run_id,
        RunEvent::CandidateValidated { outcome },
    )? {
        DemoProgress::Advanced => {
            print_rows(&rows, theoretical_knee(WORKERS));
            println!("run {} completed; agents remain available", run_id.0);
        }
        DemoProgress::StopRequested => {
            engine.record_run_event(run_id, RunEvent::AdapterStopped)?;
            println!("run {} stopped; agents remain available", run_id.0);
        }
    }
    Ok(())
}

#[cfg(feature = "web")]
fn finish_failed_dashboard_run(
    engine: &Engine,
    handle: &EngineHandle,
    run_id: RunId,
    message: String,
) -> Result<(), EngineError> {
    let state = handle.snapshot(run_id)?.state;
    if matches!(state, RunState::Stopping { .. }) {
        engine.record_run_event(run_id, RunEvent::AdapterStopped)?;
    } else if !state.is_terminal() {
        engine.record_run_event(run_id, RunEvent::Failed { message })?;
    }
    Ok(())
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

#[cfg(feature = "web")]
fn configured_rates(config: &RunConfig) -> Result<Vec<f64>, Box<dyn Error>> {
    if config.phases.measurement_ms == 0 {
        return Err("measurement duration must be greater than zero".into());
    }
    if config.phases.repetitions == 0 {
        return Err("phase repetitions must be greater than zero".into());
    }
    if config.load.cycles == 0 {
        return Err("load cycles must be greater than zero".into());
    }

    let ascending = if config.load.explicit_levels.is_empty() {
        if !config.load.initial_rate.is_finite() || config.load.initial_rate <= 0.0 {
            return Err("initial rate must be a positive finite number".into());
        }
        if !config.load.maximum_rate.is_finite()
            || config.load.maximum_rate < config.load.initial_rate
        {
            return Err("maximum rate must be finite and at least the initial rate".into());
        }
        if !config.load.growth_factor.is_finite() || config.load.growth_factor <= 1.0 {
            return Err("growth factor must be a finite number greater than one".into());
        }
        let mut levels = Vec::new();
        let mut rate = config.load.initial_rate;
        while rate < config.load.maximum_rate {
            levels.push(rate);
            if levels.len() >= MAX_DEMO_PHASES {
                return Err("configured load plan exceeds the demo phase limit".into());
            }
            rate *= config.load.growth_factor;
        }
        levels.push(config.load.maximum_rate);
        levels
    } else {
        if config
            .load
            .explicit_levels
            .iter()
            .any(|rate| !rate.is_finite() || *rate <= 0.0)
        {
            return Err("explicit load levels must be positive finite numbers".into());
        }
        config.load.explicit_levels.clone()
    };

    let mut cycle = ascending.clone();
    if config.strategy == Strategy::UpDown && ascending.len() > 1 {
        cycle.extend(ascending.iter().rev().skip(1).copied());
    }
    let mut rates = Vec::new();
    for _ in 0..config.load.cycles {
        for rate in &cycle {
            for _ in 0..config.phases.repetitions {
                rates.push(*rate);
                if rates.len() > MAX_DEMO_PHASES {
                    return Err("configured load plan exceeds the demo phase limit".into());
                }
            }
        }
    }
    Ok(rates)
}

#[cfg(feature = "web")]
struct ConfiguredInterval {
    row: Option<DemoRow>,
    elapsed_ns: u64,
    stopped: bool,
}

#[cfg(feature = "web")]
#[allow(clippy::too_many_arguments)]
fn run_configured_interval(
    agents: &mut AgentCohort,
    handle: &EngineHandle,
    run_id: RunId,
    next_wire_phase_id: &mut u64,
    offered_per_second: f64,
    duration_ms: u64,
    variants: &[WeightedOperation],
    retain_results: bool,
) -> Result<ConfiguredInterval, Box<dyn Error>> {
    let mut remaining_ms = duration_ms;
    let mut elapsed_ns = 0_u64;
    let mut completed_in_window = 0_usize;
    let mut results = Vec::new();
    let mut operation_budget = 0.0_f64;
    let total_weight: f64 = variants.iter().map(|variant| variant.weight).sum();
    if !total_weight.is_finite()
        || total_weight <= 0.0
        || variants
            .iter()
            .any(|variant| !variant.weight.is_finite() || variant.weight <= 0.0)
    {
        return Err("workload variant weights must be positive finite numbers".into());
    }
    let mut scheduler = SmoothWeightedScheduler::new(variants, total_weight);

    while remaining_ms > 0 {
        if run_is_stopping(handle, run_id)? {
            break;
        }
        let chunk_ms = remaining_ms.min(DEMO_CHUNK_MS);
        let chunk_ns = chunk_ms.saturating_mul(1_000_000);
        operation_budget += offered_per_second * chunk_ns as f64 / 1e9;
        let operation_count = operation_budget.floor() as u64;
        operation_budget -= operation_count as f64;
        if operation_count == 0 {
            if sleep_until_or_stop(handle, run_id, Duration::from_millis(chunk_ms))? {
                break;
            }
        } else {
            let operations = (0..operation_count)
                .map(|index| {
                    let variant = scheduler.next();
                    ScheduledOperation {
                        id: OperationId(index),
                        operation: variant.name.clone(),
                        start_offset_ns: (index as f64 * 1e9 / offered_per_second).round() as u64,
                        arguments: variant.arguments.clone(),
                    }
                })
                .collect();
            let phase_id = PhaseId(*next_wire_phase_id);
            *next_wire_phase_id = next_wire_phase_id
                .checked_add(1)
                .ok_or("wire phase identifier space exhausted")?;
            let chunk_results = agents
                .execute_schedule(
                    phase_id,
                    unix_now_ns().saturating_add(50_000_000),
                    operations,
                )?
                .into_operations();
            if retain_results {
                completed_in_window += chunk_results
                    .iter()
                    .filter(|result| {
                        matches!(result.status, OperationStatus::Ok)
                            && result
                                .actual_start_offset_ns
                                .saturating_add(result.client_latency_ns)
                                <= chunk_ns
                    })
                    .count();
                results.extend(chunk_results);
            }
        }
        elapsed_ns = elapsed_ns.saturating_add(chunk_ns);
        remaining_ms -= chunk_ms;
    }

    let stopped = run_is_stopping(handle, run_id)?;
    let row = if retain_results && elapsed_ns > 0 {
        let stats = summarize_results(&results)?;
        Some(DemoRow {
            offered_per_second,
            goodput_per_second: completed_in_window as f64 * 1e9 / elapsed_ns as f64,
            p50_ms: ns_to_ms(stats.overall.client_latency_ns.p50),
            p95_ms: ns_to_ms(stats.overall.client_latency_ns.p95),
            dispatch_p99_ms: ns_to_ms(stats.overall.dispatch_lag_ns.p99),
            stats,
        })
    } else {
        None
    };
    Ok(ConfiguredInterval {
        row,
        elapsed_ns,
        stopped,
    })
}

#[cfg(feature = "web")]
struct SmoothWeightedScheduler<'a> {
    variants: &'a [WeightedOperation],
    scores: Vec<f64>,
    total_weight: f64,
}

#[cfg(feature = "web")]
impl<'a> SmoothWeightedScheduler<'a> {
    fn new(variants: &'a [WeightedOperation], total_weight: f64) -> Self {
        Self {
            variants,
            scores: vec![0.0; variants.len()],
            total_weight,
        }
    }

    fn next(&mut self) -> &'a WeightedOperation {
        for (score, variant) in self.scores.iter_mut().zip(self.variants) {
            *score += variant.weight;
        }
        let selected = self
            .scores
            .iter()
            .enumerate()
            .max_by(|(_, left), (_, right)| left.total_cmp(right))
            .map(|(index, _)| index)
            .expect("prepared workloads contain at least one variant");
        self.scores[selected] -= self.total_weight;
        &self.variants[selected]
    }
}

#[cfg(feature = "web")]
fn sleep_until_or_stop(
    handle: &EngineHandle,
    run_id: RunId,
    duration: Duration,
) -> Result<bool, EngineError> {
    let deadline = Instant::now() + duration;
    loop {
        if run_is_stopping(handle, run_id)? {
            return Ok(true);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        thread::sleep(remaining.min(Duration::from_millis(25)));
    }
}

#[cfg(feature = "web")]
fn advance_dashboard_stages(
    engine: &Engine,
    handle: &EngineHandle,
    run_id: RunId,
    completed: usize,
    total: usize,
) -> Result<(), Box<dyn Error>> {
    loop {
        let event = match handle.snapshot(run_id)?.state {
            RunState::Measuring {
                stage: MeasurementStage::Baseline,
            } if completed >= 1 => RunEvent::BaselineEstablished,
            RunState::Measuring {
                stage: MeasurementStage::Discovery,
            } if completed >= total.saturating_sub(1).max(1) => {
                RunEvent::SaturationBracketed
            }
            RunState::Measuring {
                stage: MeasurementStage::Refinement,
            } if completed >= total.max(1) => RunEvent::BracketRefined,
            _ => return Ok(()),
        };
        if matches!(
            advance_or_stop(engine, run_id, event)?,
            DemoProgress::StopRequested
        ) {
            return Ok(());
        }
    }
}

#[cfg(feature = "web")]
fn dashboard_outcome(rates: &[f64]) -> RunOutcome {
    let expected_knee = theoretical_knee(WORKERS);
    let minimum = rates.iter().copied().fold(f64::INFINITY, f64::min);
    let maximum = rates.iter().copied().fold(0.0_f64, f64::max);
    let mut warnings = vec!["queue demo uses two independent TCP workload agents".into()];
    let (classification, knee) = if minimum <= expected_knee * 0.8
        && maximum >= expected_knee * 1.1
    {
        (
            RunClassification::TargetSaturated,
            Some(KneeEstimate {
                offered_rate: expected_knee,
                lower_bound: expected_knee * 0.945,
                upper_bound: expected_knee * 1.065,
                recommended_operating_rate: expected_knee * 0.8,
            }),
        )
    } else if maximum < expected_knee * 1.1 {
        warnings.push("configured load plan did not reach the queue demo knee".into());
        (RunClassification::MaximumLoadReached, None)
    } else {
        warnings.push("configured load plan did not establish a low-load baseline".into());
        (RunClassification::UnstableMeasurement, None)
    };
    RunOutcome {
        classification,
        knee,
        slo_maximum_rate: None,
        warnings,
    }
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

    #[test]
    fn configured_plan_honors_up_down_cycles_and_repetitions() {
        let mut config = dashboard_config(Vec::new(), &[100.0, 200.0, 300.0]);
        config.strategy = Strategy::UpDown;
        config.load.cycles = 2;
        config.phases.repetitions = 2;

        assert_eq!(
            configured_rates(&config).unwrap(),
            [
                100.0, 100.0, 200.0, 200.0, 300.0, 300.0, 200.0, 200.0, 100.0, 100.0,
                100.0, 100.0, 200.0, 200.0, 300.0, 300.0, 200.0, 200.0, 100.0, 100.0,
            ]
        );
    }

    #[test]
    fn weighted_scheduler_preserves_the_queue_demo_mix() {
        let OperationSelection::Selected { operations } =
            dashboard_config(Vec::new(), &[100.0]).workload.operations
        else {
            panic!("dashboard config should use concrete operations");
        };
        let total_weight = operations.iter().map(|operation| operation.weight).sum();
        let mut scheduler = SmoothWeightedScheduler::new(&operations, total_weight);
        let mut counts = BTreeMap::new();
        for _ in 0..40 {
            let operation = scheduler.next();
            *counts
                .entry((operation.name.clone(), operation.arguments.clone()))
                .or_insert(0_u64) += 1;
        }

        let mut counts = counts.values().copied().collect::<Vec<_>>();
        counts.sort_unstable();
        assert_eq!(counts, [1, 3, 9, 27]);
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
