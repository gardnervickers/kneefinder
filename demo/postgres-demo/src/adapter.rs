use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    error::Error,
    io::{self, BufRead, BufReader, BufWriter, Write},
    net::TcpListener,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Sender, TryRecvError, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use kneefinder::{
    histogram::LatencyHistogram,
    protocol::{
        AdapterIdentity, AdapterMessage, ArgumentKind, ArgumentValue, Capabilities,
        ControllerMessage, HistogramEncoding, Load, LoadModel, ManagedOperation,
        ManagedPhaseRequest, OperationArgument, OperationDescriptor, OperationId,
        OperationInvocation, OperationKind, OperationResult, OperationStatus, PROTOCOL_VERSION,
        PhaseCompletion, PhaseId, PhaseResult,
    },
    stats::{OperationVariant, PhaseAccumulator, PhaseAggregationPlan},
};
use postgres::{Client, NoTls};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct AdapterConfig {
    #[serde(default = "default_database_url")]
    database_url: String,
    #[serde(default = "default_connections")]
    connections: usize,
    #[serde(default = "default_lock_hold_ms")]
    lock_hold_ms: u64,
}

fn default_database_url() -> String {
    env::var("KNEEFINDER_POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://kneefinder:kneefinder@127.0.0.1:5432/kneefinder".into())
}

fn default_connections() -> usize {
    env::var("KNEEFINDER_POSTGRES_CONNECTIONS")
        .ok()
        .and_then(|connections| connections.parse().ok())
        .unwrap_or(4)
}

fn default_lock_hold_ms() -> u64 {
    env::var("KNEEFINDER_POSTGRES_LOCK_HOLD_MS")
        .ok()
        .and_then(|milliseconds| milliseconds.parse().ok())
        .unwrap_or(10)
}

fn maximum_lock_hold_ms() -> u64 {
    20
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionExit {
    EndOfStream,
    Shutdown,
}

pub fn run() -> Result<(), Box<dyn Error>> {
    let stdout = io::stdout();
    run_session(BufReader::new(io::stdin()), BufWriter::new(stdout.lock())).map(|_| ())
}

pub fn run_hanging() -> Result<(), Box<dyn Error>> {
    let stdout = io::stdout();
    run_session_mode(
        BufReader::new(io::stdin()),
        BufWriter::new(stdout.lock()),
        true,
    )
    .map(|_| ())
}

pub fn run_tcp(address: &str) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(address)?;
    let address = listener.local_addr()?;
    println!("tcp://{address}");
    io::stdout().flush()?;

    loop {
        let (stream, peer) = listener.accept()?;
        stream.set_nodelay(true)?;
        eprintln!("PostgreSQL demo agent accepted coordinator connection from {peer}");
        let input = BufReader::new(stream.try_clone()?);
        let output = BufWriter::new(stream);
        match run_session(input, output) {
            Ok(SessionExit::EndOfStream) => {
                eprintln!("PostgreSQL demo agent connection closed; waiting for a coordinator");
            }
            Ok(SessionExit::Shutdown) => return Ok(()),
            Err(error) => {
                eprintln!(
                    "PostgreSQL demo agent session failed ({error}); waiting for another coordinator"
                );
            }
        }
    }
}

fn run_session(
    input: impl BufRead + Send + 'static,
    mut output: impl Write,
) -> Result<SessionExit, Box<dyn Error>> {
    run_session_mode(input, &mut output, false)
}

fn run_session_mode(
    input: impl BufRead + Send + 'static,
    mut output: impl Write,
    hang_on_phase: bool,
) -> Result<SessionExit, Box<dyn Error>> {
    let (sender, events) = mpsc::channel();
    let input_sender = sender.clone();
    thread::Builder::new()
        .name("kneefinder-postgres-input".into())
        .spawn(move || read_input(input, input_sender))?;

    let mut service: Option<PostgresService> = None;
    let mut initialized = false;
    let mut prepared: Option<PreparedPhase> = None;
    let mut active: Option<ActiveWorker> = None;

    while let Ok(event) = events.recv() {
        match event {
            RuntimeEvent::Controller(message) => match message {
                ControllerMessage::Initialize {
                    protocol_version,
                    config: supplied_config,
                    ..
                } if protocol_version == PROTOCOL_VERSION
                    && !initialized
                    && prepared.is_none()
                    && active.is_none() =>
                {
                    let config: AdapterConfig = serde_json::from_value(supplied_config)?;
                    service = if hang_on_phase {
                        None
                    } else {
                        Some(PostgresService::new(config)?)
                    };
                    initialized = true;
                    write_message(&mut output, &ready_message())?;
                }
                ControllerMessage::Initialize {
                    protocol_version, ..
                } => {
                    let (code, message) = if protocol_version != PROTOCOL_VERSION {
                        (
                            "unsupported_protocol",
                            format!(
                                "adapter supports protocol {PROTOCOL_VERSION}, got {protocol_version}"
                            ),
                        )
                    } else {
                        ("invalid_state", "adapter is already initialized".into())
                    };
                    write_error(&mut output, None, code, message, false)?;
                }
                ControllerMessage::PreparePhase { phase_id, request }
                    if initialized && prepared.is_none() && active.is_none() =>
                {
                    match PreparedPhase::new(phase_id, request) {
                        Ok(phase) => {
                            prepared = Some(phase);
                            write_message(&mut output, &AdapterMessage::PhaseReady { phase_id })?;
                        }
                        Err(message) => {
                            write_error(
                                &mut output,
                                Some(phase_id),
                                "invalid_phase",
                                message,
                                false,
                            )?;
                        }
                    }
                }
                ControllerMessage::StartPhase {
                    phase_id,
                    phase_start_unix_ns,
                } if prepared.as_ref().is_some_and(|phase| phase.id == phase_id)
                    && active.is_none() =>
                {
                    let phase = prepared
                        .take()
                        .expect("the phase id guard requires a prepared phase");
                    write_message(&mut output, &AdapterMessage::PhaseStarted { phase_id })?;
                    if hang_on_phase {
                        loop {
                            thread::park();
                        }
                    }
                    let Some(service) = &service else {
                        write_error(
                            &mut output,
                            Some(phase_id),
                            "not_initialized",
                            "initialize the adapter before starting work".into(),
                            true,
                        )?;
                        continue;
                    };
                    active = Some(start_managed_worker(
                        phase,
                        phase_start_unix_ns,
                        service.clone(),
                        sender.clone(),
                    )?);
                }
                ControllerMessage::CancelPhase {
                    phase_id,
                    cancel_at_unix_ns,
                } => {
                    if let Some(worker) = &active
                        && worker.phase_id == phase_id
                    {
                        if let Some(cutoff) = cancel_at_unix_ns {
                            worker.cancel_at_unix_ns.store(cutoff, Ordering::Release);
                        } else {
                            worker.cancel.store(true, Ordering::Release);
                        }
                    } else if prepared.as_ref().is_some_and(|phase| phase.id == phase_id) {
                        let phase = prepared
                            .take()
                            .expect("the phase id guard requires a prepared phase");
                        let result = empty_phase_result_at(&phase.request, 1)?;
                        write_message(
                            &mut output,
                            &AdapterMessage::PhaseComplete {
                                phase_id,
                                completion: PhaseCompletion::Cancelled,
                                result,
                            },
                        )?;
                    }
                }
                ControllerMessage::Shutdown => {
                    cancel_and_join(&mut active);
                    return Ok(SessionExit::Shutdown);
                }
                message => {
                    write_message(
                        &mut output,
                        &AdapterMessage::Error {
                            phase_id: message_phase_id(&message),
                            code: "invalid_state".into(),
                            message: "message is not valid in the adapter's current state".into(),
                            retryable: false,
                        },
                    )?;
                }
            },
            RuntimeEvent::ManagedFinished {
                phase_id,
                completion,
                result,
            } if active
                .as_ref()
                .is_some_and(|worker| worker.phase_id == phase_id) =>
            {
                join_active(&mut active);
                match result {
                    Ok(result) => write_message(
                        &mut output,
                        &AdapterMessage::PhaseComplete {
                            phase_id,
                            completion,
                            result,
                        },
                    )?,
                    Err(message) => {
                        write_error(&mut output, Some(phase_id), "phase_failed", message, false)?
                    }
                }
            }
            RuntimeEvent::ManagedFinished { .. } => {
                write_error(
                    &mut output,
                    None,
                    "stale_worker",
                    "worker completed for a phase that is no longer active".into(),
                    false,
                )?;
            }
            RuntimeEvent::InputFailed(message) => {
                cancel_and_join(&mut active);
                write_error(&mut output, None, "invalid_message", message, false)?;
                return Ok(SessionExit::EndOfStream);
            }
            RuntimeEvent::InputClosed => {
                cancel_and_join(&mut active);
                return Ok(SessionExit::EndOfStream);
            }
        }
    }

    Ok(SessionExit::EndOfStream)
}

fn read_input(input: impl BufRead, sender: Sender<RuntimeEvent>) {
    for line in input.lines() {
        let line = match line {
            Ok(line) => line,
            Err(error) => {
                let _ = sender.send(RuntimeEvent::InputFailed(error.to_string()));
                return;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(&line) {
            Ok(message) => {
                if sender.send(RuntimeEvent::Controller(message)).is_err() {
                    return;
                }
            }
            Err(error) => {
                let _ = sender.send(RuntimeEvent::InputFailed(error.to_string()));
                return;
            }
        }
    }
    let _ = sender.send(RuntimeEvent::InputClosed);
}

fn ready_message() -> AdapterMessage {
    AdapterMessage::Ready {
        protocol_version: PROTOCOL_VERSION,
        identity: AdapterIdentity {
            name: "kneefinder-postgres-demo".into(),
            version: Some(env!("CARGO_PKG_VERSION").into()),
        },
        capabilities: Capabilities {
            adapter_managed_phases: true,
            load_models: vec![LoadModel::OpenLoop],
            histogram_encodings: vec![HistogramEncoding::HdrV2Base64],
        },
        operations: vec![
            OperationDescriptor {
                name: "lookup".into(),
                description: Some("read an account balance through PostgreSQL MVCC".into()),
                kind: OperationKind::Read,
                enabled_by_default: true,
                default_weight: 4.0,
                arguments: vec![OperationArgument {
                    name: "account".into(),
                    description: Some("account id to read".into()),
                    kind: ArgumentKind::Integer,
                    values: Vec::new(),
                    required: true,
                    default: Some(ArgumentValue::Integer(1)),
                }],
            },
            OperationDescriptor {
                name: "transfer".into(),
                description: Some("update a pair of accounts in a PostgreSQL transaction".into()),
                kind: OperationKind::Write,
                enabled_by_default: false,
                default_weight: 1.0,
                arguments: vec![OperationArgument {
                    name: "route".into(),
                    description: Some("account pair updated by the transaction".into()),
                    kind: ArgumentKind::Enum,
                    values: vec!["hot".into(), "cold".into()],
                    required: true,
                    default: Some(ArgumentValue::String("hot".into())),
                }],
            },
        ],
    }
}

#[derive(Debug)]
enum RuntimeEvent {
    Controller(ControllerMessage),
    ManagedFinished {
        phase_id: PhaseId,
        completion: PhaseCompletion,
        result: Result<PhaseResult, String>,
    },
    InputFailed(String),
    InputClosed,
}

struct ActiveWorker {
    phase_id: PhaseId,
    cancel: Arc<AtomicBool>,
    cancel_at_unix_ns: Arc<AtomicU64>,
    handle: JoinHandle<()>,
}

fn join_active(active: &mut Option<ActiveWorker>) {
    if let Some(worker) = active.take() {
        let _ = worker.handle.join();
    }
}

fn cancel_and_join(active: &mut Option<ActiveWorker>) {
    if let Some(worker) = active.as_ref() {
        worker.cancel.store(true, Ordering::Release);
    }
    join_active(active);
}

struct PreparedPhase {
    id: PhaseId,
    request: ManagedPhaseRequest,
}

impl PreparedPhase {
    fn new(id: PhaseId, request: ManagedPhaseRequest) -> Result<Self, String> {
        validate_managed_request(&request)?;
        PhaseAccumulator::new(aggregation_plan(&request)?).map_err(|error| error.to_string())?;
        Ok(Self { id, request })
    }
}

fn start_managed_worker(
    phase: PreparedPhase,
    phase_start_unix_ns: u64,
    service: PostgresService,
    sender: Sender<RuntimeEvent>,
) -> Result<ActiveWorker, io::Error> {
    let phase_id = phase.id;
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_at_unix_ns = Arc::new(AtomicU64::new(0));
    let worker_cancel = Arc::clone(&cancel);
    let worker_cancel_at_unix_ns = Arc::clone(&cancel_at_unix_ns);
    let handle = thread::Builder::new()
        .name(format!("kneefinder-postgres-managed-{}", phase_id.0))
        .spawn(move || {
            let (completion, result) = match run_managed_phase(
                service,
                &phase.request,
                phase_start_unix_ns,
                worker_cancel,
                worker_cancel_at_unix_ns,
            ) {
                Ok((completion, result)) => (completion, Ok(result)),
                Err(error) => (PhaseCompletion::Cancelled, Err(error)),
            };
            let _ = sender.send(RuntimeEvent::ManagedFinished {
                phase_id,
                completion,
                result,
            });
        })?;
    Ok(ActiveWorker {
        phase_id,
        cancel,
        cancel_at_unix_ns,
        handle,
    })
}

trait OperationService: Clone + Send + Sync + 'static {
    fn concurrency(&self) -> usize;

    fn call(
        &self,
        operation: &str,
        arguments: &BTreeMap<String, ArgumentValue>,
    ) -> Result<(), Box<dyn Error + Send + Sync>>;
}

fn run_managed_phase<S: OperationService>(
    service: S,
    request: &ManagedPhaseRequest,
    phase_start_unix_ns: u64,
    cancel: Arc<AtomicBool>,
    cancel_at_unix_ns: Arc<AtomicU64>,
) -> Result<(PhaseCompletion, PhaseResult), String> {
    validate_managed_request(request)?;
    let rate = match request.load {
        Load::OpenLoop {
            requests_per_second,
        } => requests_per_second,
        Load::ClosedLoop { .. } => return Err("only open-loop phases are supported".into()),
    };

    if !wait_until_unix(phase_start_unix_ns, &cancel, &cancel_at_unix_ns) {
        return Ok((
            PhaseCompletion::Cancelled,
            empty_phase_result_at(request, 1)?,
        ));
    }

    if request.warmup_ns > 0
        && !drive_segment(
            service.clone(),
            request,
            rate,
            request.warmup_ns,
            &cancel,
            &cancel_at_unix_ns,
            None,
        )?
        .0
    {
        return Ok((
            PhaseCompletion::Cancelled,
            empty_phase_result_at(request, 1)?,
        ));
    }

    let mut accumulator =
        PhaseAccumulator::new(aggregation_plan(request)?).map_err(|error| error.to_string())?;
    let (completed, elapsed_ns) = drive_segment(
        service,
        request,
        rate,
        request.measurement_ns,
        &cancel,
        &cancel_at_unix_ns,
        Some(&mut accumulator),
    )?;
    let elapsed_ns = if completed {
        elapsed_ns
    } else {
        managed_elapsed_at_cutoff(request, phase_start_unix_ns, &cancel_at_unix_ns, elapsed_ns)
    };
    let result = accumulator
        .to_phase_result_at(elapsed_ns)
        .map_err(|error| error.to_string())?;
    Ok((
        if completed {
            PhaseCompletion::Completed
        } else {
            PhaseCompletion::Cancelled
        },
        result,
    ))
}

#[derive(Debug)]
struct ManagedJob {
    operation: OperationInvocation,
    variant: OperationVariant,
}

#[derive(Debug)]
enum WorkerResult {
    Completed(OperationResult),
    Unstarted {
        variant: OperationVariant,
        intended_start_offset_ns: u64,
    },
}

fn drive_segment<S: OperationService>(
    service: S,
    request: &ManagedPhaseRequest,
    rate: f64,
    duration_ns: u64,
    cancel: &Arc<AtomicBool>,
    cancel_at_unix_ns: &Arc<AtomicU64>,
    mut accumulator: Option<&mut PhaseAccumulator>,
) -> Result<(bool, u64), String> {
    let (shard_index, shard_count) = phase_shard(&request.operations)?;
    let total_weight = request
        .operations
        .iter()
        .map(|operation| operation.weight)
        .sum();
    let segment_started = Instant::now();
    let latest_start_ns = duration_ns
        .checked_add(request.operation_timeout_ns)
        .ok_or("phase duration and operation timeout overflow")?;
    let worker_count = service.concurrency();
    let queue_capacity = worker_count.saturating_mul(1_024).max(64);
    let (jobs, job_receiver) = mpsc::sync_channel::<ManagedJob>(queue_capacity);
    let job_receiver = Arc::new(Mutex::new(job_receiver));
    let (worker_results, results) = mpsc::channel();
    let mut workers = Vec::with_capacity(worker_count);
    for worker_index in 0..worker_count {
        let service = service.clone();
        let jobs = Arc::clone(&job_receiver);
        let worker_results = worker_results.clone();
        let worker_cancel = Arc::clone(cancel);
        let worker_cancel_at_unix_ns = Arc::clone(cancel_at_unix_ns);
        workers.push(
            thread::Builder::new()
                .name(format!("kneefinder-postgres-call-{worker_index}"))
                .spawn(move || {
                    managed_call_worker(
                        service,
                        jobs,
                        worker_results,
                        segment_started,
                        latest_start_ns,
                        worker_cancel,
                        worker_cancel_at_unix_ns,
                    )
                })
                .map_err(|error| error.to_string())?,
        );
    }
    drop(worker_results);

    let mut completed_results = Vec::new();
    let mut global_index = u64::from(shard_index);
    let mut completed_schedule = true;
    loop {
        drain_worker_results(&results, accumulator.as_deref_mut(), &mut completed_results)?;
        if cancellation_reached(cancel, cancel_at_unix_ns) {
            completed_schedule = false;
            break;
        }
        let Some(intended_start_offset_ns) = intended_offset(global_index, rate)? else {
            break;
        };
        if intended_start_offset_ns >= duration_ns {
            break;
        }
        if !wait_until_offset(
            segment_started,
            intended_start_offset_ns,
            cancel,
            cancel_at_unix_ns,
        ) {
            completed_schedule = false;
            break;
        }

        let selected = select_operation(&request.operations, total_weight, global_index);
        debug_assert_eq!(selected.shard_index, shard_index);
        debug_assert_eq!(selected.shard_count, shard_count);
        let variant = OperationVariant {
            operation: selected.operation.clone(),
            arguments: selected.arguments.clone(),
        };
        let job = ManagedJob {
            operation: OperationInvocation {
                id: OperationId(global_index),
                operation: selected.operation.clone(),
                start_offset_ns: intended_start_offset_ns,
                arguments: selected.arguments.clone(),
            },
            variant,
        };
        match jobs.try_send(job) {
            Ok(()) => {}
            Err(TrySendError::Full(job)) => {
                if let Some(accumulator) = accumulator.as_deref_mut() {
                    accumulator
                        .record_unstarted_offer(&job.variant, job.operation.start_offset_ns)
                        .map_err(|error| error.to_string())?;
                }
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err("managed PostgreSQL worker queue disconnected".into());
            }
        }
        global_index = global_index
            .checked_add(u64::from(shard_count))
            .ok_or("global operation index overflow")?;
    }

    drop(jobs);
    for worker in workers {
        worker
            .join()
            .map_err(|_| "managed PostgreSQL call worker panicked")?;
    }
    drain_worker_results(&results, accumulator.as_deref_mut(), &mut completed_results)?;
    if let Some(accumulator) = accumulator {
        accumulator
            .record_batch(&completed_results)
            .map_err(|error| error.to_string())?;
    }

    let completed = completed_schedule && !cancellation_reached(cancel, cancel_at_unix_ns);
    let elapsed_ns = if completed {
        duration_ns
    } else {
        elapsed_ns(segment_started).clamp(1, duration_ns)
    };
    Ok((completed, elapsed_ns))
}

fn managed_call_worker<S: OperationService>(
    service: S,
    jobs: Arc<Mutex<mpsc::Receiver<ManagedJob>>>,
    results: Sender<WorkerResult>,
    segment_started: Instant,
    latest_start_ns: u64,
    cancel: Arc<AtomicBool>,
    cancel_at_unix_ns: Arc<AtomicU64>,
) {
    loop {
        let job = {
            let receiver = jobs.lock().expect("managed worker queue mutex poisoned");
            receiver.recv()
        };
        let Ok(job) = job else {
            return;
        };
        if cancellation_reached(&cancel, &cancel_at_unix_ns)
            || elapsed_ns(segment_started) > latest_start_ns
        {
            if results
                .send(WorkerResult::Unstarted {
                    variant: job.variant,
                    intended_start_offset_ns: job.operation.start_offset_ns,
                })
                .is_err()
            {
                return;
            }
            continue;
        }
        let actual_start_offset_ns = elapsed_ns(segment_started).max(job.operation.start_offset_ns);
        if results
            .send(WorkerResult::Completed(execute_operation(
                service.clone(),
                actual_start_offset_ns,
                job.operation,
            )))
            .is_err()
        {
            return;
        }
    }
}

fn drain_worker_results(
    results: &mpsc::Receiver<WorkerResult>,
    mut accumulator: Option<&mut PhaseAccumulator>,
    completed_results: &mut Vec<OperationResult>,
) -> Result<(), String> {
    loop {
        match results.try_recv() {
            Ok(WorkerResult::Completed(result)) => {
                if accumulator.is_some() {
                    completed_results.push(result);
                }
            }
            Ok(WorkerResult::Unstarted {
                variant,
                intended_start_offset_ns,
            }) => {
                if let Some(accumulator) = accumulator.as_deref_mut() {
                    accumulator
                        .record_unstarted_offer(&variant, intended_start_offset_ns)
                        .map_err(|error| error.to_string())?;
                }
            }
            Err(TryRecvError::Empty | TryRecvError::Disconnected) => return Ok(()),
        }
    }
}

fn empty_phase_result_at(
    request: &ManagedPhaseRequest,
    elapsed_ns: u64,
) -> Result<PhaseResult, String> {
    PhaseAccumulator::new(aggregation_plan(request)?)
        .and_then(|accumulator| accumulator.to_phase_result_at(elapsed_ns))
        .map_err(|error| error.to_string())
}

fn aggregation_plan(request: &ManagedPhaseRequest) -> Result<PhaseAggregationPlan, String> {
    Ok(PhaseAggregationPlan {
        measurement_ns: request.measurement_ns,
        bucket_count: request.bucket_count,
        histogram: request.histogram,
        variants: request
            .operations
            .iter()
            .map(|operation| OperationVariant {
                operation: operation.operation.clone(),
                arguments: operation.arguments.clone(),
            })
            .collect(),
    })
}

fn validate_managed_request(request: &ManagedPhaseRequest) -> Result<(), String> {
    match request.load {
        Load::OpenLoop {
            requests_per_second,
        } if requests_per_second.is_finite() && requests_per_second > 0.0 => {}
        Load::OpenLoop { .. } => return Err("open-loop rate must be positive and finite".into()),
        Load::ClosedLoop { .. } => return Err("only open-loop phases are supported".into()),
    }
    if request.measurement_ns == 0 {
        return Err("measurement duration must be greater than zero".into());
    }
    if request.bucket_count == 0 {
        return Err("time-bucket count must be greater than zero".into());
    }
    if request.operations.is_empty() {
        return Err("at least one operation variant is required".into());
    }
    request
        .warmup_ns
        .checked_add(request.measurement_ns)
        .and_then(|duration| duration.checked_add(request.operation_timeout_ns))
        .ok_or("phase durations overflow")?;

    let mut variants = BTreeSet::new();
    let mut total_weight = 0.0;
    for operation in &request.operations {
        validate_bound_operation(&operation.operation, &operation.arguments).map_err(|()| {
            format!(
                "unsupported PostgreSQL operation variant {:?}",
                operation.operation
            )
        })?;
        if !operation.weight.is_finite() || operation.weight <= 0.0 {
            return Err("operation weights must be positive and finite".into());
        }
        total_weight += operation.weight;
        if !total_weight.is_finite() {
            return Err("total operation weight must be finite".into());
        }
        if operation.shard_count == 0 || operation.shard_index >= operation.shard_count {
            return Err("operation shard must satisfy shard_index < shard_count".into());
        }
        if !variants.insert(OperationVariant {
            operation: operation.operation.clone(),
            arguments: operation.arguments.clone(),
        }) {
            return Err("managed phase contains a duplicate bound operation variant".into());
        }
    }
    phase_shard(&request.operations)?;
    LatencyHistogram::new(request.histogram).map_err(|error| error.to_string())?;
    Ok(())
}

fn phase_shard(operations: &[ManagedOperation]) -> Result<(u32, u32), String> {
    let first = operations
        .first()
        .ok_or("at least one operation variant is required")?;
    let shard = (first.shard_index, first.shard_count);
    if operations
        .iter()
        .any(|operation| (operation.shard_index, operation.shard_count) != shard)
    {
        return Err("every variant in one phase must use the same fixed shard".into());
    }
    Ok(shard)
}

fn select_operation(
    operations: &[ManagedOperation],
    total_weight: f64,
    global_index: u64,
) -> &ManagedOperation {
    let random = splitmix64(global_index ^ 0x504f_5354_4752_4553);
    let unit = (random >> 11) as f64 * (1.0 / ((1_u64 << 53) as f64));
    let target = unit * total_weight;
    let mut cumulative = 0.0;
    for operation in operations {
        cumulative += operation.weight;
        if target < cumulative {
            return operation;
        }
    }
    operations
        .last()
        .expect("managed request validation requires operations")
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn intended_offset(global_index: u64, rate: f64) -> Result<Option<u64>, String> {
    let offset = global_index as f64 * 1_000_000_000.0 / rate;
    if !offset.is_finite() {
        return Err("intended operation deadline overflow".into());
    }
    if offset > u64::MAX as f64 {
        return Ok(None);
    }
    Ok(Some(offset.round() as u64))
}

fn execute_operation<S: OperationService>(
    service: S,
    actual_start_offset_ns: u64,
    operation: OperationInvocation,
) -> OperationResult {
    let started = Instant::now();
    let status = match validate_bound_operation(&operation.operation, &operation.arguments) {
        Err(()) => OperationStatus::Error {
            code: Some("invalid_arguments".into()),
        },
        Ok(()) => match service.call(&operation.operation, &operation.arguments) {
            Ok(()) => OperationStatus::Ok,
            Err(error) => {
                eprintln!("PostgreSQL operation failed: {error}");
                OperationStatus::Error {
                    code: Some("postgres_error".into()),
                }
            }
        },
    };

    OperationResult {
        id: operation.id,
        operation: operation.operation,
        arguments: operation.arguments,
        intended_start_offset_ns: operation.start_offset_ns,
        actual_start_offset_ns,
        client_latency_ns: elapsed_ns(started).max(1),
        status,
    }
}

fn validate_bound_operation(
    operation: &str,
    arguments: &BTreeMap<String, ArgumentValue>,
) -> Result<(), ()> {
    match operation {
        "lookup"
            if matches!(
                arguments.get("account"),
                Some(ArgumentValue::Integer(1..=4))
            ) =>
        {
            Ok(())
        }
        "transfer"
            if matches!(
                arguments.get("route"),
                Some(ArgumentValue::String(route)) if matches!(route.as_str(), "hot" | "cold")
            ) =>
        {
            Ok(())
        }
        _ => Err(()),
    }
}

#[derive(Clone)]
struct PostgresService {
    pool: Arc<PostgresPool>,
    lock_hold_seconds: f64,
    connections: usize,
}

struct PostgresPool {
    clients: Mutex<Vec<Client>>,
    available: Condvar,
}

impl PostgresService {
    fn new(config: AdapterConfig) -> Result<Self, Box<dyn Error>> {
        if config.connections == 0 {
            return Err("connections must be nonzero".into());
        }
        if config.lock_hold_ms == 0 || config.lock_hold_ms > maximum_lock_hold_ms() {
            return Err(format!(
                "lock_hold_ms must be between 1 and {}",
                maximum_lock_hold_ms()
            )
            .into());
        }

        let mut clients = Vec::with_capacity(config.connections);
        for _ in 0..config.connections {
            clients.push(Client::connect(&config.database_url, NoTls)?);
        }
        initialize_schema(&mut clients[0])?;
        eprintln!(
            "PostgreSQL demo: {} connections; hot-row lock held {} ms; expected knee near {:.0} req/s",
            config.connections,
            config.lock_hold_ms,
            theoretical_knee(config.lock_hold_ms)
        );
        Ok(Self {
            pool: Arc::new(PostgresPool {
                clients: Mutex::new(clients),
                available: Condvar::new(),
            }),
            lock_hold_seconds: config.lock_hold_ms as f64 / 1_000.0,
            connections: config.connections,
        })
    }

    fn call(
        &self,
        operation: &str,
        arguments: &std::collections::BTreeMap<String, ArgumentValue>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.pool
            .with_client(|client| match (operation, arguments) {
                ("lookup", arguments) => {
                    let Some(ArgumentValue::Integer(account)) = arguments.get("account") else {
                        return Err("lookup requires integer argument account".into());
                    };
                    client.query_one(
                        "SELECT balance FROM kneefinder_accounts WHERE id = $1",
                        &[account],
                    )?;
                    Ok(())
                }
                ("transfer", arguments)
                    if arguments.get("route") == Some(&ArgumentValue::String("hot".into())) =>
                {
                    transfer(client, 1, 2, self.lock_hold_seconds)
                }
                ("transfer", arguments)
                    if arguments.get("route") == Some(&ArgumentValue::String("cold".into())) =>
                {
                    transfer(client, 3, 4, self.lock_hold_seconds)
                }
                _ => Err(format!("unsupported operation variant {operation:?}").into()),
            })
    }
}

impl OperationService for PostgresService {
    fn concurrency(&self) -> usize {
        self.connections
    }

    fn call(
        &self,
        operation: &str,
        arguments: &BTreeMap<String, ArgumentValue>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        PostgresService::call(self, operation, arguments)
    }
}

impl PostgresPool {
    fn with_client<T>(
        &self,
        call: impl FnOnce(&mut Client) -> Result<T, Box<dyn Error + Send + Sync>>,
    ) -> Result<T, Box<dyn Error + Send + Sync>> {
        let mut clients = self
            .clients
            .lock()
            .map_err(|_| "PostgreSQL pool mutex poisoned")?;
        while clients.is_empty() {
            clients = self
                .available
                .wait(clients)
                .map_err(|_| "PostgreSQL pool mutex poisoned")?;
        }
        let mut client = clients.pop().expect("pool checked as non-empty");
        drop(clients);

        let result = call(&mut client);
        let mut clients = self
            .clients
            .lock()
            .map_err(|_| "PostgreSQL pool mutex poisoned")?;
        clients.push(client);
        self.available.notify_one();
        result
    }
}

fn initialize_schema(client: &mut Client) -> Result<(), postgres::Error> {
    let mut transaction = client.transaction()?;
    transaction.query_one("SELECT pg_advisory_xact_lock(7046029254386353131)", &[])?;
    transaction.batch_execute(
        "CREATE TABLE IF NOT EXISTS kneefinder_accounts (
             id BIGINT PRIMARY KEY,
             balance BIGINT NOT NULL
         );
         INSERT INTO kneefinder_accounts (id, balance)
         SELECT id, 1000000 FROM generate_series(1, 4) AS id
         ON CONFLICT (id) DO NOTHING;",
    )?;
    transaction.commit()
}

fn transfer(
    client: &mut Client,
    from: i64,
    to: i64,
    lock_hold_seconds: f64,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut transaction = client.transaction()?;
    transaction.query_one(
        "UPDATE kneefinder_accounts SET balance = balance - 1 WHERE id = $1 RETURNING balance",
        &[&from],
    )?;
    transaction.query_one("SELECT pg_sleep($1)", &[&lock_hold_seconds])?;
    transaction.execute(
        "UPDATE kneefinder_accounts SET balance = balance + 1 WHERE id = $1",
        &[&to],
    )?;
    transaction.commit()?;
    Ok(())
}

fn theoretical_knee(lock_hold_ms: u64) -> f64 {
    1_000.0 / lock_hold_ms as f64 / 0.16
}

fn wait_until_unix(
    deadline_unix_ns: u64,
    cancel: &AtomicBool,
    cancel_at_unix_ns: &AtomicU64,
) -> bool {
    loop {
        if cancellation_reached(cancel, cancel_at_unix_ns) {
            return false;
        }
        let remaining = deadline_unix_ns.saturating_sub(unix_now_ns());
        if remaining == 0 {
            return true;
        }
        wait_briefly(remaining);
    }
}

fn wait_until_offset(
    start: Instant,
    offset_ns: u64,
    cancel: &AtomicBool,
    cancel_at_unix_ns: &AtomicU64,
) -> bool {
    let Some(deadline) = start.checked_add(Duration::from_nanos(offset_ns)) else {
        return false;
    };
    loop {
        if cancellation_reached(cancel, cancel_at_unix_ns) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return true;
        }
        wait_briefly(remaining.as_nanos().min(u64::MAX as u128) as u64);
    }
}

fn wait_briefly(remaining_ns: u64) {
    const SPIN_WINDOW_NS: u64 = 100_000;
    if remaining_ns > SPIN_WINDOW_NS {
        thread::sleep(Duration::from_nanos(remaining_ns - SPIN_WINDOW_NS));
    } else {
        std::hint::spin_loop();
    }
}

fn cancellation_reached(cancel: &AtomicBool, cancel_at_unix_ns: &AtomicU64) -> bool {
    if cancel.load(Ordering::Acquire) {
        return true;
    }
    let cutoff = cancel_at_unix_ns.load(Ordering::Acquire);
    cutoff != 0 && unix_now_ns() >= cutoff
}

fn managed_elapsed_at_cutoff(
    request: &ManagedPhaseRequest,
    phase_start_unix_ns: u64,
    cancel_at_unix_ns: &AtomicU64,
    fallback: u64,
) -> u64 {
    let cutoff = cancel_at_unix_ns.load(Ordering::Acquire);
    if cutoff == 0 {
        return fallback.max(1).min(request.measurement_ns);
    }
    let measurement_start = phase_start_unix_ns.saturating_add(request.warmup_ns);
    cutoff
        .saturating_sub(measurement_start)
        .max(1)
        .min(request.measurement_ns)
}

fn elapsed_ns(start: Instant) -> u64 {
    start.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn message_phase_id(message: &ControllerMessage) -> Option<PhaseId> {
    match message {
        ControllerMessage::Initialize { .. } | ControllerMessage::Shutdown => None,
        ControllerMessage::PreparePhase { phase_id, .. }
        | ControllerMessage::StartPhase { phase_id, .. }
        | ControllerMessage::CancelPhase { phase_id, .. } => Some(*phase_id),
    }
}

fn write_error(
    output: &mut impl Write,
    phase_id: Option<PhaseId>,
    code: &str,
    message: String,
    retryable: bool,
) -> Result<(), Box<dyn Error>> {
    write_message(
        output,
        &AdapterMessage::Error {
            phase_id,
            code: code.into(),
            message,
            retryable,
        },
    )
}

fn write_message(writer: &mut impl Write, message: &AdapterMessage) -> Result<(), Box<dyn Error>> {
    serde_json::to_writer(&mut *writer, message)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kneefinder::histogram::HistogramDecodeLimits;

    #[derive(Clone)]
    struct FakeService {
        calls: Arc<AtomicU64>,
    }

    impl OperationService for FakeService {
        fn concurrency(&self) -> usize {
            4
        }

        fn call(
            &self,
            _operation: &str,
            _arguments: &BTreeMap<String, ArgumentValue>,
        ) -> Result<(), Box<dyn Error + Send + Sync>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn arguments(argument: &str, value: ArgumentValue) -> BTreeMap<String, ArgumentValue> {
        BTreeMap::from([(argument.into(), value)])
    }

    fn managed_request(shard_index: u32, shard_count: u32) -> ManagedPhaseRequest {
        ManagedPhaseRequest {
            warmup_ns: 4_000_000,
            measurement_ns: 20_000_000,
            operation_timeout_ns: 20_000_000,
            load: Load::OpenLoop {
                requests_per_second: 1_000.0,
            },
            operations: vec![
                ManagedOperation {
                    operation: "lookup".into(),
                    arguments: arguments("account", ArgumentValue::Integer(1)),
                    weight: 4.0,
                    shard_index,
                    shard_count,
                },
                ManagedOperation {
                    operation: "transfer".into(),
                    arguments: arguments("route", ArgumentValue::String("hot".into())),
                    weight: 1.0,
                    shard_index,
                    shard_count,
                },
            ],
            bucket_count: 4,
            histogram: kneefinder::protocol::HistogramSpec {
                lowest_discernible_ns: 1,
                highest_trackable_ns: 100_000_000,
                significant_figures: 3,
            },
        }
    }

    #[test]
    fn validates_only_advertised_postgres_variants() {
        assert!(
            validate_bound_operation("lookup", &arguments("account", ArgumentValue::Integer(1)))
                .is_ok()
        );
        assert!(
            validate_bound_operation(
                "transfer",
                &arguments("route", ArgumentValue::String("hot".into()))
            )
            .is_ok()
        );
        assert!(
            validate_bound_operation("lookup", &arguments("account", ArgumentValue::Integer(9)))
                .is_err()
        );
        assert!(
            validate_bound_operation(
                "transfer",
                &arguments("route", ArgumentValue::String("unknown".into()))
            )
            .is_err()
        );
    }

    #[test]
    fn advertises_only_the_managed_open_loop_contract() {
        let AdapterMessage::Ready { capabilities, .. } = ready_message() else {
            panic!("ready_message must return Ready");
        };

        assert!(capabilities.adapter_managed_phases);
        assert_eq!(capabilities.load_models, [LoadModel::OpenLoop]);
        assert_eq!(
            capabilities.histogram_encodings,
            [HistogramEncoding::HdrV2Base64]
        );
    }

    #[test]
    fn managed_phase_preserves_fixed_shards_and_mergeable_counts() {
        let request = managed_request(1, 2);
        let calls = Arc::new(AtomicU64::new(0));
        let service = FakeService {
            calls: Arc::clone(&calls),
        };
        let (completion, result) = run_managed_phase(
            service,
            &request,
            unix_now_ns(),
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap();

        assert_eq!(completion, PhaseCompletion::Completed);
        assert_eq!(result.offered, 10);
        assert_eq!(result.started, 10);
        assert_eq!(result.completed, 10);
        assert_eq!(result.successful, 10);
        assert_eq!(
            result
                .per_operation
                .iter()
                .map(|item| item.offered)
                .sum::<u64>(),
            10
        );
        assert_eq!(
            result
                .time_buckets
                .iter()
                .map(|bucket| bucket.offered)
                .sum::<u64>(),
            10
        );
        assert_eq!(calls.load(Ordering::Relaxed), 12);

        let histogram = LatencyHistogram::decode(
            &result.client_latency,
            request.histogram,
            HistogramDecodeLimits::default(),
        )
        .unwrap();
        assert_eq!(histogram.len(), result.completed);

        let mut merged = PhaseAccumulator::new(aggregation_plan(&request).unwrap()).unwrap();
        merged.merge_phase_result(&result).unwrap();
        let report = merged.finish(1_000.0).unwrap();
        assert_eq!(report.offered_count, 10);
        assert_eq!(report.stats.overall.attempts, 10);
    }

    #[test]
    fn managed_phase_honors_the_coordinator_cancellation_cutoff() {
        let mut request = managed_request(0, 1);
        request.warmup_ns = 0;
        request.measurement_ns = 50_000_000;
        let phase_start_unix_ns = unix_now_ns();
        let cutoff = phase_start_unix_ns.saturating_add(5_000_000);
        let (completion, result) = run_managed_phase(
            FakeService {
                calls: Arc::new(AtomicU64::new(0)),
            },
            &request,
            phase_start_unix_ns,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicU64::new(cutoff)),
        )
        .unwrap();

        assert_eq!(completion, PhaseCompletion::Cancelled);
        assert_eq!(result.elapsed_ns, 5_000_000);
        assert!(result.offered < 50);
        assert_eq!(result.started, result.completed);
    }
}
