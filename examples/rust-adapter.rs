//! Minimal standalone adapter-managed kneefinder adapter.
//!
//! Replace `call_target` with a synchronous call into the system you want to
//! measure. The surrounding runtime preserves offered-load timing, responds to
//! the prepare/start barrier, supports a coordinator-owned cancellation cutoff,
//! and returns mergeable HdrHistogram V2 phase summaries.

use std::{
    collections::BTreeSet,
    error::Error,
    hint::black_box,
    io::{self, BufRead, Write},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Sender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use kneefinder::{
    histogram::LatencyHistogram,
    protocol::{
        AdapterIdentity, AdapterMessage, Capabilities, ControllerMessage, HistogramEncoding, Load,
        LoadModel, ManagedOperation, ManagedPhaseRequest, OperationDescriptor, OperationId,
        OperationKind, OperationResult, OperationStatus, PROTOCOL_VERSION, PhaseCompletion,
        PhaseId, PhaseResult,
    },
    stats::{OperationVariant, PhaseAccumulator, PhaseAggregationPlan},
};

const OPERATION: &str = "noop";

fn main() -> Result<(), Box<dyn Error>> {
    let (sender, events) = mpsc::channel();
    let input_sender = sender.clone();
    thread::Builder::new()
        .name("kneefinder-example-input".into())
        .spawn(move || read_input(input_sender))?;

    let mut output = io::BufWriter::new(io::stdout().lock());
    let mut initialized = false;
    let mut prepared: Option<PreparedPhase> = None;
    let mut active: Option<ActiveWorker> = None;

    while let Ok(event) = events.recv() {
        match event {
            RuntimeEvent::Controller(message) => match message {
                ControllerMessage::Initialize {
                    protocol_version, ..
                } if protocol_version == PROTOCOL_VERSION
                    && !initialized
                    && prepared.is_none()
                    && active.is_none() =>
                {
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
                        Err(message) => write_error(
                            &mut output,
                            Some(phase_id),
                            "invalid_phase",
                            message,
                            false,
                        )?,
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
                    active = Some(start_worker(phase, phase_start_unix_ns, sender.clone())?);
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
                        write_message(
                            &mut output,
                            &AdapterMessage::PhaseComplete {
                                phase_id,
                                completion: PhaseCompletion::Cancelled,
                                result: empty_phase_result(&phase.request)?,
                            },
                        )?;
                    }
                }
                ControllerMessage::Shutdown => {
                    stop_worker(&mut active);
                    break;
                }
                message => write_error(
                    &mut output,
                    message_phase_id(&message),
                    "invalid_state",
                    "message is not valid in the adapter's current state".into(),
                    false,
                )?,
            },
            RuntimeEvent::Finished {
                phase_id,
                completion,
                result,
            } if active
                .as_ref()
                .is_some_and(|worker| worker.phase_id == phase_id) =>
            {
                join_worker(&mut active);
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
            RuntimeEvent::Finished { .. } => write_error(
                &mut output,
                None,
                "stale_worker",
                "worker completed for a phase that is no longer active".into(),
                false,
            )?,
            RuntimeEvent::InputFailed(message) => {
                write_error(&mut output, None, "invalid_message", message, false)?;
                break;
            }
            RuntimeEvent::InputClosed => {
                stop_worker(&mut active);
                break;
            }
        }
    }
    Ok(())
}

fn ready_message() -> AdapterMessage {
    AdapterMessage::Ready {
        protocol_version: PROTOCOL_VERSION,
        identity: AdapterIdentity {
            name: "rust-adapter-example".into(),
            version: Some(env!("CARGO_PKG_VERSION").into()),
        },
        capabilities: Capabilities {
            adapter_managed_phases: true,
            load_models: vec![LoadModel::OpenLoop],
            histogram_encodings: vec![HistogramEncoding::HdrV2Base64],
        },
        operations: vec![OperationDescriptor {
            name: OPERATION.into(),
            description: Some("execute a synchronous no-op".into()),
            kind: OperationKind::Other,
            enabled_by_default: true,
            default_weight: 1.0,
            arguments: Vec::new(),
        }],
    }
}

#[derive(Debug)]
enum RuntimeEvent {
    Controller(ControllerMessage),
    Finished {
        phase_id: PhaseId,
        completion: PhaseCompletion,
        result: Result<PhaseResult, String>,
    },
    InputFailed(String),
    InputClosed,
}

fn read_input(sender: Sender<RuntimeEvent>) {
    for line in io::stdin().lock().lines() {
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

struct PreparedPhase {
    id: PhaseId,
    request: ManagedPhaseRequest,
}

impl PreparedPhase {
    fn new(id: PhaseId, request: ManagedPhaseRequest) -> Result<Self, String> {
        validate_request(&request)?;
        PhaseAccumulator::new(aggregation_plan(&request)?).map_err(|error| error.to_string())?;
        Ok(Self { id, request })
    }
}

struct ActiveWorker {
    phase_id: PhaseId,
    cancel: Arc<AtomicBool>,
    cancel_at_unix_ns: Arc<AtomicU64>,
    handle: JoinHandle<()>,
}

fn start_worker(
    phase: PreparedPhase,
    phase_start_unix_ns: u64,
    sender: Sender<RuntimeEvent>,
) -> Result<ActiveWorker, io::Error> {
    let phase_id = phase.id;
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_at_unix_ns = Arc::new(AtomicU64::new(0));
    let worker_cancel = Arc::clone(&cancel);
    let worker_cutoff = Arc::clone(&cancel_at_unix_ns);
    let handle = thread::Builder::new()
        .name(format!("kneefinder-example-phase-{}", phase_id.0))
        .spawn(move || {
            let (completion, result) = match run_phase(
                &phase.request,
                phase_start_unix_ns,
                &worker_cancel,
                &worker_cutoff,
            ) {
                Ok((completion, result)) => (completion, Ok(result)),
                Err(error) => (PhaseCompletion::Cancelled, Err(error)),
            };
            let _ = sender.send(RuntimeEvent::Finished {
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

fn stop_worker(active: &mut Option<ActiveWorker>) {
    if let Some(worker) = active.take() {
        worker.cancel.store(true, Ordering::Release);
        let _ = worker.handle.join();
    }
}

fn join_worker(active: &mut Option<ActiveWorker>) {
    if let Some(worker) = active.take() {
        let _ = worker.handle.join();
    }
}

fn run_phase(
    request: &ManagedPhaseRequest,
    phase_start_unix_ns: u64,
    cancel: &AtomicBool,
    cancel_at_unix_ns: &AtomicU64,
) -> Result<(PhaseCompletion, PhaseResult), String> {
    validate_request(request)?;
    let rate = match request.load {
        Load::OpenLoop {
            requests_per_second,
        } => requests_per_second,
        Load::ClosedLoop { .. } => return Err("only open-loop phases are supported".into()),
    };

    if !wait_until_unix(phase_start_unix_ns, cancel, cancel_at_unix_ns) {
        return Ok((PhaseCompletion::Cancelled, empty_phase_result(request)?));
    }
    if request.warmup_ns > 0
        && !drive_segment(
            request,
            rate,
            request.warmup_ns,
            cancel,
            cancel_at_unix_ns,
            None,
        )?
        .0
    {
        return Ok((PhaseCompletion::Cancelled, empty_phase_result(request)?));
    }

    let mut accumulator =
        PhaseAccumulator::new(aggregation_plan(request)?).map_err(|error| error.to_string())?;
    let (completed, fallback_elapsed_ns) = drive_segment(
        request,
        rate,
        request.measurement_ns,
        cancel,
        cancel_at_unix_ns,
        Some(&mut accumulator),
    )?;
    let elapsed_ns = if completed {
        request.measurement_ns
    } else {
        elapsed_at_cutoff(
            request,
            phase_start_unix_ns,
            cancel_at_unix_ns,
            fallback_elapsed_ns,
        )
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

fn drive_segment(
    request: &ManagedPhaseRequest,
    rate: f64,
    duration_ns: u64,
    cancel: &AtomicBool,
    cancel_at_unix_ns: &AtomicU64,
    mut accumulator: Option<&mut PhaseAccumulator>,
) -> Result<(bool, u64), String> {
    let (shard_index, shard_count) = phase_shard(&request.operations)?;
    let started = Instant::now();
    let mut global_index = u64::from(shard_index);
    loop {
        if cancellation_reached(cancel, cancel_at_unix_ns) {
            return Ok((false, elapsed_ns(started).min(duration_ns)));
        }
        let intended_start_offset_ns = intended_offset(global_index, rate)?;
        if intended_start_offset_ns >= duration_ns {
            return Ok((true, duration_ns));
        }
        if !wait_until(started, intended_start_offset_ns, cancel, cancel_at_unix_ns) {
            return Ok((false, elapsed_ns(started).min(duration_ns)));
        }

        let operation = &request.operations[0];
        let actual_start_offset_ns = elapsed_ns(started).max(intended_start_offset_ns);
        let call_started = Instant::now();
        let status = match call_target(operation) {
            Ok(()) => OperationStatus::Ok,
            Err(code) => OperationStatus::Error {
                code: Some(code.into()),
            },
        };
        let result = OperationResult {
            id: OperationId(global_index),
            operation: operation.operation.clone(),
            arguments: operation.arguments.clone(),
            intended_start_offset_ns,
            actual_start_offset_ns,
            client_latency_ns: elapsed_ns(call_started).max(1),
            status,
        };
        if let Some(accumulator) = accumulator.as_deref_mut() {
            accumulator
                .record(&result)
                .map_err(|error| error.to_string())?;
        }
        global_index = global_index
            .checked_add(u64::from(shard_count))
            .ok_or("global operation index overflow")?;
    }
}

/// Replace this function with a native synchronous client call. Return stable,
/// low-cardinality error codes so kneefinder can group failures.
fn call_target(operation: &ManagedOperation) -> Result<(), &'static str> {
    black_box((&operation.operation, &operation.arguments));
    Ok(())
}

fn validate_request(request: &ManagedPhaseRequest) -> Result<(), String> {
    match request.load {
        Load::OpenLoop {
            requests_per_second,
        } if requests_per_second.is_finite() && requests_per_second > 0.0 => {}
        Load::OpenLoop { .. } => return Err("open-loop rate must be positive and finite".into()),
        Load::ClosedLoop { .. } => return Err("only open-loop phases are supported".into()),
    }
    if request.measurement_ns == 0 || request.bucket_count == 0 {
        return Err("measurement duration and time-bucket count must be non-zero".into());
    }
    request
        .warmup_ns
        .checked_add(request.measurement_ns)
        .and_then(|duration| duration.checked_add(request.operation_timeout_ns))
        .ok_or("phase durations overflow")?;
    let mut variants = BTreeSet::new();
    for operation in &request.operations {
        if operation.operation != OPERATION || !operation.arguments.is_empty() {
            return Err("this example supports only noop with no arguments".into());
        }
        if !operation.weight.is_finite() || operation.weight <= 0.0 {
            return Err("operation weights must be positive and finite".into());
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
    if request.operations.len() != 1 {
        return Err("this minimal example requires exactly one operation variant".into());
    }
    phase_shard(&request.operations)?;
    LatencyHistogram::new(request.histogram).map_err(|error| error.to_string())?;
    Ok(())
}

fn phase_shard(operations: &[ManagedOperation]) -> Result<(u32, u32), String> {
    let first = operations
        .first()
        .ok_or("one operation variant is required")?;
    let shard = (first.shard_index, first.shard_count);
    if operations
        .iter()
        .any(|operation| (operation.shard_index, operation.shard_count) != shard)
    {
        return Err("every variant in one phase must use the same fixed shard".into());
    }
    Ok(shard)
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

fn empty_phase_result(request: &ManagedPhaseRequest) -> Result<PhaseResult, String> {
    PhaseAccumulator::new(aggregation_plan(request)?)
        .and_then(|accumulator| accumulator.to_phase_result_at(1))
        .map_err(|error| error.to_string())
}

fn intended_offset(global_index: u64, rate: f64) -> Result<u64, String> {
    let offset = global_index as f64 * 1_000_000_000.0 / rate;
    if !offset.is_finite() || offset > u64::MAX as f64 {
        return Err("intended operation deadline overflow".into());
    }
    Ok(offset.round() as u64)
}

fn wait_until(
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
        if remaining > Duration::from_millis(1) {
            thread::sleep(remaining - Duration::from_millis(1));
        } else {
            std::hint::spin_loop();
        }
    }
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
        if remaining > 1_000_000 {
            thread::sleep(Duration::from_nanos(remaining - 1_000_000));
        } else {
            std::hint::spin_loop();
        }
    }
}

fn cancellation_reached(cancel: &AtomicBool, cancel_at_unix_ns: &AtomicU64) -> bool {
    if cancel.load(Ordering::Acquire) {
        return true;
    }
    let cutoff = cancel_at_unix_ns.load(Ordering::Acquire);
    cutoff != 0 && unix_now_ns() >= cutoff
}

fn elapsed_at_cutoff(
    request: &ManagedPhaseRequest,
    phase_start_unix_ns: u64,
    cancel_at_unix_ns: &AtomicU64,
    fallback: u64,
) -> u64 {
    let cutoff = cancel_at_unix_ns.load(Ordering::Acquire);
    if cutoff == 0 {
        return fallback.max(1).min(request.measurement_ns);
    }
    cutoff
        .saturating_sub(phase_start_unix_ns.saturating_add(request.warmup_ns))
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

fn write_message(output: &mut impl Write, message: &AdapterMessage) -> Result<(), Box<dyn Error>> {
    serde_json::to_writer(&mut *output, message)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}
