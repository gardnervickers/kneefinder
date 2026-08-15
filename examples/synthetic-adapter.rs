//! Deterministic high-throughput adapter used for protocol conformance and
//! load-generator capacity characterization.
//!
//! The target operation is a CPU no-op. Adapter-managed phases use one fixed
//! shard per adapter process, spin to intended deadlines, and aggregate results
//! locally into mergeable HdrHistogram V2 payloads.

use std::{
    collections::{BTreeMap, BTreeSet},
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
        AdapterIdentity, AdapterMessage, ArgumentKind, ArgumentValue, Capabilities,
        ControllerMessage, HistogramEncoding, Load, LoadModel, ManagedOperation,
        ManagedPhaseRequest, OperationArgument, OperationDescriptor, OperationId,
        OperationInvocation, OperationKind, OperationResult, OperationStatus, PROTOCOL_VERSION,
        PhaseCompletion, PhaseId, PhaseResult,
    },
    stats::{OperationVariant, PhaseAccumulator, PhaseAggregationPlan},
};

const OPERATION: &str = "noop";
const CLASS_ARGUMENT: &str = "class";
const OUTCOME_ARGUMENT: &str = "outcome";
const ERROR_CODE: &str = "synthetic_error";

fn main() -> Result<(), Box<dyn Error>> {
    let (sender, events) = mpsc::channel();
    let input_sender = sender.clone();
    thread::Builder::new()
        .name("kneefinder-synthetic-input".into())
        .spawn(move || read_input(input_sender))?;

    let stdout = io::stdout();
    let mut output = io::BufWriter::new(stdout.lock());
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
                    active = Some(start_managed_worker(
                        phase,
                        phase_start_unix_ns,
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
                    // Cancellation is idempotent. A late cancellation can race
                    // with a completion already queued by the worker.
                }
                ControllerMessage::Shutdown => {
                    if let Some(worker) = active.take() {
                        worker.cancel.store(true, Ordering::Release);
                        let _ = worker.handle.join();
                    }
                    break;
                }
                message => {
                    write_error(
                        &mut output,
                        message_phase_id(&message),
                        "invalid_state",
                        "message is not valid in the adapter's current state".into(),
                        false,
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
                write_error(&mut output, None, "invalid_message", message, false)?;
                break;
            }
            RuntimeEvent::InputClosed => {
                if let Some(worker) = active.take() {
                    worker.cancel.store(true, Ordering::Release);
                    let _ = worker.handle.join();
                }
                break;
            }
        }
    }
    Ok(())
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

fn ready_message() -> AdapterMessage {
    AdapterMessage::Ready {
        protocol_version: PROTOCOL_VERSION,
        identity: AdapterIdentity {
            name: "kneefinder-synthetic-adapter".into(),
            version: Some(env!("CARGO_PKG_VERSION").into()),
        },
        capabilities: Capabilities {
            adapter_managed_phases: true,
            load_models: vec![LoadModel::OpenLoop],
            histogram_encodings: vec![HistogramEncoding::HdrV2Base64],
        },
        operations: vec![OperationDescriptor {
            name: OPERATION.into(),
            description: Some("execute a deterministic CPU no-op".into()),
            kind: OperationKind::Other,
            enabled_by_default: true,
            default_weight: 1.0,
            arguments: vec![
                OperationArgument {
                    name: CLASS_ARGUMENT.into(),
                    description: Some("stable variant used to exercise grouped statistics".into()),
                    kind: ArgumentKind::Enum,
                    values: ["a", "b", "c", "d"].map(String::from).into(),
                    required: true,
                    default: Some(ArgumentValue::String("a".into())),
                },
                OperationArgument {
                    name: OUTCOME_ARGUMENT.into(),
                    description: Some("deterministic terminal outcome".into()),
                    kind: ArgumentKind::Enum,
                    values: ["ok", "error", "timeout"].map(String::from).into(),
                    required: true,
                    default: Some(ArgumentValue::String("ok".into())),
                },
            ],
        }],
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
    sender: Sender<RuntimeEvent>,
) -> Result<ActiveWorker, io::Error> {
    let phase_id = phase.id;
    let cancel = Arc::new(AtomicBool::new(false));
    let cancel_at_unix_ns = Arc::new(AtomicU64::new(0));
    let worker_cancel = Arc::clone(&cancel);
    let worker_cancel_at_unix_ns = Arc::clone(&cancel_at_unix_ns);
    let handle = thread::Builder::new()
        .name(format!("kneefinder-synthetic-managed-{}", phase_id.0))
        .spawn(move || {
            let (completion, result) = match run_managed_phase_with_cutoff(
                &phase.request,
                phase_start_unix_ns,
                &worker_cancel,
                &worker_cancel_at_unix_ns,
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

#[cfg(test)]
fn run_managed_phase(
    request: &ManagedPhaseRequest,
    phase_start_unix_ns: u64,
    cancel: &AtomicBool,
) -> Result<(PhaseCompletion, PhaseResult), String> {
    run_managed_phase_with_cutoff(request, phase_start_unix_ns, cancel, &AtomicU64::new(0))
}

fn run_managed_phase_with_cutoff(
    request: &ManagedPhaseRequest,
    phase_start_unix_ns: u64,
    cancel: &AtomicBool,
    cancel_at_unix_ns: &AtomicU64,
) -> Result<(PhaseCompletion, PhaseResult), String> {
    validate_managed_request(request)?;
    let rate = match request.load {
        Load::OpenLoop {
            requests_per_second,
        } => requests_per_second,
        Load::ClosedLoop { .. } => return Err("only open-loop phases are supported".into()),
    };

    if !spin_until_unix(phase_start_unix_ns, cancel, cancel_at_unix_ns) {
        return Ok((
            PhaseCompletion::Cancelled,
            empty_phase_result_at(request, 1)?,
        ));
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
        return Ok((
            PhaseCompletion::Cancelled,
            empty_phase_result_at(request, 1)?,
        ));
    }

    let mut accumulator =
        PhaseAccumulator::new(aggregation_plan(request)?).map_err(|error| error.to_string())?;
    let (completed, elapsed_ns) = drive_segment(
        request,
        rate,
        request.measurement_ns,
        cancel,
        cancel_at_unix_ns,
        Some(&mut accumulator),
    )?;
    let elapsed_ns = if completed {
        elapsed_ns
    } else {
        managed_elapsed_at_cutoff(request, phase_start_unix_ns, cancel_at_unix_ns, elapsed_ns)
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
    let total_weight = request
        .operations
        .iter()
        .map(|operation| operation.weight)
        .sum();
    let variants = request
        .operations
        .iter()
        .map(|operation| OperationVariant {
            operation: operation.operation.clone(),
            arguments: operation.arguments.clone(),
        })
        .collect::<Vec<_>>();
    let segment_started = Instant::now();
    let latest_completion_ns = duration_ns
        .checked_add(request.operation_timeout_ns)
        .ok_or("phase duration and operation timeout overflow")?;
    let mut global_index = u64::from(shard_index);

    loop {
        if cancellation_reached(cancel, cancel_at_unix_ns) {
            return Ok((false, elapsed_ns(segment_started).min(duration_ns)));
        }
        let Some(intended_offset_ns) = intended_offset(global_index, rate)? else {
            return Ok((true, duration_ns));
        };
        if intended_offset_ns >= duration_ns {
            return Ok((true, duration_ns));
        }
        if elapsed_ns(segment_started) > latest_completion_ns {
            // Every remaining deadline is already in the past. Preserve the
            // complete offered schedule without waiting on those missed calls.
            let completed = match accumulator.as_deref_mut() {
                Some(accumulator) => record_remaining_unstarted_offers(
                    request,
                    rate,
                    duration_ns,
                    total_weight,
                    shard_count,
                    global_index,
                    cancel,
                    cancel_at_unix_ns,
                    accumulator,
                    &variants,
                ),
                None => Ok(true),
            }?;
            return Ok((completed, elapsed_ns(segment_started).min(duration_ns)));
        }
        if !spin_until(
            segment_started,
            intended_offset_ns,
            cancel,
            cancel_at_unix_ns,
        ) {
            return Ok((false, elapsed_ns(segment_started).min(duration_ns)));
        }

        let selected = select_operation(&request.operations, total_weight, global_index);
        debug_assert_eq!(selected.shard_index, shard_index);
        debug_assert_eq!(selected.shard_count, shard_count);
        let actual_start_offset_ns = elapsed_ns(segment_started).max(intended_offset_ns);
        let operation = OperationInvocation {
            id: OperationId(global_index),
            operation: selected.operation.clone(),
            start_offset_ns: intended_offset_ns,
            arguments: selected.arguments.clone(),
        };
        let result = execute_operation(
            operation,
            actual_start_offset_ns,
            request.operation_timeout_ns,
        );
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

#[allow(clippy::too_many_arguments)]
fn record_remaining_unstarted_offers(
    request: &ManagedPhaseRequest,
    rate: f64,
    duration_ns: u64,
    total_weight: f64,
    shard_count: u32,
    mut global_index: u64,
    cancel: &AtomicBool,
    cancel_at_unix_ns: &AtomicU64,
    accumulator: &mut PhaseAccumulator,
    variants: &[OperationVariant],
) -> Result<bool, String> {
    loop {
        if cancellation_reached(cancel, cancel_at_unix_ns) {
            return Ok(false);
        }
        let Some(intended_offset_ns) = intended_offset(global_index, rate)? else {
            return Ok(true);
        };
        if intended_offset_ns >= duration_ns {
            return Ok(true);
        }
        let selected = select_operation_index(&request.operations, total_weight, global_index);
        accumulator
            .record_unstarted_offer(&variants[selected], intended_offset_ns)
            .map_err(|error| error.to_string())?;
        global_index = global_index
            .checked_add(u64::from(shard_count))
            .ok_or("global operation index overflow")?;
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
    let rate = match request.load {
        Load::OpenLoop {
            requests_per_second,
        } if requests_per_second.is_finite() && requests_per_second > 0.0 => requests_per_second,
        Load::OpenLoop { .. } => return Err("open-loop rate must be positive and finite".into()),
        Load::ClosedLoop { .. } => return Err("only open-loop phases are supported".into()),
    };
    let _ = rate;
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
        validate_bound_operation(&operation.operation, &operation.arguments)?;
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

fn validate_bound_operation(
    operation: &str,
    arguments: &BTreeMap<String, ArgumentValue>,
) -> Result<(), String> {
    if operation != OPERATION {
        return Err(format!("unsupported operation {operation:?}"));
    }
    if arguments.len() != 2
        || !matches!(
            arguments.get(CLASS_ARGUMENT),
            Some(ArgumentValue::String(class)) if matches!(class.as_str(), "a" | "b" | "c" | "d")
        )
        || !matches!(
            arguments.get(OUTCOME_ARGUMENT),
            Some(ArgumentValue::String(outcome)) if matches!(outcome.as_str(), "ok" | "error" | "timeout")
        )
    {
        return Err("noop requires class=a|b|c|d and outcome=ok|error|timeout".into());
    }
    Ok(())
}

fn execute_operation(
    operation: OperationInvocation,
    actual_start_offset_ns: u64,
    operation_timeout_ns: u64,
) -> OperationResult {
    let started = Instant::now();
    let status = match validate_bound_operation(&operation.operation, &operation.arguments) {
        Ok(()) => match operation.arguments.get(OUTCOME_ARGUMENT) {
            Some(ArgumentValue::String(outcome)) if outcome == "error" => OperationStatus::Error {
                code: Some(ERROR_CODE.into()),
            },
            Some(ArgumentValue::String(outcome)) if outcome == "timeout" => {
                OperationStatus::Timeout
            }
            _ => {
                black_box((&operation.operation, &operation.arguments, operation.id.0));
                OperationStatus::Ok
            }
        },
        Err(_) => OperationStatus::Error {
            code: Some("invalid_arguments".into()),
        },
    };
    let measured_latency = elapsed_ns(started).max(1);
    let client_latency_ns = if matches!(status, OperationStatus::Timeout) {
        operation_timeout_ns.max(measured_latency)
    } else {
        measured_latency
    };
    OperationResult {
        id: operation.id,
        operation: operation.operation,
        arguments: operation.arguments,
        intended_start_offset_ns: operation.start_offset_ns,
        actual_start_offset_ns,
        client_latency_ns,
        status,
    }
}

fn select_operation(
    operations: &[ManagedOperation],
    total_weight: f64,
    global_index: u64,
) -> &ManagedOperation {
    &operations[select_operation_index(operations, total_weight, global_index)]
}

fn select_operation_index(
    operations: &[ManagedOperation],
    total_weight: f64,
    global_index: u64,
) -> usize {
    let random = splitmix64(global_index ^ 0x4b4e_4545_4649_4e44);
    let unit = (random >> 11) as f64 * (1.0 / ((1_u64 << 53) as f64));
    let target = unit * total_weight;
    let mut cumulative = 0.0;
    for (index, operation) in operations.iter().enumerate() {
        cumulative += operation.weight;
        if target < cumulative {
            return index;
        }
    }
    operations.len() - 1
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

fn spin_until(
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
        if Instant::now() >= deadline {
            return true;
        }
        std::hint::spin_loop();
    }
}

fn spin_until_unix(
    deadline_unix_ns: u64,
    cancel: &AtomicBool,
    cancel_at_unix_ns: &AtomicU64,
) -> bool {
    loop {
        if cancellation_reached(cancel, cancel_at_unix_ns) {
            return false;
        }
        if unix_now_ns() >= deadline_unix_ns {
            return true;
        }
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

fn write_message(output: &mut impl Write, message: &AdapterMessage) -> Result<(), Box<dyn Error>> {
    serde_json::to_writer(&mut *output, message)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use kneefinder::histogram::HistogramDecodeLimits;

    use super::*;

    fn arguments(class: &str, outcome: &str) -> BTreeMap<String, ArgumentValue> {
        BTreeMap::from([
            (CLASS_ARGUMENT.into(), ArgumentValue::String(class.into())),
            (
                OUTCOME_ARGUMENT.into(),
                ArgumentValue::String(outcome.into()),
            ),
        ])
    }

    fn request(shard_index: u32, shard_count: u32) -> ManagedPhaseRequest {
        ManagedPhaseRequest {
            warmup_ns: 100_000,
            measurement_ns: 5_000_000,
            operation_timeout_ns: 100_000_000,
            load: Load::OpenLoop {
                requests_per_second: 100_000.0,
            },
            operations: vec![
                ManagedOperation {
                    operation: OPERATION.into(),
                    arguments: arguments("a", "ok"),
                    weight: 3.0,
                    shard_index,
                    shard_count,
                },
                ManagedOperation {
                    operation: OPERATION.into(),
                    arguments: arguments("b", "error"),
                    weight: 1.0,
                    shard_index,
                    shard_count,
                },
            ],
            bucket_count: 5,
            histogram: kneefinder::histogram::HistogramSpec {
                lowest_discernible_ns: 1,
                highest_trackable_ns: 1_000_000_000,
                significant_figures: 3,
            },
        }
    }

    #[test]
    fn global_sequence_shards_are_fixed_and_disjoint() {
        let left = (0_u64..20)
            .filter(|index| index % 2 == 0)
            .collect::<Vec<_>>();
        let right = (0_u64..20)
            .filter(|index| index % 2 == 1)
            .collect::<Vec<_>>();

        assert!(left.iter().all(|index| !right.contains(index)));
        assert_eq!(
            left.into_iter().chain(right).collect::<BTreeSet<_>>().len(),
            20
        );
    }

    #[test]
    fn managed_phase_reports_mergeable_histograms_variants_errors_and_buckets() {
        let request = request(1, 2);
        let (completion, result) =
            run_managed_phase(&request, unix_now_ns(), &AtomicBool::new(false)).unwrap();

        assert_eq!(completion, PhaseCompletion::Completed);
        assert!(result.offered > 100);
        assert_eq!(result.started, result.offered);
        assert_eq!(result.completed, result.offered);
        assert_eq!(result.successful + result.failed, result.completed);
        assert_eq!(result.timed_out, 0);
        assert_eq!(result.per_operation.len(), 2);
        assert_eq!(result.time_buckets.len(), 5);
        assert_eq!(
            result
                .time_buckets
                .iter()
                .map(|bucket| bucket.offered)
                .sum::<u64>(),
            result.offered
        );
        assert_eq!(
            result.errors_by_code,
            [kneefinder::protocol::PhaseErrorCount {
                code: Some(ERROR_CODE.into()),
                count: result.failed,
            }]
        );

        let histogram = LatencyHistogram::decode(
            &result.client_latency,
            request.histogram,
            HistogramDecodeLimits::default(),
        )
        .unwrap();
        assert_eq!(histogram.len(), result.completed);
    }

    #[test]
    fn saturated_generation_records_every_offer_without_fabricating_attempts() {
        let mut request = request(0, 1);
        request.warmup_ns = 0;
        request.measurement_ns = 1_000_000;
        request.operation_timeout_ns = 0;
        request.load = Load::OpenLoop {
            requests_per_second: 10_000_000.0,
        };
        request.operations.truncate(1);

        let (completion, result) =
            run_managed_phase(&request, unix_now_ns(), &AtomicBool::new(false)).unwrap();

        assert_eq!(completion, PhaseCompletion::Completed);
        assert_eq!(result.offered, 10_000);
        assert!(result.offered > result.started);
        assert_eq!(result.started, result.completed);
        assert_eq!(result.successful, result.completed);
        assert_eq!(result.failed, 0);
        assert_eq!(result.timed_out, 0);
        assert_eq!(result.per_operation.len(), 1);
        assert_eq!(result.per_operation[0].offered, result.offered);
        assert_eq!(result.per_operation[0].started, result.started);
        assert_eq!(result.per_operation[0].completed, result.completed);
        assert_eq!(
            result
                .time_buckets
                .iter()
                .map(|bucket| bucket.offered)
                .sum::<u64>(),
            result.offered
        );
        assert_eq!(
            result
                .time_buckets
                .iter()
                .map(|bucket| bucket.successful)
                .sum::<u64>(),
            result.successful_in_window
        );

        let histogram = LatencyHistogram::decode(
            &result.client_latency,
            request.histogram,
            HistogramDecodeLimits::default(),
        )
        .unwrap();
        assert_eq!(histogram.len(), result.completed);

        let mut merged = PhaseAccumulator::new(aggregation_plan(&request).unwrap()).unwrap();
        merged.merge(&result).unwrap();
        let report = merged.finish(10_000_000.0).unwrap();
        assert_eq!(report.offered_count, result.offered);
        assert_eq!(report.started_count, result.started);
        assert_eq!(report.completed_count, result.completed);
    }

    #[test]
    fn cancellation_before_warmup_returns_an_empty_valid_summary() {
        let request = request(0, 1);
        let cancel = AtomicBool::new(true);
        let (completion, result) = run_managed_phase(&request, unix_now_ns(), &cancel).unwrap();

        assert_eq!(completion, PhaseCompletion::Cancelled);
        assert_eq!(result.offered, 0);
        assert_eq!(result.completed, 0);
        assert_eq!(result.per_operation.len(), 2);
        assert_eq!(result.time_buckets.len(), 5);
    }

    #[test]
    fn cancellation_interrupts_an_active_deadline_spin() {
        let mut request = request(0, 1);
        request.warmup_ns = 0;
        request.measurement_ns = 100_000_000;
        request.load = Load::OpenLoop {
            requests_per_second: 1_000.0,
        };
        let cancel = Arc::new(AtomicBool::new(false));
        let request_cancel = Arc::clone(&cancel);
        let interrupter = thread::spawn(move || {
            thread::sleep(Duration::from_millis(2));
            request_cancel.store(true, Ordering::Release);
        });

        let (completion, result) = run_managed_phase(&request, unix_now_ns(), &cancel).unwrap();
        interrupter.join().unwrap();

        assert_eq!(completion, PhaseCompletion::Cancelled);
        assert!(result.offered < 100);
        assert!(result.elapsed_ns < request.measurement_ns);
    }

    #[test]
    fn managed_shards_report_the_shared_cancellation_cutoff() {
        let mut left = request(0, 2);
        left.warmup_ns = 0;
        left.measurement_ns = 100_000_000;
        left.load = Load::OpenLoop {
            requests_per_second: 10_000.0,
        };
        let mut right = left.clone();
        for operation in &mut right.operations {
            operation.shard_index = 1;
        }
        let phase_start = unix_now_ns().saturating_add(1_000_000);
        let cutoff = phase_start.saturating_add(5_000_000);

        let left_call = thread::spawn(move || {
            run_managed_phase_with_cutoff(
                &left,
                phase_start,
                &AtomicBool::new(false),
                &AtomicU64::new(cutoff),
            )
            .unwrap()
        });
        let right_call = thread::spawn(move || {
            run_managed_phase_with_cutoff(
                &right,
                phase_start,
                &AtomicBool::new(false),
                &AtomicU64::new(cutoff),
            )
            .unwrap()
        });
        let (left_completion, left_result) = left_call.join().unwrap();
        let (right_completion, right_result) = right_call.join().unwrap();

        assert_eq!(left_completion, PhaseCompletion::Cancelled);
        assert_eq!(right_completion, PhaseCompletion::Cancelled);
        assert_eq!(left_result.elapsed_ns, 5_000_000);
        assert_eq!(right_result.elapsed_ns, left_result.elapsed_ns);
    }

    #[test]
    fn managed_phase_rejects_mixed_shards() {
        let mut request = request(0, 2);
        request.operations[1].shard_index = 1;

        assert!(
            validate_managed_request(&request)
                .unwrap_err()
                .contains("same fixed shard")
        );
    }
}
