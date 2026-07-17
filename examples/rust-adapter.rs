//! Minimal standalone kneefinder adapter.
//!
//! Replace `call_target` with calls into the system you want to measure. The
//! surrounding code is the transport/runtime side of the adapter contract.

use std::{
    collections::BTreeMap,
    error::Error,
    io::{self, BufRead, Write},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use kneefinder::protocol::{
    AdapterMessage, ArgumentKind, ArgumentValue, Capabilities, ControllerMessage, LoadModel,
    OperationArgument, OperationDescriptor, OperationKind, OperationResult, OperationStatus,
    PROTOCOL_VERSION, ScheduledOperation,
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    for line in io::stdin().lock().lines() {
        let message = serde_json::from_str::<ControllerMessage>(&line?)?;
        match message {
            ControllerMessage::Initialize {
                protocol_version, ..
            } if protocol_version == PROTOCOL_VERSION => {
                write_message(
                    &mut stdout,
                    &AdapterMessage::Ready {
                        protocol_version: PROTOCOL_VERSION,
                        capabilities: Capabilities {
                            scheduled_operations: true,
                            adapter_managed_phases: false,
                            load_models: vec![LoadModel::OpenLoop],
                            max_batch_size: None,
                        },
                        operations: operation_descriptors(),
                    },
                )?;
            }
            ControllerMessage::Initialize {
                protocol_version, ..
            } => {
                write_message(
                    &mut stdout,
                    &AdapterMessage::Error {
                        phase_id: None,
                        code: "unsupported_protocol".into(),
                        message: format!(
                            "adapter supports protocol {PROTOCOL_VERSION}, got {protocol_version}"
                        ),
                        retryable: false,
                    },
                )?;
            }
            ControllerMessage::Schedule {
                phase_id,
                phase_start_unix_ns,
                operations,
            } => {
                let calls = operations
                    .into_iter()
                    .map(|operation| thread::spawn(move || execute(phase_start_unix_ns, operation)))
                    .collect::<Vec<_>>();
                let operations = calls
                    .into_iter()
                    .map(|call| call.join().expect("adapter call thread panicked"))
                    .collect();
                write_message(
                    &mut stdout,
                    &AdapterMessage::Results {
                        phase_id,
                        operations,
                    },
                )?;
            }
            ControllerMessage::Shutdown => break,
            ControllerMessage::CancelPhase { .. } => {
                // A production runtime should propagate cancellation to calls.
            }
            ControllerMessage::RunPhase { phase_id, .. } => {
                write_message(
                    &mut stdout,
                    &AdapterMessage::Error {
                        phase_id: Some(phase_id),
                        code: "unsupported_mode".into(),
                        message: "this example supports scheduled operations only".into(),
                        retryable: false,
                    },
                )?;
            }
        }
    }
    Ok(())
}

fn operation_descriptors() -> Vec<OperationDescriptor> {
    vec![
        OperationDescriptor {
            name: "get".into(),
            description: Some("fetch a value by integer key".into()),
            kind: OperationKind::Read,
            enabled_by_default: true,
            default_weight: 9.0,
            arguments: vec![OperationArgument {
                name: "key".into(),
                description: None,
                kind: ArgumentKind::Integer,
                required: true,
                default: Some(ArgumentValue::Integer(0)),
            }],
        },
        OperationDescriptor {
            name: "put".into(),
            description: Some("store a string value under an integer key".into()),
            kind: OperationKind::Write,
            enabled_by_default: true,
            default_weight: 1.0,
            arguments: vec![
                OperationArgument {
                    name: "key".into(),
                    description: None,
                    kind: ArgumentKind::Integer,
                    required: true,
                    default: Some(ArgumentValue::Integer(0)),
                },
                OperationArgument {
                    name: "value".into(),
                    description: None,
                    kind: ArgumentKind::String,
                    required: true,
                    default: Some(ArgumentValue::String("hello".into())),
                },
            ],
        },
    ]
}

fn execute(phase_start_unix_ns: u64, operation: ScheduledOperation) -> OperationResult {
    sleep_until(phase_start_unix_ns.saturating_add(operation.start_offset_ns));
    let actual_start_offset_ns = unix_now_ns().saturating_sub(phase_start_unix_ns);
    let started = Instant::now();
    let status = match call_target(&operation.operation, &operation.arguments) {
        Ok(()) => OperationStatus::Ok,
        Err(code) => OperationStatus::Error {
            code: Some(code.into()),
        },
    };
    OperationResult {
        id: operation.id,
        operation: operation.operation,
        arguments: operation.arguments,
        intended_start_offset_ns: operation.start_offset_ns,
        actual_start_offset_ns,
        client_latency_ns: started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        status,
    }
}

/// Replace this function with native calls into your database, service, or
/// library. Return stable low-cardinality codes so kneefinder can group errors.
fn call_target(
    operation: &str,
    arguments: &BTreeMap<String, ArgumentValue>,
) -> Result<(), &'static str> {
    let key = match arguments.get("key") {
        Some(ArgumentValue::Integer(key)) if *key >= 0 => *key,
        _ => return Err("invalid_key"),
    };
    match operation {
        "get" => thread::sleep(Duration::from_millis(2 + key.unsigned_abs() % 2)),
        "put" if matches!(arguments.get("value"), Some(ArgumentValue::String(_))) => {
            thread::sleep(Duration::from_millis(5));
        }
        "put" => return Err("invalid_value"),
        _ => return Err("unknown_operation"),
    }
    Ok(())
}

fn write_message(output: &mut impl Write, message: &AdapterMessage) -> Result<(), Box<dyn Error>> {
    serde_json::to_writer(&mut *output, message)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn sleep_until(unix_ns: u64) {
    let remaining = unix_ns.saturating_sub(unix_now_ns());
    if remaining > 0 {
        thread::sleep(Duration::from_nanos(remaining));
    }
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}
