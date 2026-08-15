//! Minimal standalone kneefinder adapter.
//!
//! Replace `call_target` with calls into the system you want to measure. The
//! surrounding code is the transport/runtime side of the adapter contract.

use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    io::{self, BufRead, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use kneefinder::protocol::{
    AdapterIdentity, AdapterMessage, ArgumentKind, ArgumentValue, Capabilities, ControllerMessage,
    LoadModel, OperationArgument, OperationDescriptor, OperationKind, OperationResult,
    OperationStatus, PROTOCOL_VERSION, PhaseId, ScheduledOperation,
};

fn main() -> Result<(), Box<dyn Error>> {
    let stdout = Mutex::new(io::BufWriter::new(io::stdout()));
    let cancellations = Mutex::new(HashMap::<PhaseId, Arc<AtomicBool>>::new());
    thread::scope(|scope| -> Result<(), Box<dyn Error>> {
        for line in io::stdin().lock().lines() {
            let message = serde_json::from_str::<ControllerMessage>(&line?)?;
            match message {
                ControllerMessage::Initialize {
                    protocol_version, ..
                } if protocol_version == PROTOCOL_VERSION => {
                    write_shared_message(
                        &stdout,
                        &AdapterMessage::Ready {
                            protocol_version: PROTOCOL_VERSION,
                            identity: AdapterIdentity {
                                name: "rust-adapter-example".into(),
                                version: Some(env!("CARGO_PKG_VERSION").into()),
                            },
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
                    write_shared_message(
                        &stdout,
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
                    mut operations,
                } => {
                    let cancellation = Arc::new(AtomicBool::new(false));
                    cancellations
                        .lock()
                        .expect("cancellation mutex poisoned")
                        .insert(phase_id, Arc::clone(&cancellation));
                    let stdout = &stdout;
                    let cancellations = &cancellations;
                    scope.spawn(move || {
                        operations.sort_by_key(|operation| operation.start_offset_ns);
                        let expected = operations.len();
                        let mut calls = Vec::with_capacity(expected);
                        for operation in operations {
                            if cancellation.load(Ordering::Acquire) {
                                break;
                            }
                            sleep_until(
                                phase_start_unix_ns.saturating_add(operation.start_offset_ns),
                            );
                            if cancellation.load(Ordering::Acquire) {
                                break;
                            }
                            calls.push(thread::spawn(move || {
                                execute(phase_start_unix_ns, operation)
                            }));
                        }
                        let cancelled = calls.len() < expected;
                        let results = calls
                            .into_iter()
                            .map(|call| call.join().expect("adapter call thread panicked"))
                            .collect::<Vec<_>>();
                        if !results.is_empty()
                            && let Err(error) = write_shared_message(
                                stdout,
                                &AdapterMessage::Results {
                                    phase_id,
                                    operations: results,
                                },
                            )
                        {
                            eprintln!("failed to write adapter results: {error}");
                        }
                        if cancelled
                            && let Err(error) = write_shared_message(
                                stdout,
                                &AdapterMessage::Error {
                                    phase_id: Some(phase_id),
                                    code: "cancelled".into(),
                                    message: "scheduled phase was cancelled".into(),
                                    retryable: false,
                                },
                            )
                        {
                            eprintln!("failed to write cancellation result: {error}");
                        }
                        cancellations
                            .lock()
                            .expect("cancellation mutex poisoned")
                            .remove(&phase_id);
                    });
                }
                ControllerMessage::Shutdown => break,
                ControllerMessage::CancelPhase { phase_id } => {
                    if let Some(cancellation) = cancellations
                        .lock()
                        .expect("cancellation mutex poisoned")
                        .get(&phase_id)
                    {
                        cancellation.store(true, Ordering::Release);
                    }
                }
                ControllerMessage::RunPhase { phase_id, .. } => {
                    write_shared_message(
                        &stdout,
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
        for cancellation in cancellations
            .lock()
            .expect("cancellation mutex poisoned")
            .values()
        {
            cancellation.store(true, Ordering::Release);
        }
        Ok(())
    })
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
                values: Vec::new(),
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
                    values: Vec::new(),
                    required: true,
                    default: Some(ArgumentValue::Integer(0)),
                },
                OperationArgument {
                    name: "value".into(),
                    description: None,
                    kind: ArgumentKind::String,
                    values: Vec::new(),
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

fn write_shared_message(
    output: &Mutex<impl Write>,
    message: &AdapterMessage,
) -> Result<(), Box<dyn Error>> {
    write_message(
        &mut *output.lock().expect("adapter output mutex poisoned"),
        message,
    )
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
