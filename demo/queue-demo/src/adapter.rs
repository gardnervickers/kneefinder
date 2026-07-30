use std::{
    env,
    error::Error,
    io::{self, BufRead, BufReader, BufWriter, Write},
    net::TcpListener,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use kneefinder::protocol::{
    AdapterIdentity, AdapterMessage, ArgumentKind, ArgumentValue, Capabilities, ControllerMessage,
    LoadModel, OperationArgument, OperationDescriptor, OperationKind, OperationResult,
    OperationStatus, PROTOCOL_VERSION, ScheduledOperation,
};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct AdapterConfig {
    #[serde(default = "default_workers")]
    workers: usize,
    #[serde(default = "default_read_service_ms")]
    read_service_ms: u64,
    #[serde(default = "default_write_service_ms")]
    write_service_ms: u64,
    #[serde(default = "default_queue_capacity")]
    queue_capacity: usize,
}

fn default_workers() -> usize {
    env::var("KNEEFINDER_QUEUE_DEMO_WORKERS")
        .ok()
        .and_then(|workers| workers.parse().ok())
        .unwrap_or(4)
}

fn default_read_service_ms() -> u64 {
    10
}

fn default_write_service_ms() -> u64 {
    20
}

fn default_queue_capacity() -> usize {
    4_096
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionExit {
    EndOfStream,
    Shutdown,
}

pub fn run() -> Result<(), Box<dyn Error>> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_session(stdin.lock(), BufWriter::new(stdout.lock())).map(|_| ())
}

pub fn run_hanging() -> Result<(), Box<dyn Error>> {
    let stdin = io::stdin();
    let stdout = io::stdout();
    run_session_mode(stdin.lock(), BufWriter::new(stdout.lock()), true).map(|_| ())
}

pub fn run_tcp(address: &str) -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind(address)?;
    let address = listener.local_addr()?;
    println!("tcp://{address}");
    io::stdout().flush()?;

    loop {
        let (stream, peer) = listener.accept()?;
        stream.set_nodelay(true)?;
        eprintln!("demo TCP agent accepted coordinator connection from {peer}");
        let input = BufReader::new(stream.try_clone()?);
        let output = BufWriter::new(stream);
        match run_session(input, output) {
            Ok(SessionExit::EndOfStream) => {
                eprintln!("demo TCP agent connection closed; waiting for a coordinator");
            }
            Ok(SessionExit::Shutdown) => return Ok(()),
            Err(error) => {
                eprintln!(
                    "demo TCP agent session failed ({error}); waiting for another coordinator"
                );
            }
        }
    }
}

fn run_session(input: impl BufRead, mut output: impl Write) -> Result<SessionExit, Box<dyn Error>> {
    run_session_mode(input, &mut output, false)
}

fn run_session_mode(
    input: impl BufRead,
    mut output: impl Write,
    hang_on_schedule: bool,
) -> Result<SessionExit, Box<dyn Error>> {
    let mut service: Option<QueueService> = None;

    for line in input.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let message = serde_json::from_str::<ControllerMessage>(&line)?;
        match message {
            ControllerMessage::Initialize {
                protocol_version,
                config: supplied_config,
                ..
            } => {
                if protocol_version != PROTOCOL_VERSION {
                    write_message(
                        &mut output,
                        &AdapterMessage::Error {
                            phase_id: None,
                            code: "unsupported_protocol".into(),
                            message: format!(
                                "adapter supports protocol {PROTOCOL_VERSION}, got {protocol_version}"
                            ),
                            retryable: false,
                        },
                    )?;
                    continue;
                }

                let config: AdapterConfig = serde_json::from_value(supplied_config)?;
                service = Some(QueueService::new(config)?);
                write_message(
                    &mut output,
                    &AdapterMessage::Ready {
                        protocol_version: PROTOCOL_VERSION,
                        identity: AdapterIdentity {
                            name: "kneefinder-queue-demo".into(),
                            version: Some(env!("CARGO_PKG_VERSION").into()),
                        },
                        capabilities: Capabilities {
                            scheduled_operations: true,
                            adapter_managed_phases: false,
                            load_models: vec![LoadModel::OpenLoop],
                            max_batch_size: None,
                        },
                        operations: vec![
                            OperationDescriptor {
                                name: "read".into(),
                                description: Some(
                                    "enqueue a read; key 0 costs 10 ms and key 1 costs 20 ms"
                                        .into(),
                                ),
                                kind: OperationKind::Read,
                                enabled_by_default: true,
                                default_weight: 9.0,
                                arguments: vec![OperationArgument {
                                    name: "key".into(),
                                    description: Some("integer key to read".into()),
                                    kind: ArgumentKind::Integer,
                                    values: Vec::new(),
                                    required: true,
                                    default: Some(ArgumentValue::Integer(0)),
                                }],
                            },
                            OperationDescriptor {
                                name: "write".into(),
                                description: Some(
                                    "enqueue a write; small costs 20 ms and large costs 40 ms"
                                        .into(),
                                ),
                                kind: OperationKind::Write,
                                enabled_by_default: false,
                                default_weight: 1.0,
                                arguments: vec![OperationArgument {
                                    name: "value".into(),
                                    description: Some("string value to write".into()),
                                    kind: ArgumentKind::Enum,
                                    values: vec!["small".into(), "large".into()],
                                    required: true,
                                    default: Some(ArgumentValue::String("small".into())),
                                }],
                            },
                        ],
                    },
                )?;
            }
            ControllerMessage::Schedule {
                phase_id,
                phase_start_unix_ns,
                mut operations,
            } => {
                if hang_on_schedule {
                    loop {
                        thread::park();
                    }
                }
                let Some(service) = &service else {
                    write_message(
                        &mut output,
                        &AdapterMessage::Error {
                            phase_id: Some(phase_id),
                            code: "not_initialized".into(),
                            message: "initialize the adapter before scheduling work".into(),
                            retryable: true,
                        },
                    )?;
                    continue;
                };

                operations.sort_by_key(|operation| operation.start_offset_ns);
                let mut calls = Vec::with_capacity(operations.len());
                for operation in operations {
                    sleep_until(phase_start_unix_ns.saturating_add(operation.start_offset_ns));
                    let actual_start_unix_ns = unix_now_ns();
                    let service = service.clone();
                    calls.push(thread::spawn(move || {
                        execute_operation(
                            service,
                            phase_start_unix_ns,
                            actual_start_unix_ns,
                            operation,
                        )
                    }));
                }

                let results = calls
                    .into_iter()
                    .map(|call| call.join().expect("client call thread panicked"))
                    .collect();
                write_message(
                    &mut output,
                    &AdapterMessage::Results {
                        phase_id,
                        operations: results,
                    },
                )?;
            }
            ControllerMessage::CancelPhase { .. } => {}
            ControllerMessage::Shutdown => return Ok(SessionExit::Shutdown),
            ControllerMessage::RunPhase { phase_id, .. } => {
                write_message(
                    &mut output,
                    &AdapterMessage::Error {
                        phase_id: Some(phase_id),
                        code: "unsupported_mode".into(),
                        message: "demo adapter supports scheduled operations only".into(),
                        retryable: false,
                    },
                )?;
            }
        }
    }

    Ok(SessionExit::EndOfStream)
}

fn execute_operation(
    service: QueueService,
    phase_start_unix_ns: u64,
    actual_start_unix_ns: u64,
    operation: ScheduledOperation,
) -> OperationResult {
    let started = Instant::now();
    let status = match validate_arguments(&operation)
        .and_then(|()| service.call(&operation.operation, &operation.arguments))
    {
        Ok(()) => OperationStatus::Ok,
        Err(error) => OperationStatus::Error {
            code: Some(error.to_string()),
        },
    };

    OperationResult {
        id: operation.id,
        operation: operation.operation,
        arguments: operation.arguments,
        intended_start_offset_ns: operation.start_offset_ns,
        actual_start_offset_ns: actual_start_unix_ns.saturating_sub(phase_start_unix_ns),
        client_latency_ns: started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        status,
    }
}

fn validate_arguments(operation: &ScheduledOperation) -> Result<(), Box<dyn Error + Send + Sync>> {
    match operation.operation.as_str() {
        "read"
            if matches!(
                operation.arguments.get("key"),
                Some(ArgumentValue::Integer(_))
            ) =>
        {
            Ok(())
        }
        "write"
            if matches!(
                operation.arguments.get("value"),
                Some(ArgumentValue::String(_))
            ) =>
        {
            Ok(())
        }
        "read" => Err("read requires integer argument key".into()),
        "write" => Err("write requires string argument value".into()),
        unknown => Err(format!("unknown operation {unknown:?}").into()),
    }
}

#[derive(Clone)]
struct QueueService {
    requests: mpsc::SyncSender<QueuedRequest>,
    read_service_time: Duration,
    write_service_time: Duration,
}

struct QueuedRequest {
    value: u8,
    service_time: Duration,
    response: mpsc::Sender<u8>,
}

impl QueueService {
    fn new(config: AdapterConfig) -> Result<Self, Box<dyn Error>> {
        if config.workers == 0
            || config.read_service_ms == 0
            || config.write_service_ms == 0
            || config.queue_capacity == 0
        {
            return Err(
                "workers, read_service_ms, write_service_ms, and queue_capacity must be nonzero"
                    .into(),
            );
        }

        let (requests, receiver) = mpsc::sync_channel(config.queue_capacity);
        let receiver = Arc::new(Mutex::new(receiver));
        for worker_id in 0..config.workers {
            let receiver = Arc::clone(&receiver);
            thread::Builder::new()
                .name(format!("demo-queue-worker-{worker_id}"))
                .spawn(move || queue_worker(receiver))?;
        }

        let average_read_ms = config.read_service_ms as f64 * 1.25;
        let average_write_ms = config.write_service_ms as f64 * 1.25;
        let mixed_service_ms = (9.0 * average_read_ms + average_write_ms) / 10.0;
        eprintln!(
            "demo queue: {} workers; 90/10 ops and 3/1 arg variants; knee {:.0} req/s",
            config.workers,
            config.workers as f64 * 1_000.0 / mixed_service_ms
        );
        Ok(Self {
            requests,
            read_service_time: Duration::from_millis(config.read_service_ms),
            write_service_time: Duration::from_millis(config.write_service_ms),
        })
    }

    fn call(
        &self,
        operation: &str,
        arguments: &std::collections::BTreeMap<String, ArgumentValue>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let service_time = match (operation, arguments) {
            ("read", arguments) if arguments.get("key") == Some(&ArgumentValue::Integer(0)) => {
                self.read_service_time
            }
            ("read", arguments) if arguments.get("key") == Some(&ArgumentValue::Integer(1)) => {
                self.read_service_time * 2
            }
            ("write", arguments)
                if arguments.get("value") == Some(&ArgumentValue::String("small".into())) =>
            {
                self.write_service_time
            }
            ("write", arguments)
                if arguments.get("value") == Some(&ArgumentValue::String("large".into())) =>
            {
                self.write_service_time * 2
            }
            _ => return Err(format!("unsupported operation variant {operation:?}").into()),
        };
        let (response, received) = mpsc::channel();
        self.requests.send(QueuedRequest {
            value: 41,
            service_time,
            response,
        })?;
        if received.recv()? != 42 {
            return Err("unexpected queue response".into());
        }
        Ok(())
    }
}

fn queue_worker(receiver: Arc<Mutex<mpsc::Receiver<QueuedRequest>>>) {
    loop {
        let request = receiver.lock().expect("queue mutex poisoned").recv();
        let Ok(request) = request else {
            return;
        };
        thread::sleep(request.service_time);
        let _ = request.response.send(request.value.wrapping_add(1));
    }
}

fn sleep_until(deadline_unix_ns: u64) {
    loop {
        let remaining = deadline_unix_ns.saturating_sub(unix_now_ns());
        if remaining == 0 {
            return;
        }
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

fn write_message(writer: &mut impl Write, message: &AdapterMessage) -> Result<(), Box<dyn Error>> {
    serde_json::to_writer(&mut *writer, message)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}
