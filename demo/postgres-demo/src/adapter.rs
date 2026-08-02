use std::{
    env,
    error::Error,
    io::{self, BufRead, BufReader, BufWriter, Write},
    net::TcpListener,
    sync::{Arc, Condvar, Mutex},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use kneefinder::protocol::{
    AdapterIdentity, AdapterMessage, ArgumentKind, ArgumentValue, Capabilities, ControllerMessage,
    LoadModel, OperationArgument, OperationDescriptor, OperationKind, OperationResult,
    OperationStatus, PROTOCOL_VERSION, ScheduledOperation,
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

fn run_session(input: impl BufRead, mut output: impl Write) -> Result<SessionExit, Box<dyn Error>> {
    run_session_mode(input, &mut output, false)
}

fn run_session_mode(
    input: impl BufRead,
    mut output: impl Write,
    hang_on_schedule: bool,
) -> Result<SessionExit, Box<dyn Error>> {
    let mut service: Option<PostgresService> = None;

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
                service = if hang_on_schedule {
                    None
                } else {
                    Some(PostgresService::new(config)?)
                };
                write_message(
                    &mut output,
                    &AdapterMessage::Ready {
                        protocol_version: PROTOCOL_VERSION,
                        identity: AdapterIdentity {
                            name: "kneefinder-postgres-demo".into(),
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
                                name: "lookup".into(),
                                description: Some(
                                    "read an account balance through PostgreSQL MVCC".into(),
                                ),
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
                                description: Some(
                                    "update a pair of accounts in a PostgreSQL transaction".into(),
                                ),
                                kind: OperationKind::Write,
                                enabled_by_default: false,
                                default_weight: 1.0,
                                arguments: vec![OperationArgument {
                                    name: "route".into(),
                                    description: Some(
                                        "account pair updated by the transaction".into(),
                                    ),
                                    kind: ArgumentKind::Enum,
                                    values: vec!["hot".into(), "cold".into()],
                                    required: true,
                                    default: Some(ArgumentValue::String("hot".into())),
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
    service: PostgresService,
    phase_start_unix_ns: u64,
    actual_start_unix_ns: u64,
    operation: ScheduledOperation,
) -> OperationResult {
    let started = Instant::now();
    let status = match validate_arguments(&operation) {
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
        actual_start_offset_ns: actual_start_unix_ns.saturating_sub(phase_start_unix_ns),
        client_latency_ns: started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        status,
    }
}

fn validate_arguments(operation: &ScheduledOperation) -> Result<(), ()> {
    match operation.operation.as_str() {
        "lookup"
            if matches!(
                operation.arguments.get("account"),
                Some(ArgumentValue::Integer(1..=4))
            ) =>
        {
            Ok(())
        }
        "transfer"
            if matches!(
                operation.arguments.get("route"),
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn operation(name: &str, argument: &str, value: ArgumentValue) -> ScheduledOperation {
        ScheduledOperation {
            id: kneefinder::protocol::OperationId(1),
            operation: name.into(),
            start_offset_ns: 0,
            arguments: BTreeMap::from([(argument.into(), value)]),
        }
    }

    #[test]
    fn validates_only_advertised_postgres_variants() {
        assert!(
            validate_arguments(&operation("lookup", "account", ArgumentValue::Integer(1))).is_ok()
        );
        assert!(
            validate_arguments(&operation(
                "transfer",
                "route",
                ArgumentValue::String("hot".into())
            ))
            .is_ok()
        );
        assert!(
            validate_arguments(&operation("lookup", "account", ArgumentValue::Integer(9))).is_err()
        );
        assert!(
            validate_arguments(&operation(
                "transfer",
                "route",
                ArgumentValue::String("unknown".into())
            ))
            .is_err()
        );
    }
}
