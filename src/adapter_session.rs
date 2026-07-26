//! Transport-independent adapter sessions and the default NDJSON subprocess transport.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    io::{self, BufRead, BufReader, BufWriter, Write},
    net::{Shutdown, TcpStream, ToSocketAddrs},
    process::{Child, ChildStdin, Command, ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread,
    time::{Duration, Instant},
};

use serde_json::Value;

use crate::{
    config::AdapterCommand,
    protocol::{
        AdapterIdentity, AdapterMessage, Capabilities, ControllerMessage, OperationDescriptor,
        OperationId, OperationResult, PROTOCOL_VERSION, PhaseId, RunId, ScheduledOperation,
    },
};

const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_DIAGNOSTIC_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_DIAGNOSTIC_LINES: usize = 1_024;
const BUFFERED_ADAPTER_MESSAGES: usize = 8;

#[derive(Debug, Clone)]
pub struct SessionOptions {
    pub connection_timeout: Duration,
    pub handshake_timeout: Duration,
    pub response_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub maximum_frame_bytes: usize,
    pub maximum_diagnostic_lines: usize,
}

impl Default for SessionOptions {
    fn default() -> Self {
        Self {
            connection_timeout: Duration::from_secs(5),
            handshake_timeout: Duration::from_secs(10),
            response_timeout: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(5),
            maximum_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            maximum_diagnostic_lines: DEFAULT_MAX_DIAGNOSTIC_LINES,
        }
    }
}

/// Message transport used by [`AdapterSession`]. Stdio and TCP use the same
/// handshake, scheduling, cancellation, and validation logic.
pub trait AdapterTransport: Send {
    fn send(&mut self, message: &ControllerMessage) -> Result<(), TransportError>;
    fn receive(&mut self, timeout: Duration) -> Result<AdapterMessage, TransportError>;
    fn diagnostics(&self) -> Vec<String>;
    fn close(&mut self, timeout: Duration) -> Result<(), TransportError>;
}

/// Default zero-setup transport: one supervised adapter child using NDJSON on
/// stdin/stdout and a bounded diagnostic tail captured from stderr.
pub struct SubprocessTransport {
    child: Child,
    input: Option<BufWriter<ChildStdin>>,
    messages: Receiver<Result<AdapterMessage, TransportError>>,
    diagnostics: Arc<Mutex<VecDeque<String>>>,
}

impl SubprocessTransport {
    pub fn spawn(
        command: &AdapterCommand,
        options: &SessionOptions,
    ) -> Result<Self, TransportError> {
        let mut child = Command::new(&command.program)
            .args(&command.arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(TransportError::io)?;
        let Some(input) = child.stdin.take() else {
            terminate_child(&mut child);
            return Err(TransportError::Io("adapter stdin was not available".into()));
        };
        let Some(output) = child.stdout.take() else {
            terminate_child(&mut child);
            return Err(TransportError::Io(
                "adapter stdout was not available".into(),
            ));
        };
        let Some(stderr) = child.stderr.take() else {
            terminate_child(&mut child);
            return Err(TransportError::Io(
                "adapter stderr was not available".into(),
            ));
        };

        let (sender, messages) = mpsc::sync_channel(BUFFERED_ADAPTER_MESSAGES);
        let maximum_frame_bytes = options.maximum_frame_bytes;
        if let Err(error) = thread::Builder::new()
            .name("kneefinder-adapter-stdout".into())
            .spawn(move || read_adapter_messages(output, maximum_frame_bytes, sender))
        {
            terminate_child(&mut child);
            return Err(TransportError::io(error));
        }

        let diagnostics = Arc::new(Mutex::new(VecDeque::new()));
        let diagnostic_tail = Arc::clone(&diagnostics);
        let maximum_diagnostic_lines = options.maximum_diagnostic_lines;
        if let Err(error) = thread::Builder::new()
            .name("kneefinder-adapter-stderr".into())
            .spawn(move || capture_diagnostics(stderr, maximum_diagnostic_lines, diagnostic_tail))
        {
            terminate_child(&mut child);
            return Err(TransportError::io(error));
        }

        Ok(Self {
            child,
            input: Some(BufWriter::new(input)),
            messages,
            diagnostics,
        })
    }

    fn process_error(&mut self, fallback: TransportError) -> TransportError {
        match self.child.try_wait() {
            Ok(Some(status)) => TransportError::ProcessExited(status),
            Ok(None) => fallback,
            Err(error) => TransportError::io(error),
        }
    }

    fn wait_until(&mut self, deadline: Instant) -> Result<Option<ExitStatus>, TransportError> {
        loop {
            if let Some(status) = self.child.try_wait().map_err(TransportError::io)? {
                return Ok(Some(status));
            }
            if Instant::now() >= deadline {
                return Ok(None);
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

fn terminate_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

impl AdapterTransport for SubprocessTransport {
    fn send(&mut self, message: &ControllerMessage) -> Result<(), TransportError> {
        if let Some(status) = self.child.try_wait().map_err(TransportError::io)? {
            return Err(TransportError::ProcessExited(status));
        }
        let frame = serde_json::to_vec(message).map_err(TransportError::json)?;
        let Some(input) = self.input.as_mut() else {
            return Err(TransportError::Closed);
        };
        input.write_all(&frame).map_err(TransportError::io)?;
        input.write_all(b"\n").map_err(TransportError::io)?;
        input.flush().map_err(TransportError::io)
    }

    fn receive(&mut self, timeout: Duration) -> Result<AdapterMessage, TransportError> {
        match self.messages.recv_timeout(timeout) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => {
                Err(self.process_error(TransportError::ReceiveTimeout(timeout)))
            }
            Err(RecvTimeoutError::Disconnected) => Err(self.process_error(TransportError::Closed)),
        }
    }

    fn diagnostics(&self) -> Vec<String> {
        self.diagnostics
            .lock()
            .expect("adapter diagnostics mutex poisoned")
            .iter()
            .cloned()
            .collect()
    }

    fn close(&mut self, timeout: Duration) -> Result<(), TransportError> {
        self.input.take();
        let deadline = Instant::now() + timeout;
        match self.wait_until(deadline)? {
            Some(status) if status.success() => Ok(()),
            Some(status) => Err(TransportError::ProcessExited(status)),
            None => {
                self.child.kill().map_err(TransportError::io)?;
                self.child.wait().map_err(TransportError::io)?;
                Err(TransportError::ShutdownTimeout(timeout))
            }
        }
    }
}

impl Drop for SubprocessTransport {
    fn drop(&mut self) {
        self.input.take();
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

/// Persistent NDJSON transport to an explicitly configured adapter endpoint.
/// The coordinator always establishes the connection; the adapter only accepts
/// it and responds on the resulting full-duplex stream.
pub struct TcpTransport {
    endpoint: String,
    control: TcpStream,
    input: Option<BufWriter<TcpStream>>,
    messages: Receiver<Result<AdapterMessage, TransportError>>,
}

impl TcpTransport {
    pub fn connect(endpoint: &str, options: &SessionOptions) -> Result<Self, TransportError> {
        let addresses = endpoint
            .to_socket_addrs()
            .map_err(|error| TransportError::ConnectionFailed {
                endpoint: endpoint.into(),
                message: error.to_string(),
            })?
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            return Err(TransportError::ConnectionFailed {
                endpoint: endpoint.into(),
                message: "endpoint resolved to no addresses".into(),
            });
        }

        let mut last_error = None;
        let mut connected = None;
        for address in addresses {
            match TcpStream::connect_timeout(&address, options.connection_timeout) {
                Ok(stream) => {
                    connected = Some(stream);
                    break;
                }
                Err(error) => last_error = Some(error),
            }
        }
        let control = connected.ok_or_else(|| TransportError::ConnectionFailed {
            endpoint: endpoint.into(),
            message: last_error
                .map(|error| error.to_string())
                .unwrap_or_else(|| "connection attempt failed".into()),
        })?;
        control.set_nodelay(true).map_err(TransportError::io)?;
        let input = control.try_clone().map_err(TransportError::io)?;
        let output = control.try_clone().map_err(TransportError::io)?;

        let (sender, messages) = mpsc::sync_channel(BUFFERED_ADAPTER_MESSAGES);
        let maximum_frame_bytes = options.maximum_frame_bytes;
        if let Err(error) = thread::Builder::new()
            .name("kneefinder-adapter-tcp".into())
            .spawn(move || read_adapter_messages(output, maximum_frame_bytes, sender))
        {
            let _ = control.shutdown(Shutdown::Both);
            return Err(TransportError::io(error));
        }

        Ok(Self {
            endpoint: endpoint.into(),
            control,
            input: Some(BufWriter::new(input)),
            messages,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl AdapterTransport for TcpTransport {
    fn send(&mut self, message: &ControllerMessage) -> Result<(), TransportError> {
        let frame = serde_json::to_vec(message).map_err(TransportError::json)?;
        let Some(input) = self.input.as_mut() else {
            return Err(TransportError::Closed);
        };
        input.write_all(&frame).map_err(TransportError::io)?;
        input.write_all(b"\n").map_err(TransportError::io)?;
        input.flush().map_err(TransportError::io)
    }

    fn receive(&mut self, timeout: Duration) -> Result<AdapterMessage, TransportError> {
        match self.messages.recv_timeout(timeout) {
            Ok(result) => result,
            Err(RecvTimeoutError::Timeout) => Err(TransportError::ReceiveTimeout(timeout)),
            Err(RecvTimeoutError::Disconnected) => Err(TransportError::Closed),
        }
    }

    fn diagnostics(&self) -> Vec<String> {
        Vec::new()
    }

    fn close(&mut self, _timeout: Duration) -> Result<(), TransportError> {
        self.input.take();
        match self.control.shutdown(Shutdown::Both) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotConnected => Ok(()),
            Err(error) => Err(TransportError::io(error)),
        }
    }
}

impl Drop for TcpTransport {
    fn drop(&mut self) {
        self.input.take();
        let _ = self.control.shutdown(Shutdown::Both);
    }
}

fn read_adapter_messages(
    output: impl io::Read,
    maximum_frame_bytes: usize,
    sender: SyncSender<Result<AdapterMessage, TransportError>>,
) {
    let mut output = BufReader::new(output);
    loop {
        let frame = match read_frame(&mut output, maximum_frame_bytes) {
            Ok(Some(frame)) => frame,
            Ok(None) => break,
            Err(error) => {
                let _ = sender.send(Err(error));
                break;
            }
        };
        let message = serde_json::from_slice(&frame).map_err(TransportError::json);
        let malformed = message.is_err();
        if sender.send(message).is_err() || malformed {
            break;
        }
    }
}

fn read_frame(
    reader: &mut impl BufRead,
    maximum_frame_bytes: usize,
) -> Result<Option<Vec<u8>>, TransportError> {
    let mut frame = Vec::new();
    loop {
        let available = reader.fill_buf().map_err(TransportError::io)?;
        if available.is_empty() {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Err(TransportError::TruncatedFrame)
            };
        }
        let (consumed, complete) = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or((available.len(), false), |position| (position + 1, true));
        if frame.len().saturating_add(consumed) > maximum_frame_bytes {
            return Err(TransportError::FrameTooLarge {
                maximum: maximum_frame_bytes,
            });
        }
        frame.extend_from_slice(&available[..consumed]);
        reader.consume(consumed);
        if complete {
            return Ok(Some(frame));
        }
    }
}

fn capture_diagnostics(
    stderr: impl io::Read,
    maximum_lines: usize,
    diagnostics: Arc<Mutex<VecDeque<String>>>,
) {
    let mut stderr = BufReader::new(stderr);
    while let Ok(Some((line, truncated))) =
        read_bounded_line(&mut stderr, DEFAULT_MAX_DIAGNOSTIC_BYTES)
    {
        if maximum_lines == 0 {
            continue;
        }
        let mut line = String::from_utf8_lossy(&line)
            .trim_end_matches(['\r', '\n'])
            .to_owned();
        if truncated {
            line.push_str(" [truncated]");
        }
        let mut tail = diagnostics
            .lock()
            .expect("adapter diagnostics mutex poisoned");
        if tail.len() == maximum_lines {
            tail.pop_front();
        }
        tail.push_back(line);
    }
}

fn read_bounded_line(
    reader: &mut impl BufRead,
    maximum_bytes: usize,
) -> io::Result<Option<(Vec<u8>, bool)>> {
    let mut line = Vec::new();
    let mut saw_bytes = false;
    let mut truncated = false;
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if saw_bytes {
                Ok(Some((line, truncated)))
            } else {
                Ok(None)
            };
        }
        saw_bytes = true;
        let (consumed, complete) = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or((available.len(), false), |position| (position + 1, true));
        let remaining = maximum_bytes.saturating_sub(line.len());
        let retained = consumed.min(remaining);
        line.extend_from_slice(&available[..retained]);
        truncated |= retained < consumed;
        reader.consume(consumed);
        if complete {
            return Ok(Some((line, truncated)));
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdapterReady {
    pub identity: AdapterIdentity,
    pub capabilities: Capabilities,
    pub operations: Vec<OperationDescriptor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionState {
    New,
    Ready,
    PhaseActive(PhaseId),
    Failed,
    Closed,
}

/// Owns adapter protocol state and validation independently of how frames are
/// transported.
pub struct AdapterSession<T> {
    transport: T,
    options: SessionOptions,
    state: SessionState,
    capabilities: Option<Capabilities>,
}

impl<T: AdapterTransport> AdapterSession<T> {
    pub fn new(transport: T, options: SessionOptions) -> Self {
        Self {
            transport,
            options,
            state: SessionState::New,
            capabilities: None,
        }
    }

    pub fn state(&self) -> SessionState {
        self.state
    }

    pub fn diagnostics(&self) -> Vec<String> {
        self.transport.diagnostics()
    }

    pub fn initialize(
        &mut self,
        run_id: RunId,
        config: Value,
    ) -> Result<AdapterReady, SessionError> {
        self.require_state(SessionState::New)?;
        self.send(&ControllerMessage::Initialize {
            protocol_version: PROTOCOL_VERSION,
            run_id,
            config,
        })?;
        let message = self.receive(self.options.handshake_timeout)?;
        match message {
            AdapterMessage::Ready {
                protocol_version,
                identity,
                capabilities,
                operations,
            } if protocol_version == PROTOCOL_VERSION => {
                if identity.name.trim().is_empty() {
                    return self.fail(SessionError::InvalidAdapterIdentity);
                }
                self.capabilities = Some(capabilities.clone());
                self.state = SessionState::Ready;
                Ok(AdapterReady {
                    identity,
                    capabilities,
                    operations,
                })
            }
            AdapterMessage::Ready {
                protocol_version, ..
            } => self.fail(SessionError::ProtocolVersionMismatch {
                expected: PROTOCOL_VERSION,
                actual: protocol_version,
            }),
            AdapterMessage::Error {
                phase_id,
                code,
                message,
                retryable,
            } => self.fail(SessionError::Adapter {
                phase_id,
                code,
                message,
                retryable,
            }),
            message => self.fail(SessionError::UnexpectedMessage {
                state: SessionState::New,
                message: message_kind(&message),
            }),
        }
    }

    pub fn schedule(
        &mut self,
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
        operations: Vec<ScheduledOperation>,
    ) -> Result<Vec<OperationResult>, SessionError> {
        self.require_state(SessionState::Ready)?;
        if let Some(maximum) = self
            .capabilities
            .as_ref()
            .and_then(|capabilities| capabilities.max_batch_size)
            && operations.len() > maximum as usize
        {
            return Err(SessionError::BatchTooLarge {
                actual: operations.len(),
                maximum,
            });
        }

        let mut expected = HashMap::with_capacity(operations.len());
        for operation in &operations {
            if expected.insert(operation.id, operation.clone()).is_some() {
                return Err(SessionError::DuplicateScheduledOperation(operation.id));
            }
        }
        if operations.is_empty() {
            return Ok(Vec::new());
        }

        self.send(&ControllerMessage::Schedule {
            phase_id,
            phase_start_unix_ns,
            operations: operations.clone(),
        })?;
        self.state = SessionState::PhaseActive(phase_id);

        let deadline = Instant::now() + self.options.response_timeout;
        let mut received = HashMap::with_capacity(expected.len());
        while received.len() < expected.len() {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return self.fail(SessionError::Transport(TransportError::ReceiveTimeout(
                    self.options.response_timeout,
                )));
            };
            let message = self.receive(remaining)?;
            match message {
                AdapterMessage::Results {
                    phase_id: actual_phase,
                    operations: results,
                } if actual_phase == phase_id => {
                    if results.is_empty() {
                        return self.fail(SessionError::EmptyResults { phase_id });
                    }
                    for result in results {
                        let Some(scheduled) = expected.get(&result.id) else {
                            return self.fail(SessionError::UnexpectedOperationResult(result.id));
                        };
                        if received.contains_key(&result.id) {
                            return self.fail(SessionError::DuplicateOperationResult(result.id));
                        }
                        if result.operation != scheduled.operation
                            || result.arguments != scheduled.arguments
                            || result.intended_start_offset_ns != scheduled.start_offset_ns
                        {
                            return self.fail(SessionError::OperationResultMismatch(result.id));
                        }
                        received.insert(result.id, result);
                    }
                }
                AdapterMessage::Results {
                    phase_id: actual, ..
                } => {
                    return self.fail(SessionError::UnexpectedPhase {
                        expected: phase_id,
                        actual,
                    });
                }
                AdapterMessage::Error {
                    phase_id: error_phase,
                    code,
                    message,
                    retryable,
                } => {
                    return self.fail(SessionError::Adapter {
                        phase_id: error_phase,
                        code,
                        message,
                        retryable,
                    });
                }
                message => {
                    return self.fail(SessionError::UnexpectedMessage {
                        state: self.state,
                        message: message_kind(&message),
                    });
                }
            }
        }

        self.state = SessionState::Ready;
        Ok(operations
            .iter()
            .map(|operation| {
                received
                    .remove(&operation.id)
                    .expect("every scheduled operation was validated")
            })
            .collect())
    }

    pub fn cancel(&mut self, phase_id: PhaseId) -> Result<(), SessionError> {
        match self.state {
            SessionState::Ready | SessionState::PhaseActive(_) => {
                self.send(&ControllerMessage::CancelPhase { phase_id })?;
                Ok(())
            }
            state => Err(SessionError::InvalidState {
                expected: SessionState::Ready,
                actual: state,
            }),
        }
    }

    pub fn shutdown(&mut self) -> Result<(), SessionError> {
        if self.state == SessionState::Closed {
            return Ok(());
        }
        let send = self.transport.send(&ControllerMessage::Shutdown);
        let close = self.transport.close(self.options.shutdown_timeout);
        self.state = SessionState::Closed;
        send.and(close).map_err(SessionError::Transport)
    }

    fn require_state(&self, expected: SessionState) -> Result<(), SessionError> {
        if self.state == expected {
            Ok(())
        } else {
            Err(SessionError::InvalidState {
                expected,
                actual: self.state,
            })
        }
    }

    fn receive(&mut self, timeout: Duration) -> Result<AdapterMessage, SessionError> {
        self.transport.receive(timeout).map_err(|error| {
            self.state = SessionState::Failed;
            SessionError::Transport(error)
        })
    }

    fn send(&mut self, message: &ControllerMessage) -> Result<(), SessionError> {
        self.transport.send(message).map_err(|error| {
            self.state = SessionState::Failed;
            SessionError::Transport(error)
        })
    }

    fn fail<R>(&mut self, error: SessionError) -> Result<R, SessionError> {
        self.state = SessionState::Failed;
        Err(error)
    }
}

fn message_kind(message: &AdapterMessage) -> &'static str {
    match message {
        AdapterMessage::Ready { .. } => "ready",
        AdapterMessage::Results { .. } => "results",
        AdapterMessage::PhaseComplete { .. } => "phase_complete",
        AdapterMessage::Error { .. } => "error",
    }
}

#[derive(Debug)]
pub enum TransportError {
    Io(String),
    Json(String),
    ConnectionFailed { endpoint: String, message: String },
    ReceiveTimeout(Duration),
    ShutdownTimeout(Duration),
    FrameTooLarge { maximum: usize },
    TruncatedFrame,
    ProcessExited(ExitStatus),
    Closed,
}

impl TransportError {
    fn io(error: impl fmt::Display) -> Self {
        Self::Io(error.to_string())
    }

    fn json(error: impl fmt::Display) -> Self {
        Self::Json(error.to_string())
    }
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "adapter transport I/O failed: {message}"),
            Self::Json(message) => write!(formatter, "adapter emitted malformed JSON: {message}"),
            Self::ConnectionFailed { endpoint, message } => {
                write!(
                    formatter,
                    "failed to connect to adapter {endpoint:?}: {message}"
                )
            }
            Self::ReceiveTimeout(timeout) => {
                write!(formatter, "adapter did not respond within {timeout:?}")
            }
            Self::ShutdownTimeout(timeout) => write!(
                formatter,
                "adapter did not stop within {timeout:?} and was forcefully terminated"
            ),
            Self::FrameTooLarge { maximum } => {
                write!(formatter, "adapter frame exceeds the {maximum}-byte limit")
            }
            Self::TruncatedFrame => formatter.write_str("adapter closed stdout mid-frame"),
            Self::ProcessExited(status) => write!(formatter, "adapter exited with {status}"),
            Self::Closed => formatter.write_str("adapter transport closed unexpectedly"),
        }
    }
}

impl std::error::Error for TransportError {}

#[derive(Debug)]
pub enum SessionError {
    Transport(TransportError),
    InvalidState {
        expected: SessionState,
        actual: SessionState,
    },
    ProtocolVersionMismatch {
        expected: u16,
        actual: u16,
    },
    InvalidAdapterIdentity,
    UnexpectedMessage {
        state: SessionState,
        message: &'static str,
    },
    Adapter {
        phase_id: Option<PhaseId>,
        code: String,
        message: String,
        retryable: bool,
    },
    BatchTooLarge {
        actual: usize,
        maximum: u32,
    },
    DuplicateScheduledOperation(OperationId),
    EmptyResults {
        phase_id: PhaseId,
    },
    UnexpectedPhase {
        expected: PhaseId,
        actual: PhaseId,
    },
    UnexpectedOperationResult(OperationId),
    DuplicateOperationResult(OperationId),
    OperationResultMismatch(OperationId),
}

impl From<TransportError> for SessionError {
    fn from(value: TransportError) -> Self {
        Self::Transport(value)
    }
}

impl fmt::Display for SessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(formatter),
            Self::InvalidState { expected, actual } => {
                write!(
                    formatter,
                    "adapter session is {actual:?}; expected {expected:?}"
                )
            }
            Self::ProtocolVersionMismatch { expected, actual } => write!(
                formatter,
                "adapter protocol version {actual} does not match controller version {expected}"
            ),
            Self::InvalidAdapterIdentity => {
                formatter.write_str("adapter identity name must not be empty")
            }
            Self::UnexpectedMessage { state, message } => {
                write!(
                    formatter,
                    "unexpected adapter message {message:?} while {state:?}"
                )
            }
            Self::Adapter {
                phase_id,
                code,
                message,
                retryable,
            } => write!(
                formatter,
                "adapter error {code:?} for phase {phase_id:?} (retryable={retryable}): {message}"
            ),
            Self::BatchTooLarge { actual, maximum } => write!(
                formatter,
                "scheduled batch contains {actual} operations but the adapter limit is {maximum}"
            ),
            Self::DuplicateScheduledOperation(id) => {
                write!(formatter, "operation {} was scheduled more than once", id.0)
            }
            Self::EmptyResults { phase_id } => {
                write!(
                    formatter,
                    "adapter returned an empty result batch for phase {}",
                    phase_id.0
                )
            }
            Self::UnexpectedPhase { expected, actual } => write!(
                formatter,
                "adapter returned results for phase {} while phase {} was active",
                actual.0, expected.0
            ),
            Self::UnexpectedOperationResult(id) => {
                write!(formatter, "adapter returned unscheduled operation {}", id.0)
            }
            Self::DuplicateOperationResult(id) => {
                write!(
                    formatter,
                    "adapter returned operation {} more than once",
                    id.0
                )
            }
            Self::OperationResultMismatch(id) => write!(
                formatter,
                "adapter result for operation {} does not match its scheduled operation",
                id.0
            ),
        }
    }
}

impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;

    use super::*;
    use crate::protocol::{LoadModel, OperationStatus};

    struct FakeTransport {
        sent: Arc<Mutex<Vec<ControllerMessage>>>,
        received: VecDeque<Result<AdapterMessage, TransportError>>,
    }

    impl FakeTransport {
        fn new(messages: Vec<AdapterMessage>) -> (Self, Arc<Mutex<Vec<ControllerMessage>>>) {
            let sent = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    sent: Arc::clone(&sent),
                    received: messages.into_iter().map(Ok).collect(),
                },
                sent,
            )
        }
    }

    impl AdapterTransport for FakeTransport {
        fn send(&mut self, message: &ControllerMessage) -> Result<(), TransportError> {
            self.sent.lock().unwrap().push(message.clone());
            Ok(())
        }

        fn receive(&mut self, _timeout: Duration) -> Result<AdapterMessage, TransportError> {
            self.received
                .pop_front()
                .unwrap_or(Err(TransportError::Closed))
        }

        fn diagnostics(&self) -> Vec<String> {
            Vec::new()
        }

        fn close(&mut self, _timeout: Duration) -> Result<(), TransportError> {
            Ok(())
        }
    }

    fn ready(version: u16) -> AdapterMessage {
        AdapterMessage::Ready {
            protocol_version: version,
            identity: AdapterIdentity {
                name: "fake-adapter".into(),
                version: Some("1.0.0".into()),
            },
            capabilities: Capabilities {
                scheduled_operations: true,
                adapter_managed_phases: false,
                load_models: vec![LoadModel::OpenLoop],
                max_batch_size: Some(8),
            },
            operations: Vec::new(),
        }
    }

    fn scheduled(id: u64) -> ScheduledOperation {
        ScheduledOperation {
            id: OperationId(id),
            operation: "read".into(),
            start_offset_ns: id * 100,
            arguments: Default::default(),
        }
    }

    fn result(id: u64) -> OperationResult {
        OperationResult {
            id: OperationId(id),
            operation: "read".into(),
            arguments: Default::default(),
            intended_start_offset_ns: id * 100,
            actual_start_offset_ns: id * 100 + 1,
            client_latency_ns: 10,
            status: OperationStatus::Ok,
        }
    }

    #[test]
    fn handshake_is_transport_independent() {
        let (transport, sent) = FakeTransport::new(vec![ready(PROTOCOL_VERSION)]);
        let mut session = AdapterSession::new(transport, SessionOptions::default());

        session
            .initialize(RunId(7), serde_json::json!({"target": "test"}))
            .unwrap();

        assert_eq!(session.state(), SessionState::Ready);
        assert!(matches!(
            &sent.lock().unwrap()[0],
            ControllerMessage::Initialize {
                run_id: RunId(7),
                ..
            }
        ));
    }

    #[test]
    fn protocol_mismatch_fails_the_session() {
        let (transport, _) = FakeTransport::new(vec![ready(PROTOCOL_VERSION + 1)]);
        let mut session = AdapterSession::new(transport, SessionOptions::default());

        assert!(matches!(
            session.initialize(RunId(1), Value::Null),
            Err(SessionError::ProtocolVersionMismatch { .. })
        ));
        assert_eq!(session.state(), SessionState::Failed);
    }

    #[test]
    fn empty_adapter_identity_fails_the_handshake() {
        let mut response = ready(PROTOCOL_VERSION);
        let AdapterMessage::Ready { identity, .. } = &mut response else {
            unreachable!();
        };
        identity.name.clear();
        let (transport, _) = FakeTransport::new(vec![response]);
        let mut session = AdapterSession::new(transport, SessionOptions::default());

        assert!(matches!(
            session.initialize(RunId(1), Value::Null),
            Err(SessionError::InvalidAdapterIdentity)
        ));
        assert_eq!(session.state(), SessionState::Failed);
    }

    #[test]
    fn scheduled_results_are_validated_and_restored_to_schedule_order() {
        let responses = vec![
            ready(PROTOCOL_VERSION),
            AdapterMessage::Results {
                phase_id: PhaseId(3),
                operations: vec![result(2), result(1)],
            },
        ];
        let (transport, _) = FakeTransport::new(responses);
        let mut session = AdapterSession::new(transport, SessionOptions::default());
        session.initialize(RunId(1), Value::Null).unwrap();

        let results = session
            .schedule(PhaseId(3), 42, vec![scheduled(1), scheduled(2)])
            .unwrap();

        assert_eq!(
            results.iter().map(|result| result.id.0).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(session.state(), SessionState::Ready);
    }

    #[test]
    fn duplicate_results_fail_the_session() {
        let responses = vec![
            ready(PROTOCOL_VERSION),
            AdapterMessage::Results {
                phase_id: PhaseId(3),
                operations: vec![result(1), result(1)],
            },
        ];
        let (transport, _) = FakeTransport::new(responses);
        let mut session = AdapterSession::new(transport, SessionOptions::default());
        session.initialize(RunId(1), Value::Null).unwrap();

        assert!(matches!(
            session.schedule(PhaseId(3), 42, vec![scheduled(1), scheduled(2)]),
            Err(SessionError::DuplicateOperationResult(OperationId(1)))
        ));
        assert_eq!(session.state(), SessionState::Failed);
    }

    #[test]
    fn adapter_errors_preserve_retryability_and_phase_context() {
        let responses = vec![
            ready(PROTOCOL_VERSION),
            AdapterMessage::Error {
                phase_id: Some(PhaseId(3)),
                code: "overloaded".into(),
                message: "try a lower rate".into(),
                retryable: true,
            },
        ];
        let (transport, _) = FakeTransport::new(responses);
        let mut session = AdapterSession::new(transport, SessionOptions::default());
        session.initialize(RunId(1), Value::Null).unwrap();

        assert!(matches!(
            session.schedule(PhaseId(3), 42, vec![scheduled(1)]),
            Err(SessionError::Adapter {
                phase_id: Some(PhaseId(3)),
                code,
                retryable: true,
                ..
            }) if code == "overloaded"
        ));
    }

    #[test]
    fn cancellation_uses_the_same_transport_without_closing_the_session() {
        let (transport, sent) = FakeTransport::new(vec![ready(PROTOCOL_VERSION)]);
        let mut session = AdapterSession::new(transport, SessionOptions::default());
        session.initialize(RunId(1), Value::Null).unwrap();

        session.cancel(PhaseId(9)).unwrap();

        assert!(matches!(
            &sent.lock().unwrap()[1],
            ControllerMessage::CancelPhase {
                phase_id: PhaseId(9)
            }
        ));
        assert_eq!(session.state(), SessionState::Ready);
    }

    #[test]
    fn frame_reader_enforces_bounds_and_newlines() {
        let mut valid = BufReader::new(&b"{}\n"[..]);
        assert_eq!(read_frame(&mut valid, 3).unwrap(), Some(b"{}\n".to_vec()));

        let mut oversized = BufReader::new(&b"1234\n"[..]);
        assert!(matches!(
            read_frame(&mut oversized, 4),
            Err(TransportError::FrameTooLarge { maximum: 4 })
        ));

        let mut truncated = BufReader::new(&b"{}"[..]);
        assert!(matches!(
            read_frame(&mut truncated, 4),
            Err(TransportError::TruncatedFrame)
        ));
    }

    #[test]
    fn tcp_transport_runs_the_existing_protocol_over_a_coordinator_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut input = BufReader::new(stream.try_clone().unwrap());
            let mut output = BufWriter::new(stream);
            let initialize = read_controller_message(&mut input);
            assert!(matches!(
                initialize,
                ControllerMessage::Initialize {
                    run_id: RunId(7),
                    ..
                }
            ));
            serde_json::to_writer(&mut output, &ready(PROTOCOL_VERSION)).unwrap();
            output.write_all(b"\n").unwrap();
            output.flush().unwrap();
            assert!(matches!(
                read_controller_message(&mut input),
                ControllerMessage::Shutdown
            ));
        });

        let options = SessionOptions::default();
        let transport = TcpTransport::connect(&endpoint.to_string(), &options).unwrap();
        let mut session = AdapterSession::new(transport, options);
        session.initialize(RunId(7), Value::Null).unwrap();
        session.shutdown().unwrap();
        server.join().unwrap();
    }

    #[test]
    fn tcp_handshake_times_out_when_an_agent_is_slow() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut input = BufReader::new(stream);
            let _ = read_controller_message(&mut input);
            thread::sleep(Duration::from_millis(150));
        });
        let options = SessionOptions {
            handshake_timeout: Duration::from_millis(25),
            ..SessionOptions::default()
        };
        let transport = TcpTransport::connect(&endpoint.to_string(), &options).unwrap();
        let mut session = AdapterSession::new(transport, options);

        assert!(matches!(
            session.initialize(RunId(1), Value::Null),
            Err(SessionError::Transport(TransportError::ReceiveTimeout(_)))
        ));
        server.join().unwrap();
    }

    #[test]
    fn tcp_handshake_reports_an_agent_disconnect() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut input = BufReader::new(stream);
            let _ = read_controller_message(&mut input);
        });
        let options = SessionOptions::default();
        let transport = TcpTransport::connect(&endpoint.to_string(), &options).unwrap();
        let mut session = AdapterSession::new(transport, options);

        assert!(matches!(
            session.initialize(RunId(1), Value::Null),
            Err(SessionError::Transport(TransportError::Closed))
        ));
        server.join().unwrap();
    }

    fn read_controller_message(reader: &mut impl BufRead) -> ControllerMessage {
        let mut line = String::new();
        assert_ne!(reader.read_line(&mut line).unwrap(), 0);
        serde_json::from_str(&line).unwrap()
    }
}
