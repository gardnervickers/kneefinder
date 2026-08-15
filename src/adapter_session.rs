//! Transport-independent adapter sessions and the default NDJSON subprocess transport.

use std::{
    collections::VecDeque,
    fmt,
    io::{self, BufRead, BufReader, BufWriter, Write},
    net::{Shutdown, TcpStream, ToSocketAddrs},
    process::{Child, ChildStdin, Command, ExitStatus, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

use crate::{
    config::AdapterCommand,
    protocol::{
        AdapterIdentity, AdapterMessage, Capabilities, ControllerMessage, HistogramEncoding, Load,
        LoadModel, ManagedPhaseRequest, OperationDescriptor, PROTOCOL_VERSION, PhaseCompletion,
        PhaseId, PhaseResult, RunId,
    },
};

const DEFAULT_MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_DIAGNOSTIC_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_DIAGNOSTIC_LINES: usize = 1_024;
const BUFFERED_ADAPTER_MESSAGES: usize = 8;
const INTERRUPT_POLL_INTERVAL: Duration = Duration::from_millis(25);
const MANAGED_CANCELLATION_LEAD_NS: u64 = 50_000_000;

#[derive(Debug, Clone)]
pub struct SessionOptions {
    pub connection_timeout: Duration,
    pub handshake_timeout: Duration,
    pub response_timeout: Duration,
    pub cancellation_timeout: Duration,
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
            cancellation_timeout: Duration::from_secs(1),
            shutdown_timeout: Duration::from_secs(5),
            maximum_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
            maximum_diagnostic_lines: DEFAULT_MAX_DIAGNOSTIC_LINES,
        }
    }
}

/// Message transport used by [`AdapterSession`]. Stdio and TCP use the same
/// handshake, phase execution, cancellation, and validation logic.
pub trait AdapterTransport: Send {
    fn send(&mut self, message: &ControllerMessage) -> Result<(), TransportError>;
    fn receive(&mut self, timeout: Duration) -> Result<AdapterMessage, TransportError>;
    fn diagnostics(&self) -> Vec<String>;
    fn close(&mut self, timeout: Duration) -> Result<(), TransportError>;
    fn abort(&mut self) -> Result<(), TransportError>;
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

    fn abort(&mut self) -> Result<(), TransportError> {
        self.input.take();
        if self.child.try_wait().map_err(TransportError::io)?.is_none() {
            self.child.kill().map_err(TransportError::io)?;
            self.child.wait().map_err(TransportError::io)?;
        }
        Ok(())
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

    fn abort(&mut self) -> Result<(), TransportError> {
        self.close(Duration::ZERO)
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
    PhasePreparing(PhaseId),
    PhasePrepared(PhaseId),
    PhaseActive(PhaseId),
    Failed,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleCompletion {
    Completed,
    Cancelled { forced: bool },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedPhaseOutcome {
    /// The adapter's aggregate. A forced cancellation has no aggregate because
    /// the coordinator never received a terminal phase message.
    pub result: Option<PhaseResult>,
    pub completion: ScheduleCompletion,
}

#[derive(Debug, Clone, Copy)]
struct PreparedManagedPhase {
    phase_id: PhaseId,
    response_wait: Duration,
}

/// Owns adapter protocol state and validation independently of how frames are
/// transported.
pub struct AdapterSession<T> {
    transport: T,
    options: SessionOptions,
    state: SessionState,
    capabilities: Option<Capabilities>,
    prepared_managed_phase: Option<PreparedManagedPhase>,
}

impl<T: AdapterTransport> AdapterSession<T> {
    pub fn new(transport: T, options: SessionOptions) -> Self {
        Self {
            transport,
            options,
            state: SessionState::New,
            capabilities: None,
            prepared_managed_phase: None,
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

    pub fn prepare_managed_phase(
        &mut self,
        phase_id: PhaseId,
        request: ManagedPhaseRequest,
    ) -> Result<(), SessionError> {
        self.require_state(SessionState::Ready)?;
        let capabilities = self
            .capabilities
            .as_ref()
            .expect("a ready session retained its negotiated capabilities");
        if !capabilities.adapter_managed_phases {
            return Err(SessionError::UnsupportedCapability(
                "adapter-managed phases",
            ));
        }
        let load_model = match &request.load {
            Load::OpenLoop { .. } => LoadModel::OpenLoop,
            Load::ClosedLoop { .. } => LoadModel::ClosedLoop,
        };
        if load_model != LoadModel::OpenLoop || !capabilities.load_models.contains(&load_model) {
            return Err(SessionError::UnsupportedLoadModel(load_model));
        }
        if !capabilities
            .histogram_encodings
            .contains(&HistogramEncoding::HdrV2Base64)
        {
            return Err(SessionError::UnsupportedHistogramEncoding(
                HistogramEncoding::HdrV2Base64,
            ));
        }
        let phase_ns = request
            .warmup_ns
            .checked_add(request.measurement_ns)
            .and_then(|duration| duration.checked_add(request.operation_timeout_ns))
            .ok_or(SessionError::ManagedPhaseDurationOverflow)?;
        let response_wait = Duration::from_nanos(phase_ns)
            .checked_add(self.options.response_timeout)
            .ok_or(SessionError::ManagedPhaseDurationOverflow)?;

        self.send(&ControllerMessage::PreparePhase { phase_id, request })?;
        self.state = SessionState::PhasePreparing(phase_id);
        let message = self.receive(self.options.response_timeout)?;
        match message {
            AdapterMessage::PhaseReady {
                phase_id: actual_phase,
            } if actual_phase == phase_id => {
                self.state = SessionState::PhasePrepared(phase_id);
                self.prepared_managed_phase = Some(PreparedManagedPhase {
                    phase_id,
                    response_wait,
                });
                Ok(())
            }
            AdapterMessage::PhaseReady {
                phase_id: actual, ..
            } => self.fail(SessionError::UnexpectedPhase {
                expected: phase_id,
                actual,
            }),
            AdapterMessage::Error {
                phase_id: Some(actual),
                ..
            } if actual != phase_id => self.fail(SessionError::UnexpectedPhase {
                expected: phase_id,
                actual,
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
                state: self.state,
                message: message_kind(&message),
            }),
        }
    }

    pub fn start_managed_phase_interruptible(
        &mut self,
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
        stop: &AtomicBool,
    ) -> Result<ManagedPhaseOutcome, SessionError> {
        let cancel_at_unix_ns = AtomicU64::new(0);
        self.start_managed_phase_interruptible_with_cutoff(
            phase_id,
            phase_start_unix_ns,
            stop,
            &cancel_at_unix_ns,
        )
    }

    pub fn start_managed_phase_interruptible_with_cutoff(
        &mut self,
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
        stop: &AtomicBool,
        cancel_at_unix_ns: &AtomicU64,
    ) -> Result<ManagedPhaseOutcome, SessionError> {
        self.require_state(SessionState::PhasePrepared(phase_id))?;
        let prepared = self
            .prepared_managed_phase
            .expect("a prepared session retained its managed phase deadline");
        debug_assert_eq!(prepared.phase_id, phase_id);

        let start_delay = Duration::from_nanos(phase_start_unix_ns.saturating_sub(unix_now_ns()));
        let response_wait = prepared
            .response_wait
            .checked_add(start_delay)
            .ok_or(SessionError::ManagedPhaseDurationOverflow)?;
        let response_deadline = Instant::now()
            .checked_add(response_wait)
            .ok_or(SessionError::ManagedPhaseDurationOverflow)?;
        let mut cancellation_deadline = None;
        let mut started = false;
        let start_requested = !stop.load(Ordering::Acquire);
        if !start_requested {
            let cutoff = managed_cancellation_cutoff(cancel_at_unix_ns);
            if self
                .transport
                .send(&ControllerMessage::CancelPhase {
                    phase_id,
                    cancel_at_unix_ns: Some(cutoff),
                })
                .is_err()
            {
                return Ok(self.force_managed_cancellation());
            }
            cancellation_deadline = Some(managed_cancellation_deadline(
                cutoff,
                self.options.cancellation_timeout,
            ));
        } else {
            self.send(&ControllerMessage::StartPhase {
                phase_id,
                phase_start_unix_ns,
            })?;
        }
        self.state = SessionState::PhaseActive(phase_id);

        loop {
            if stop.load(Ordering::Acquire) && cancellation_deadline.is_none() {
                let cutoff = managed_cancellation_cutoff(cancel_at_unix_ns);
                if self
                    .transport
                    .send(&ControllerMessage::CancelPhase {
                        phase_id,
                        cancel_at_unix_ns: Some(cutoff),
                    })
                    .is_err()
                {
                    return Ok(self.force_managed_cancellation());
                }
                cancellation_deadline = Some(managed_cancellation_deadline(
                    cutoff,
                    self.options.cancellation_timeout,
                ));
            }

            let now = Instant::now();
            if cancellation_deadline.is_some_and(|deadline| now >= deadline) {
                return Ok(self.force_managed_cancellation());
            }
            if cancellation_deadline.is_none() && now >= response_deadline {
                return self.fail(SessionError::Transport(TransportError::ReceiveTimeout(
                    response_wait,
                )));
            }

            let active_deadline = cancellation_deadline.unwrap_or(response_deadline);
            let wait = active_deadline
                .saturating_duration_since(now)
                .min(INTERRUPT_POLL_INTERVAL);
            let message = match self.transport.receive(wait) {
                Ok(message) => message,
                Err(TransportError::ReceiveTimeout(_)) => continue,
                Err(_) if cancellation_deadline.is_some() => {
                    let _ = self.transport.abort();
                    self.state = SessionState::Closed;
                    self.prepared_managed_phase = None;
                    return Ok(ManagedPhaseOutcome {
                        result: None,
                        completion: ScheduleCompletion::Cancelled { forced: true },
                    });
                }
                Err(error) => {
                    self.state = SessionState::Failed;
                    return Err(SessionError::Transport(error));
                }
            };
            match message {
                AdapterMessage::PhaseStarted {
                    phase_id: actual_phase,
                } if actual_phase == phase_id && start_requested && !started => {
                    started = true;
                }
                AdapterMessage::PhaseStarted {
                    phase_id: actual, ..
                }
                | AdapterMessage::PhaseReady {
                    phase_id: actual, ..
                }
                | AdapterMessage::PhaseComplete {
                    phase_id: actual, ..
                } if actual != phase_id => {
                    return self.fail(SessionError::UnexpectedPhase {
                        expected: phase_id,
                        actual,
                    });
                }
                AdapterMessage::PhaseComplete {
                    phase_id: actual,
                    completion,
                    result,
                } => {
                    debug_assert_eq!(actual, phase_id);
                    if completion == PhaseCompletion::Cancelled && cancellation_deadline.is_none() {
                        return self.fail(SessionError::UnsolicitedPhaseCancellation(phase_id));
                    }
                    if completion == PhaseCompletion::Completed && !started {
                        return self.fail(SessionError::UnexpectedMessage {
                            state: self.state,
                            message: "phase_complete",
                        });
                    }
                    self.state = SessionState::Ready;
                    self.prepared_managed_phase = None;
                    return Ok(ManagedPhaseOutcome {
                        result: Some(result),
                        completion: match completion {
                            PhaseCompletion::Completed => ScheduleCompletion::Completed,
                            PhaseCompletion::Cancelled => {
                                ScheduleCompletion::Cancelled { forced: false }
                            }
                        },
                    });
                }
                AdapterMessage::Error {
                    phase_id: Some(actual),
                    ..
                } if actual != phase_id => {
                    return self.fail(SessionError::UnexpectedPhase {
                        expected: phase_id,
                        actual,
                    });
                }
                AdapterMessage::Error {
                    phase_id,
                    code,
                    message,
                    retryable,
                } => {
                    return self.fail(SessionError::Adapter {
                        phase_id,
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
    }

    pub fn cancel(&mut self, phase_id: PhaseId) -> Result<(), SessionError> {
        match self.state {
            SessionState::Ready => {
                self.send(&ControllerMessage::CancelPhase {
                    phase_id,
                    cancel_at_unix_ns: None,
                })?;
                Ok(())
            }
            SessionState::PhasePrepared(active) | SessionState::PhaseActive(active)
                if active == phase_id =>
            {
                self.send(&ControllerMessage::CancelPhase {
                    phase_id,
                    cancel_at_unix_ns: None,
                })?;
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
        self.prepared_managed_phase = None;
        send.and(close).map_err(SessionError::Transport)
    }

    /// Closes this coordinator-owned session without asking a remote agent
    /// process to terminate. A colocated subprocess observes EOF and exits;
    /// a persistent TCP agent returns to accepting coordinator connections.
    pub fn disconnect(&mut self) -> Result<(), SessionError> {
        if self.state == SessionState::Closed {
            return Ok(());
        }
        let close = self.transport.close(self.options.shutdown_timeout);
        self.state = SessionState::Closed;
        self.prepared_managed_phase = None;
        close.map_err(SessionError::Transport)
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
        self.prepared_managed_phase = None;
        Err(error)
    }

    fn force_managed_cancellation(&mut self) -> ManagedPhaseOutcome {
        let _ = self.transport.abort();
        self.state = SessionState::Closed;
        self.prepared_managed_phase = None;
        ManagedPhaseOutcome {
            result: None,
            completion: ScheduleCompletion::Cancelled { forced: true },
        }
    }
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn managed_cancellation_cutoff(cancel_at_unix_ns: &AtomicU64) -> u64 {
    let existing = cancel_at_unix_ns.load(Ordering::Acquire);
    if existing != 0 {
        return existing;
    }
    let cutoff = unix_now_ns().saturating_add(MANAGED_CANCELLATION_LEAD_NS);
    cancel_at_unix_ns
        .compare_exchange(0, cutoff, Ordering::AcqRel, Ordering::Acquire)
        .unwrap_or_else(|actual| actual)
}

fn managed_cancellation_deadline(cutoff_unix_ns: u64, timeout: Duration) -> Instant {
    let until_cutoff = Duration::from_nanos(cutoff_unix_ns.saturating_sub(unix_now_ns()));
    Instant::now() + until_cutoff + timeout
}

fn message_kind(message: &AdapterMessage) -> &'static str {
    match message {
        AdapterMessage::Ready { .. } => "ready",
        AdapterMessage::PhaseReady { .. } => "phase_ready",
        AdapterMessage::PhaseStarted { .. } => "phase_started",
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
    UnexpectedPhase {
        expected: PhaseId,
        actual: PhaseId,
    },
    UnsupportedCapability(&'static str),
    UnsupportedLoadModel(LoadModel),
    UnsupportedHistogramEncoding(HistogramEncoding),
    ManagedPhaseDurationOverflow,
    UnsolicitedPhaseCancellation(PhaseId),
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
            Self::UnexpectedPhase { expected, actual } => write!(
                formatter,
                "adapter returned a message for phase {} while phase {} was active",
                actual.0, expected.0
            ),
            Self::UnsupportedCapability(capability) => {
                write!(formatter, "adapter does not support {capability}")
            }
            Self::UnsupportedLoadModel(model) => {
                write!(
                    formatter,
                    "adapter does not support the {model:?} load model"
                )
            }
            Self::UnsupportedHistogramEncoding(encoding) => write!(
                formatter,
                "adapter does not support the {encoding:?} histogram encoding"
            ),
            Self::ManagedPhaseDurationOverflow => {
                formatter.write_str("managed phase duration exceeds the supported range")
            }
            Self::UnsolicitedPhaseCancellation(phase_id) => write!(
                formatter,
                "adapter cancelled phase {} without a coordinator cancellation request",
                phase_id.0
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
    use crate::protocol::{
        EncodedHistogram, HistogramSpec, LoadModel, ManagedOperation, PhaseErrorCount, TimeBucket,
    };

    struct FakeTransport {
        sent: Arc<Mutex<Vec<ControllerMessage>>>,
        received: VecDeque<Result<AdapterMessage, TransportError>>,
        timeout_when_empty: bool,
        aborted: Arc<AtomicBool>,
    }

    impl FakeTransport {
        fn new(messages: Vec<AdapterMessage>) -> (Self, Arc<Mutex<Vec<ControllerMessage>>>) {
            let sent = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    sent: Arc::clone(&sent),
                    received: messages.into_iter().map(Ok).collect(),
                    timeout_when_empty: false,
                    aborted: Arc::new(AtomicBool::new(false)),
                },
                sent,
            )
        }

        fn interruptible(
            messages: Vec<AdapterMessage>,
        ) -> (Self, Arc<Mutex<Vec<ControllerMessage>>>, Arc<AtomicBool>) {
            let (mut transport, sent) = Self::new(messages);
            let aborted = Arc::clone(&transport.aborted);
            transport.timeout_when_empty = true;
            (transport, sent, aborted)
        }
    }

    impl AdapterTransport for FakeTransport {
        fn send(&mut self, message: &ControllerMessage) -> Result<(), TransportError> {
            self.sent.lock().unwrap().push(message.clone());
            Ok(())
        }

        fn receive(&mut self, _timeout: Duration) -> Result<AdapterMessage, TransportError> {
            if let Some(message) = self.received.pop_front() {
                return message;
            }
            if self.timeout_when_empty {
                thread::sleep(_timeout);
                Err(TransportError::ReceiveTimeout(_timeout))
            } else {
                Err(TransportError::Closed)
            }
        }

        fn diagnostics(&self) -> Vec<String> {
            Vec::new()
        }

        fn close(&mut self, _timeout: Duration) -> Result<(), TransportError> {
            Ok(())
        }

        fn abort(&mut self) -> Result<(), TransportError> {
            self.aborted.store(true, Ordering::Release);
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
                adapter_managed_phases: false,
                load_models: vec![LoadModel::OpenLoop],
                histogram_encodings: Vec::new(),
            },
            operations: Vec::new(),
        }
    }

    fn managed_ready(version: u16) -> AdapterMessage {
        let mut message = ready(version);
        let AdapterMessage::Ready { capabilities, .. } = &mut message else {
            unreachable!();
        };
        capabilities.adapter_managed_phases = true;
        capabilities.histogram_encodings = vec![HistogramEncoding::HdrV2Base64];
        message
    }

    fn managed_request() -> ManagedPhaseRequest {
        ManagedPhaseRequest {
            warmup_ns: 10,
            measurement_ns: 100,
            operation_timeout_ns: 20,
            load: Load::OpenLoop {
                requests_per_second: 1_000.0,
            },
            operations: vec![ManagedOperation {
                operation: "read".into(),
                arguments: Default::default(),
                weight: 1.0,
                shard_index: 0,
                shard_count: 1,
            }],
            bucket_count: 2,
            histogram: HistogramSpec {
                lowest_discernible_ns: 1,
                highest_trackable_ns: 1_000_000,
                significant_figures: 3,
            },
        }
    }

    fn encoded_histogram() -> EncodedHistogram {
        EncodedHistogram {
            encoding: HistogramEncoding::HdrV2Base64,
            data: String::new(),
        }
    }

    fn phase_result() -> PhaseResult {
        PhaseResult {
            offered: 1,
            started: 1,
            completed: 1,
            successful: 1,
            successful_in_window: 1,
            failed: 0,
            timed_out: 0,
            errors_by_code: Vec::<PhaseErrorCount>::new(),
            elapsed_ns: 100,
            in_flight_high_water: 1,
            client_latency: encoded_histogram(),
            total_latency: encoded_histogram(),
            dispatch_lag: encoded_histogram(),
            time_buckets: vec![TimeBucket {
                start_offset_ns: 0,
                duration_ns: 100,
                offered: 1,
                started: 1,
                completed: 1,
                successful: 1,
                failed: 0,
                timed_out: 0,
                in_flight_high_water: 1,
            }],
            per_operation: Vec::new(),
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
    fn managed_phase_requires_negotiated_capabilities_before_sending() {
        let (transport, sent) = FakeTransport::new(vec![ready(PROTOCOL_VERSION)]);
        let mut session = AdapterSession::new(transport, SessionOptions::default());
        session.initialize(RunId(1), Value::Null).unwrap();

        assert!(matches!(
            session.prepare_managed_phase(PhaseId(4), managed_request()),
            Err(SessionError::UnsupportedCapability(
                "adapter-managed phases"
            ))
        ));
        assert_eq!(session.state(), SessionState::Ready);
        assert_eq!(sent.lock().unwrap().len(), 1);
    }

    #[test]
    fn managed_phase_prepares_starts_and_completes_in_order() {
        let result = phase_result();
        let responses = vec![
            managed_ready(PROTOCOL_VERSION),
            AdapterMessage::PhaseReady {
                phase_id: PhaseId(4),
            },
            AdapterMessage::PhaseStarted {
                phase_id: PhaseId(4),
            },
            AdapterMessage::PhaseComplete {
                phase_id: PhaseId(4),
                completion: PhaseCompletion::Completed,
                result: result.clone(),
            },
        ];
        let (transport, sent) = FakeTransport::new(responses);
        let mut session = AdapterSession::new(transport, SessionOptions::default());
        session.initialize(RunId(1), Value::Null).unwrap();

        session
            .prepare_managed_phase(PhaseId(4), managed_request())
            .unwrap();
        assert_eq!(session.state(), SessionState::PhasePrepared(PhaseId(4)));
        let outcome = session
            .start_managed_phase_interruptible(PhaseId(4), 42_000, &AtomicBool::new(false))
            .unwrap();

        assert_eq!(
            outcome,
            ManagedPhaseOutcome {
                result: Some(result),
                completion: ScheduleCompletion::Completed,
            }
        );
        assert_eq!(session.state(), SessionState::Ready);
        assert!(matches!(
            &sent.lock().unwrap()[1],
            ControllerMessage::PreparePhase {
                phase_id: PhaseId(4),
                ..
            }
        ));
        assert!(matches!(
            &sent.lock().unwrap()[2],
            ControllerMessage::StartPhase {
                phase_id: PhaseId(4),
                phase_start_unix_ns: 42_000,
            }
        ));
    }

    #[test]
    fn managed_phase_can_be_cancelled_while_prepared_without_starting() {
        let result = phase_result();
        let responses = vec![
            managed_ready(PROTOCOL_VERSION),
            AdapterMessage::PhaseReady {
                phase_id: PhaseId(4),
            },
            AdapterMessage::PhaseComplete {
                phase_id: PhaseId(4),
                completion: PhaseCompletion::Cancelled,
                result: result.clone(),
            },
        ];
        let (transport, sent) = FakeTransport::new(responses);
        let mut session = AdapterSession::new(transport, SessionOptions::default());
        session.initialize(RunId(1), Value::Null).unwrap();
        session
            .prepare_managed_phase(PhaseId(4), managed_request())
            .unwrap();

        let cutoff = unix_now_ns().saturating_add(50_000_000);
        let outcome = session
            .start_managed_phase_interruptible_with_cutoff(
                PhaseId(4),
                42_000,
                &AtomicBool::new(true),
                &AtomicU64::new(cutoff),
            )
            .unwrap();

        assert_eq!(outcome.result, Some(result));
        assert_eq!(
            outcome.completion,
            ScheduleCompletion::Cancelled { forced: false }
        );
        assert_eq!(session.state(), SessionState::Ready);
        assert!(sent.lock().unwrap().iter().any(|message| matches!(
            message,
            ControllerMessage::CancelPhase {
                phase_id: PhaseId(4),
                cancel_at_unix_ns: Some(actual),
            } if *actual == cutoff
        )));
        assert!(
            !sent
                .lock()
                .unwrap()
                .iter()
                .any(|message| matches!(message, ControllerMessage::StartPhase { .. }))
        );
    }

    #[test]
    fn managed_phase_cancellation_deadline_aborts_without_fabricating_a_result() {
        let responses = vec![
            managed_ready(PROTOCOL_VERSION),
            AdapterMessage::PhaseReady {
                phase_id: PhaseId(4),
            },
        ];
        let (transport, _, aborted) = FakeTransport::interruptible(responses);
        let options = SessionOptions {
            cancellation_timeout: Duration::from_millis(20),
            ..SessionOptions::default()
        };
        let mut session = AdapterSession::new(transport, options);
        session.initialize(RunId(1), Value::Null).unwrap();
        session
            .prepare_managed_phase(PhaseId(4), managed_request())
            .unwrap();

        let outcome = session
            .start_managed_phase_interruptible(PhaseId(4), 42_000, &AtomicBool::new(true))
            .unwrap();

        assert_eq!(outcome.result, None);
        assert_eq!(
            outcome.completion,
            ScheduleCompletion::Cancelled { forced: true }
        );
        assert!(aborted.load(Ordering::Acquire));
        assert_eq!(session.state(), SessionState::Closed);
    }

    #[test]
    fn unsolicited_managed_phase_cancellation_fails_the_session() {
        let responses = vec![
            managed_ready(PROTOCOL_VERSION),
            AdapterMessage::PhaseReady {
                phase_id: PhaseId(4),
            },
            AdapterMessage::PhaseStarted {
                phase_id: PhaseId(4),
            },
            AdapterMessage::PhaseComplete {
                phase_id: PhaseId(4),
                completion: PhaseCompletion::Cancelled,
                result: phase_result(),
            },
        ];
        let (transport, _) = FakeTransport::new(responses);
        let mut session = AdapterSession::new(transport, SessionOptions::default());
        session.initialize(RunId(1), Value::Null).unwrap();
        session
            .prepare_managed_phase(PhaseId(4), managed_request())
            .unwrap();

        assert!(matches!(
            session.start_managed_phase_interruptible(PhaseId(4), 42_000, &AtomicBool::new(false)),
            Err(SessionError::UnsolicitedPhaseCancellation(PhaseId(4)))
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
    fn adapter_errors_preserve_retryability_and_phase_context() {
        let responses = vec![
            managed_ready(PROTOCOL_VERSION),
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
            session.prepare_managed_phase(PhaseId(3), managed_request()),
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
                phase_id: PhaseId(9),
                cancel_at_unix_ns: None,
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
    fn disconnect_closes_the_transport_without_sending_shutdown() {
        let (transport, sent) = FakeTransport::new(vec![ready(PROTOCOL_VERSION)]);
        let mut session = AdapterSession::new(transport, SessionOptions::default());

        session.initialize(RunId(7), Value::Null).unwrap();
        session.disconnect().unwrap();

        assert_eq!(session.state(), SessionState::Closed);
        assert_eq!(sent.lock().unwrap().len(), 1);
        assert!(matches!(
            sent.lock().unwrap().first(),
            Some(ControllerMessage::Initialize { .. })
        ));
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
