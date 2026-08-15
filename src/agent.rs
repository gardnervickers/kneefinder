//! Workload-agent abstraction and coordinator-owned colocated implementation.

use std::{
    collections::BTreeMap,
    fmt,
    sync::atomic::{AtomicBool, AtomicU64, Ordering},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    adapter_session::{
        AdapterReady, AdapterSession, AdapterTransport, ManagedPhaseOutcome, SessionError,
        SessionOptions, SubprocessTransport, TcpTransport,
    },
    config::{AdapterCommand, AgentEndpointConfig, AgentTransportConfig},
    protocol::{
        AdapterIdentity, Capabilities, Load, ManagedPhaseRequest, OperationDescriptor, PhaseId,
        RunId,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentInstanceId(pub String);

static NEXT_AGENT_INSTANCE: AtomicU64 = AtomicU64::new(1);
const MANAGED_CANCELLATION_LEAD_NS: u64 = 50_000_000;

impl AgentId {
    pub fn new(value: impl Into<String>) -> Result<Self, CohortError> {
        let value = value.into();
        if value.is_empty() {
            Err(CohortError::EmptyAgentId)
        } else {
            Ok(Self(value))
        }
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentPlacement {
    /// Agent execution and lifecycle are owned by the coordinator process.
    Colocated,
    /// Agent execution is hosted by a separately deployed worker process.
    Remote,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDescriptor {
    /// Stable identity used for load allocation and result attribution.
    pub id: AgentId,
    /// Unique identity for this process incarnation so restarts are visible.
    pub instance_id: AgentInstanceId,
    pub placement: AgentPlacement,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AgentReady {
    pub agent: AgentDescriptor,
    pub adapter: AdapterReady,
}

/// Coordinator-side agent interface. Every operation is initiated by the
/// coordinator. A remote implementation connects to an explicitly configured
/// agent endpoint; agents never establish sessions back to the coordinator.
pub trait WorkloadAgent: Send {
    fn descriptor(&self) -> &AgentDescriptor;

    fn initialize(&mut self, run_id: RunId, config: Value) -> Result<AgentReady, AgentError>;

    fn prepare_managed_phase(
        &mut self,
        phase_id: PhaseId,
        request: ManagedPhaseRequest,
    ) -> Result<(), AgentError>;

    fn start_managed_phase_interruptible(
        &mut self,
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
        stop: &AtomicBool,
    ) -> Result<ManagedPhaseOutcome, AgentError>;

    /// Start a managed phase with a cohort-shared absolute cancellation
    /// cutoff. Implementations that do not need it retain the original entry
    /// point.
    fn start_managed_phase_interruptible_with_cutoff(
        &mut self,
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
        stop: &AtomicBool,
        _cancel_at_unix_ns: &AtomicU64,
    ) -> Result<ManagedPhaseOutcome, AgentError> {
        self.start_managed_phase_interruptible(phase_id, phase_start_unix_ns, stop)
    }

    fn cancel(&mut self, phase_id: PhaseId) -> Result<(), AgentError>;

    /// Ends the current coordinator-owned session without terminating a
    /// separately deployed agent process.
    fn disconnect(&mut self) -> Result<(), AgentError>;

    /// Explicitly asks the agent process to terminate.
    fn shutdown(&mut self) -> Result<(), AgentError>;

    fn diagnostics(&self) -> Vec<String>;
}

/// Thin coordinator-side wrapper around one transport-backed adapter session.
pub struct SessionAgent<T> {
    descriptor: AgentDescriptor,
    session: AdapterSession<T>,
}

pub type ColocatedAgent = SessionAgent<SubprocessTransport>;
pub type TcpAgent = SessionAgent<TcpTransport>;

impl SessionAgent<SubprocessTransport> {
    pub fn spawn(
        id: AgentId,
        command: &AdapterCommand,
        options: SessionOptions,
    ) -> Result<Self, AgentError> {
        let transport = SubprocessTransport::spawn(command, &options)?;
        Ok(Self {
            descriptor: agent_descriptor(id, AgentPlacement::Colocated),
            session: AdapterSession::new(transport, options),
        })
    }
}

impl SessionAgent<TcpTransport> {
    pub fn connect(
        id: AgentId,
        endpoint: &str,
        options: SessionOptions,
    ) -> Result<Self, AgentError> {
        let transport = TcpTransport::connect(endpoint, &options)?;
        Ok(Self {
            descriptor: agent_descriptor(id, AgentPlacement::Remote),
            session: AdapterSession::new(transport, options),
        })
    }
}

fn agent_descriptor(id: AgentId, placement: AgentPlacement) -> AgentDescriptor {
    AgentDescriptor {
        instance_id: AgentInstanceId(format!(
            "{}-{}-{}",
            id.0,
            std::process::id(),
            NEXT_AGENT_INSTANCE.fetch_add(1, Ordering::Relaxed)
        )),
        id,
        placement,
    }
}

impl<T: AdapterTransport> WorkloadAgent for SessionAgent<T> {
    fn descriptor(&self) -> &AgentDescriptor {
        &self.descriptor
    }

    fn initialize(&mut self, run_id: RunId, config: Value) -> Result<AgentReady, AgentError> {
        let adapter = self.session.initialize(run_id, config)?;
        Ok(AgentReady {
            agent: self.descriptor.clone(),
            adapter,
        })
    }

    fn prepare_managed_phase(
        &mut self,
        phase_id: PhaseId,
        request: ManagedPhaseRequest,
    ) -> Result<(), AgentError> {
        self.session
            .prepare_managed_phase(phase_id, request)
            .map_err(Into::into)
    }

    fn start_managed_phase_interruptible(
        &mut self,
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
        stop: &AtomicBool,
    ) -> Result<ManagedPhaseOutcome, AgentError> {
        self.session
            .start_managed_phase_interruptible(phase_id, phase_start_unix_ns, stop)
            .map_err(Into::into)
    }

    fn start_managed_phase_interruptible_with_cutoff(
        &mut self,
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
        stop: &AtomicBool,
        cancel_at_unix_ns: &AtomicU64,
    ) -> Result<ManagedPhaseOutcome, AgentError> {
        self.session
            .start_managed_phase_interruptible_with_cutoff(
                phase_id,
                phase_start_unix_ns,
                stop,
                cancel_at_unix_ns,
            )
            .map_err(Into::into)
    }

    fn cancel(&mut self, phase_id: PhaseId) -> Result<(), AgentError> {
        self.session.cancel(phase_id).map_err(Into::into)
    }

    fn disconnect(&mut self) -> Result<(), AgentError> {
        self.session.disconnect().map_err(Into::into)
    }

    fn shutdown(&mut self) -> Result<(), AgentError> {
        self.session.shutdown().map_err(Into::into)
    }

    fn diagnostics(&self) -> Vec<String> {
        self.session.diagnostics()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CohortReady {
    pub agents: Vec<AgentDescriptor>,
    pub adapter: AdapterIdentity,
    pub capabilities: Capabilities,
    pub operations: Vec<OperationDescriptor>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentManagedPhaseResult {
    pub agent: AgentDescriptor,
    pub outcome: ManagedPhaseOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CohortManagedPhaseResult {
    pub agents: Vec<AgentManagedPhaseResult>,
}

/// A fixed agent cohort. Membership is frozen when constructed; a failed
/// member invalidates a phase instead of silently redistributing its load.
pub struct AgentCohort {
    agents: Vec<Box<dyn WorkloadAgent>>,
    initialized: bool,
}

impl AgentCohort {
    /// Establishes the configured fixed cohort. Subprocess members are spawned
    /// by the coordinator and TCP members are connected by the coordinator.
    pub fn from_endpoints(
        endpoints: &[AgentEndpointConfig],
        options: SessionOptions,
    ) -> Result<Self, CohortError> {
        let mut agents = Vec::<Box<dyn WorkloadAgent>>::with_capacity(endpoints.len());
        for endpoint in endpoints {
            let id = AgentId::new(endpoint.id.clone())?;
            let agent: Box<dyn WorkloadAgent> = match &endpoint.transport {
                AgentTransportConfig::Subprocess { command } => Box::new(
                    ColocatedAgent::spawn(id.clone(), command, options.clone()).map_err(
                        |source| CohortError::AgentFailed {
                            id: id.clone(),
                            source,
                        },
                    )?,
                ),
                AgentTransportConfig::Tcp { address } => Box::new(
                    TcpAgent::connect(id.clone(), address, options.clone()).map_err(|source| {
                        CohortError::AgentFailed {
                            id: id.clone(),
                            source,
                        }
                    })?,
                ),
            };
            agents.push(agent);
        }
        Self::new(agents)
    }

    pub fn new(agents: Vec<Box<dyn WorkloadAgent>>) -> Result<Self, CohortError> {
        if agents.is_empty() {
            return Err(CohortError::EmptyCohort);
        }
        let mut identities = BTreeMap::new();
        for agent in &agents {
            let id = &agent.descriptor().id;
            if id.0.is_empty() {
                return Err(CohortError::EmptyAgentId);
            }
            if identities.insert(id.clone(), ()).is_some() {
                return Err(CohortError::DuplicateAgent(id.clone()));
            }
        }
        Ok(Self {
            agents,
            initialized: false,
        })
    }

    pub fn descriptors(&self) -> Vec<AgentDescriptor> {
        self.agents
            .iter()
            .map(|agent| agent.descriptor().clone())
            .collect()
    }

    pub fn initialize(&mut self, run_id: RunId, config: Value) -> Result<CohortReady, CohortError> {
        if self.initialized {
            return Err(CohortError::AlreadyInitialized);
        }
        let mut ready = Vec::with_capacity(self.agents.len());
        for agent in &mut self.agents {
            let id = agent.descriptor().id.clone();
            ready.push(
                agent
                    .initialize(run_id, config.clone())
                    .map_err(|source| CohortError::AgentFailed { id, source })?,
            );
        }

        let reference = ready
            .first()
            .expect("a cohort is validated as non-empty before initialization");
        let reference_schema = operation_schema(&reference.adapter.operations);
        for candidate in ready.iter().skip(1) {
            if candidate.adapter.identity != reference.adapter.identity {
                return Err(CohortError::AdapterIdentityMismatch {
                    expected: reference.agent.id.clone(),
                    actual: candidate.agent.id.clone(),
                });
            }
            if candidate.adapter.capabilities != reference.adapter.capabilities {
                return Err(CohortError::CapabilitiesMismatch {
                    expected: reference.agent.id.clone(),
                    actual: candidate.agent.id.clone(),
                });
            }
            if operation_schema(&candidate.adapter.operations) != reference_schema {
                return Err(CohortError::OperationSchemaMismatch {
                    expected: reference.agent.id.clone(),
                    actual: candidate.agent.id.clone(),
                });
            }
        }

        self.initialized = true;
        Ok(CohortReady {
            agents: ready.iter().map(|agent| agent.agent.clone()).collect(),
            adapter: reference.adapter.identity.clone(),
            capabilities: reference.adapter.capabilities.clone(),
            operations: reference.adapter.operations.clone(),
        })
    }

    pub fn execute_managed_phase(
        &mut self,
        phase_id: PhaseId,
        schedule_lead_time: Duration,
        request: ManagedPhaseRequest,
        stop: &AtomicBool,
    ) -> Result<CohortManagedPhaseResult, CohortError> {
        if !self.initialized {
            return Err(CohortError::NotInitialized);
        }
        if !matches!(&request.load, Load::OpenLoop { .. }) {
            return Err(CohortError::ManagedPhaseRequiresOpenLoop);
        }
        let shard_count = u32::try_from(self.agents.len())
            .map_err(|_| CohortError::ManagedPhaseCohortTooLarge)?;

        for (index, agent) in self.agents.iter_mut().enumerate() {
            let mut assigned = request.clone();
            let shard_index = u32::try_from(index)
                .expect("an agent index fits when the cohort length fits in u32");
            for operation in &mut assigned.operations {
                operation.shard_index = shard_index;
                operation.shard_count = shard_count;
            }
            let id = agent.descriptor().id.clone();
            if let Err(source) = agent.prepare_managed_phase(phase_id, assigned) {
                for prepared_agent in &mut self.agents[..index] {
                    let _ = prepared_agent.cancel(phase_id);
                }
                return Err(CohortError::AgentFailed { id, source });
            }
        }

        let phase_start_unix_ns = unix_now_ns()
            .saturating_add(schedule_lead_time.as_nanos().min(u64::MAX as u128) as u64);

        let initially_stopped = stop.load(Ordering::Acquire);
        let cancel_at_unix_ns = AtomicU64::new(if initially_stopped {
            managed_cancellation_cutoff()
        } else {
            0
        });
        let cohort_stop = AtomicBool::new(initially_stopped);
        let calls_finished = AtomicBool::new(false);
        thread::scope(|scope| {
            let watcher = scope.spawn(|| {
                while !calls_finished.load(Ordering::Acquire) {
                    if stop.load(Ordering::Acquire) {
                        request_managed_cancellation(&cohort_stop, &cancel_at_unix_ns);
                        break;
                    }
                    thread::sleep(std::time::Duration::from_millis(5));
                }
            });
            let calls = self
                .agents
                .iter_mut()
                .map(|agent| {
                    let descriptor = agent.descriptor().clone();
                    let cohort_stop = &cohort_stop;
                    let cancel_at_unix_ns = &cancel_at_unix_ns;
                    scope.spawn(move || {
                        let result = agent
                            .start_managed_phase_interruptible_with_cutoff(
                                phase_id,
                                phase_start_unix_ns,
                                cohort_stop,
                                cancel_at_unix_ns,
                            )
                            .map(|outcome| AgentManagedPhaseResult {
                                agent: descriptor.clone(),
                                outcome,
                            })
                            .map_err(|source| CohortError::AgentFailed {
                                id: descriptor.id,
                                source,
                            });
                        if result.is_err() {
                            request_managed_cancellation(cohort_stop, cancel_at_unix_ns);
                        }
                        result
                    })
                })
                .collect::<Vec<_>>();

            let mut results = Vec::with_capacity(calls.len());
            let mut first_error = None;
            for call in calls {
                match call.join() {
                    Ok(Ok(result)) => results.push(result),
                    Ok(Err(error)) => {
                        if first_error.is_none() {
                            first_error = Some(error);
                        }
                    }
                    Err(_) => {
                        request_managed_cancellation(&cohort_stop, &cancel_at_unix_ns);
                        if first_error.is_none() {
                            first_error = Some(CohortError::AgentPanicked);
                        }
                    }
                }
            }
            calls_finished.store(true, Ordering::Release);
            watcher.join().map_err(|_| CohortError::AgentPanicked)?;
            if let Some(error) = first_error {
                Err(error)
            } else {
                Ok(CohortManagedPhaseResult { agents: results })
            }
        })
    }

    pub fn cancel(&mut self, phase_id: PhaseId) -> Result<(), CohortError> {
        self.for_each_concurrently(move |agent| agent.cancel(phase_id))
    }

    pub fn disconnect(&mut self) -> Result<(), CohortError> {
        self.for_each_concurrently(|agent| agent.disconnect())
    }

    pub fn shutdown(&mut self) -> Result<(), CohortError> {
        self.for_each_concurrently(|agent| agent.shutdown())
    }

    pub fn diagnostics(&self) -> BTreeMap<String, Vec<String>> {
        self.agents
            .iter()
            .map(|agent| (agent.descriptor().id.0.clone(), agent.diagnostics()))
            .collect()
    }

    fn for_each_concurrently(
        &mut self,
        action: impl Fn(&mut dyn WorkloadAgent) -> Result<(), AgentError> + Copy + Send + Sync,
    ) -> Result<(), CohortError> {
        thread::scope(|scope| {
            let calls = self
                .agents
                .iter_mut()
                .map(|agent| {
                    let id = agent.descriptor().id.clone();
                    scope.spawn(move || {
                        action(agent.as_mut())
                            .map_err(|source| CohortError::AgentFailed { id, source })
                    })
                })
                .collect::<Vec<_>>();
            let mut first_error = None;
            for call in calls {
                let result = call.join().map_err(|_| CohortError::AgentPanicked)?;
                if let Err(error) = result
                    && first_error.is_none()
                {
                    first_error = Some(error);
                }
            }
            first_error.map_or(Ok(()), Err)
        })
    }
}

fn request_managed_cancellation(stop: &AtomicBool, cancel_at_unix_ns: &AtomicU64) {
    let cutoff = managed_cancellation_cutoff();
    let _ = cancel_at_unix_ns.compare_exchange(0, cutoff, Ordering::AcqRel, Ordering::Acquire);
    stop.store(true, Ordering::Release);
}

fn managed_cancellation_cutoff() -> u64 {
    unix_now_ns().saturating_add(MANAGED_CANCELLATION_LEAD_NS)
}

fn unix_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn operation_schema(operations: &[OperationDescriptor]) -> BTreeMap<String, OperationDescriptor> {
    operations
        .iter()
        .cloned()
        .map(|operation| (operation.name.clone(), operation))
        .collect()
}

#[derive(Debug)]
pub enum AgentError {
    Session(SessionError),
    Unavailable(String),
}

impl From<SessionError> for AgentError {
    fn from(value: SessionError) -> Self {
        Self::Session(value)
    }
}

impl From<crate::adapter_session::TransportError> for AgentError {
    fn from(value: crate::adapter_session::TransportError) -> Self {
        Self::Session(SessionError::Transport(value))
    }
}

impl fmt::Display for AgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Session(error) => error.fmt(formatter),
            Self::Unavailable(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for AgentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Session(error) => Some(error),
            Self::Unavailable(_) => None,
        }
    }
}

#[derive(Debug)]
pub enum CohortError {
    EmptyCohort,
    EmptyAgentId,
    DuplicateAgent(AgentId),
    AlreadyInitialized,
    NotInitialized,
    AgentFailed { id: AgentId, source: AgentError },
    AgentPanicked,
    AdapterIdentityMismatch { expected: AgentId, actual: AgentId },
    CapabilitiesMismatch { expected: AgentId, actual: AgentId },
    OperationSchemaMismatch { expected: AgentId, actual: AgentId },
    ManagedPhaseRequiresOpenLoop,
    ManagedPhaseCohortTooLarge,
}

impl fmt::Display for CohortError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCohort => formatter.write_str("an agent cohort cannot be empty"),
            Self::EmptyAgentId => formatter.write_str("agent identity cannot be empty"),
            Self::DuplicateAgent(id) => write!(formatter, "agent {id:?} appears more than once"),
            Self::AlreadyInitialized => formatter.write_str("agent cohort is already initialized"),
            Self::NotInitialized => formatter.write_str("agent cohort is not initialized"),
            Self::AgentFailed { id, source } => write!(formatter, "agent {id} failed: {source}"),
            Self::AgentPanicked => formatter.write_str("agent execution thread panicked"),
            Self::AdapterIdentityMismatch { expected, actual } => write!(
                formatter,
                "agent {actual} adapter identity differs from cohort reference agent {expected}"
            ),
            Self::CapabilitiesMismatch { expected, actual } => write!(
                formatter,
                "agent {actual} capabilities differ from cohort reference agent {expected}"
            ),
            Self::OperationSchemaMismatch { expected, actual } => write!(
                formatter,
                "agent {actual} operation schema differs from cohort reference agent {expected}"
            ),
            Self::ManagedPhaseRequiresOpenLoop => {
                formatter.write_str("adapter-managed phases require an open-loop load")
            }
            Self::ManagedPhaseCohortTooLarge => formatter
                .write_str("agent cohort is too large to represent managed-phase shard indices"),
        }
    }
}

impl std::error::Error for CohortError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::AgentFailed { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader, BufWriter, Write},
        net::TcpListener,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::adapter_session::ScheduleCompletion;
    use crate::protocol::{
        AdapterMessage, ControllerMessage, HistogramEncoding, HistogramSpec, LoadModel,
        ManagedOperation, OperationKind, PROTOCOL_VERSION,
    };

    struct FakeManagedAgent {
        descriptor: AgentDescriptor,
        ready: AdapterReady,
        events: Arc<Mutex<Vec<String>>>,
        requests: Arc<Mutex<Vec<(String, ManagedPhaseRequest)>>>,
        fail_prepare: bool,
    }

    impl WorkloadAgent for FakeManagedAgent {
        fn descriptor(&self) -> &AgentDescriptor {
            &self.descriptor
        }

        fn initialize(&mut self, _run_id: RunId, _config: Value) -> Result<AgentReady, AgentError> {
            Ok(AgentReady {
                agent: self.descriptor.clone(),
                adapter: self.ready.clone(),
            })
        }

        fn prepare_managed_phase(
            &mut self,
            _phase_id: PhaseId,
            request: ManagedPhaseRequest,
        ) -> Result<(), AgentError> {
            self.events
                .lock()
                .unwrap()
                .push(format!("prepare:{}", self.descriptor.id));
            self.requests
                .lock()
                .unwrap()
                .push((self.descriptor.id.0.clone(), request));
            if self.fail_prepare {
                Err(AgentError::Unavailable("prepare failed".into()))
            } else {
                Ok(())
            }
        }

        fn start_managed_phase_interruptible(
            &mut self,
            _phase_id: PhaseId,
            phase_start_unix_ns: u64,
            _stop: &AtomicBool,
        ) -> Result<ManagedPhaseOutcome, AgentError> {
            self.events.lock().unwrap().push(format!(
                "start:{}:{phase_start_unix_ns}",
                self.descriptor.id
            ));
            Ok(ManagedPhaseOutcome {
                result: None,
                completion: ScheduleCompletion::Completed,
            })
        }

        fn start_managed_phase_interruptible_with_cutoff(
            &mut self,
            phase_id: PhaseId,
            phase_start_unix_ns: u64,
            stop: &AtomicBool,
            cancel_at_unix_ns: &AtomicU64,
        ) -> Result<ManagedPhaseOutcome, AgentError> {
            if stop.load(Ordering::Acquire) {
                self.events.lock().unwrap().push(format!(
                    "cutoff:{}:{}",
                    self.descriptor.id,
                    cancel_at_unix_ns.load(Ordering::Acquire)
                ));
            }
            self.start_managed_phase_interruptible(phase_id, phase_start_unix_ns, stop)
        }

        fn cancel(&mut self, _phase_id: PhaseId) -> Result<(), AgentError> {
            self.events
                .lock()
                .unwrap()
                .push(format!("cancel:{}", self.descriptor.id));
            Ok(())
        }

        fn disconnect(&mut self) -> Result<(), AgentError> {
            Ok(())
        }

        fn shutdown(&mut self) -> Result<(), AgentError> {
            Ok(())
        }

        fn diagnostics(&self) -> Vec<String> {
            Vec::new()
        }
    }

    fn ready(operation: &str) -> AdapterReady {
        AdapterReady {
            identity: AdapterIdentity {
                name: "fake-adapter".into(),
                version: Some("1.0.0".into()),
            },
            capabilities: Capabilities {
                adapter_managed_phases: true,
                load_models: vec![LoadModel::OpenLoop],
                histogram_encodings: vec![HistogramEncoding::HdrV2Base64],
            },
            operations: vec![OperationDescriptor {
                name: operation.into(),
                description: None,
                kind: OperationKind::Read,
                enabled_by_default: true,
                default_weight: 1.0,
                arguments: Vec::new(),
            }],
        }
    }

    fn fake_agent(id: &str, operation: &str) -> Box<dyn WorkloadAgent> {
        Box::new(FakeManagedAgent {
            descriptor: AgentDescriptor {
                id: AgentId(id.into()),
                instance_id: AgentInstanceId(format!("{id}-instance")),
                placement: AgentPlacement::Colocated,
            },
            ready: ready(operation),
            events: Arc::new(Mutex::new(Vec::new())),
            requests: Arc::new(Mutex::new(Vec::new())),
            fail_prepare: false,
        })
    }

    fn fake_managed_agent(
        id: &str,
        events: Arc<Mutex<Vec<String>>>,
        requests: Arc<Mutex<Vec<(String, ManagedPhaseRequest)>>>,
        fail_prepare: bool,
    ) -> Box<dyn WorkloadAgent> {
        let ready = ready("read");
        Box::new(FakeManagedAgent {
            descriptor: AgentDescriptor {
                id: AgentId(id.into()),
                instance_id: AgentInstanceId(format!("{id}-instance")),
                placement: AgentPlacement::Colocated,
            },
            ready,
            events,
            requests,
            fail_prepare,
        })
    }

    fn managed_request() -> ManagedPhaseRequest {
        ManagedPhaseRequest {
            warmup_ns: 10,
            measurement_ns: 100,
            operation_timeout_ns: 20,
            load: Load::OpenLoop {
                requests_per_second: 2_000.0,
            },
            operations: vec![ManagedOperation {
                operation: "read".into(),
                arguments: Default::default(),
                weight: 1.0,
                shard_index: 99,
                shard_count: 99,
            }],
            bucket_count: 2,
            histogram: HistogramSpec {
                lowest_discernible_ns: 1,
                highest_trackable_ns: 1_000_000,
                significant_figures: 3,
            },
        }
    }

    #[test]
    fn managed_phase_prepares_every_agent_before_start_and_assigns_fixed_shards() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut cohort = AgentCohort::new(vec![
            fake_managed_agent("local-0", Arc::clone(&events), Arc::clone(&requests), false),
            fake_managed_agent("local-1", Arc::clone(&events), Arc::clone(&requests), false),
        ])
        .unwrap();
        cohort.initialize(RunId(1), Value::Null).unwrap();
        let before_start = unix_now_ns();

        let result = cohort
            .execute_managed_phase(
                PhaseId(3),
                Duration::from_nanos(1),
                managed_request(),
                &AtomicBool::new(false),
            )
            .unwrap();

        assert_eq!(result.agents.len(), 2);
        let events = events.lock().unwrap();
        assert_eq!(&events[..2], ["prepare:local-0", "prepare:local-1"]);
        assert!(events[2..].iter().all(|event| event.starts_with("start:")));
        let starts = events[2..]
            .iter()
            .map(|event| event.rsplit_once(':').unwrap().1.parse::<u64>().unwrap())
            .collect::<Vec<_>>();
        assert!(starts.iter().all(|start| *start == starts[0]));
        assert!(starts[0] >= before_start);
        let requests = requests.lock().unwrap();
        assert_eq!(requests[0].1.operations[0].shard_index, 0);
        assert_eq!(requests[1].1.operations[0].shard_index, 1);
        assert!(
            requests
                .iter()
                .all(|(_, request)| request.operations[0].shard_count == 2)
        );
        assert!(requests.iter().all(|(_, request)| matches!(
            request.load,
            Load::OpenLoop {
                requests_per_second: 2_000.0
            }
        )));
    }

    #[test]
    fn managed_prepare_failure_cancels_prepared_peers_without_starting() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut cohort = AgentCohort::new(vec![
            fake_managed_agent("local-0", Arc::clone(&events), Arc::clone(&requests), false),
            fake_managed_agent("local-1", Arc::clone(&events), Arc::clone(&requests), true),
        ])
        .unwrap();
        cohort.initialize(RunId(1), Value::Null).unwrap();

        assert!(matches!(
            cohort.execute_managed_phase(
                PhaseId(3),
                Duration::from_nanos(1),
                managed_request(),
                &AtomicBool::new(false),
            ),
            Err(CohortError::AgentFailed {
                id: AgentId(id),
                ..
            }) if id == "local-1"
        ));
        assert_eq!(
            events.lock().unwrap().as_slice(),
            ["prepare:local-0", "prepare:local-1", "cancel:local-0"]
        );
    }

    #[test]
    fn managed_cancellation_uses_one_future_cutoff_for_every_agent() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let mut cohort = AgentCohort::new(vec![
            fake_managed_agent("local-0", Arc::clone(&events), Arc::clone(&requests), false),
            fake_managed_agent("local-1", Arc::clone(&events), requests, false),
        ])
        .unwrap();
        cohort.initialize(RunId(1), Value::Null).unwrap();
        let before = unix_now_ns();

        cohort
            .execute_managed_phase(
                PhaseId(3),
                Duration::from_nanos(1),
                managed_request(),
                &AtomicBool::new(true),
            )
            .unwrap();

        let cutoffs = events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| event.strip_prefix("cutoff:"))
            .map(|event| event.rsplit_once(':').unwrap().1.parse::<u64>().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(cutoffs.len(), 2);
        assert_eq!(cutoffs[0], cutoffs[1]);
        assert!(cutoffs[0] > before);
    }

    #[test]
    fn mismatched_operation_schemas_are_rejected_before_a_phase() {
        let mut cohort = AgentCohort::new(vec![
            fake_agent("local-0", "read"),
            fake_agent("local-1", "write"),
        ])
        .unwrap();

        assert!(matches!(
            cohort.initialize(RunId(1), Value::Null),
            Err(CohortError::OperationSchemaMismatch { .. })
        ));
    }

    #[test]
    fn mismatched_adapter_identities_are_rejected_before_a_phase() {
        let mut different = ready("read");
        different.identity.name = "different-adapter".into();
        let second = FakeManagedAgent {
            descriptor: AgentDescriptor {
                id: AgentId("local-1".into()),
                instance_id: AgentInstanceId("local-1-instance".into()),
                placement: AgentPlacement::Colocated,
            },
            ready: different,
            events: Arc::new(Mutex::new(Vec::new())),
            requests: Arc::new(Mutex::new(Vec::new())),
            fail_prepare: false,
        };
        let mut cohort =
            AgentCohort::new(vec![fake_agent("local-0", "read"), Box::new(second)]).unwrap();

        assert!(matches!(
            cohort.initialize(RunId(1), Value::Null),
            Err(CohortError::AdapterIdentityMismatch { .. })
        ));
    }

    #[test]
    fn mismatched_tcp_agent_schemas_are_rejected_on_loopback() {
        let (read_endpoint, read_server) = tcp_ready_server("read");
        let (write_endpoint, write_server) = tcp_ready_server("write");
        let options = SessionOptions::default();
        let first = TcpAgent::connect(
            AgentId::new("tcp-0").unwrap(),
            &read_endpoint,
            options.clone(),
        )
        .unwrap();
        let second =
            TcpAgent::connect(AgentId::new("tcp-1").unwrap(), &write_endpoint, options).unwrap();
        let mut cohort = AgentCohort::new(vec![Box::new(first), Box::new(second)]).unwrap();

        assert!(matches!(
            cohort.initialize(RunId(1), Value::Null),
            Err(CohortError::OperationSchemaMismatch { .. })
        ));
        read_server.join().unwrap();
        write_server.join().unwrap();
    }

    fn tcp_ready_server(operation: &'static str) -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let server = thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let mut input = BufReader::new(stream.try_clone().unwrap());
            let mut initialize = String::new();
            assert_ne!(input.read_line(&mut initialize).unwrap(), 0);
            assert!(matches!(
                serde_json::from_str::<ControllerMessage>(&initialize).unwrap(),
                ControllerMessage::Initialize { .. }
            ));
            let ready = ready(operation);
            let message = AdapterMessage::Ready {
                protocol_version: PROTOCOL_VERSION,
                identity: ready.identity,
                capabilities: ready.capabilities,
                operations: ready.operations,
            };
            let mut output = BufWriter::new(stream);
            serde_json::to_writer(&mut output, &message).unwrap();
            output.write_all(b"\n").unwrap();
            output.flush().unwrap();
        });
        (endpoint, server)
    }

    #[test]
    fn duplicate_agent_identity_is_rejected() {
        assert!(matches!(
            AgentCohort::new(vec![
                fake_agent("worker", "read"),
                fake_agent("worker", "read"),
            ]),
            Err(CohortError::DuplicateAgent(AgentId(id))) if id == "worker"
        ));
    }
}
