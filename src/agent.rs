//! Workload-agent abstraction and coordinator-owned colocated implementation.

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    adapter_session::{
        AdapterReady, AdapterSession, AdapterTransport, SessionError, SessionOptions,
        SubprocessTransport, TcpTransport,
    },
    config::{AdapterCommand, AgentEndpointConfig, AgentTransportConfig},
    protocol::{
        AdapterIdentity, Capabilities, OperationDescriptor, OperationResult, PhaseId, RunId,
        ScheduledOperation,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AgentInstanceId(pub String);

static NEXT_AGENT_INSTANCE: AtomicU64 = AtomicU64::new(1);

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

    fn execute_schedule(
        &mut self,
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
        operations: Vec<ScheduledOperation>,
    ) -> Result<Vec<OperationResult>, AgentError>;

    fn cancel(&mut self, phase_id: PhaseId) -> Result<(), AgentError>;

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

    fn execute_schedule(
        &mut self,
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
        operations: Vec<ScheduledOperation>,
    ) -> Result<Vec<OperationResult>, AgentError> {
        self.session
            .schedule(phase_id, phase_start_unix_ns, operations)
            .map_err(Into::into)
    }

    fn cancel(&mut self, phase_id: PhaseId) -> Result<(), AgentError> {
        self.session.cancel(phase_id).map_err(Into::into)
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

#[derive(Debug, Clone, PartialEq)]
pub struct AgentPhaseResult {
    pub agent: AgentDescriptor,
    pub operations: Vec<OperationResult>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CohortPhaseResult {
    pub agents: Vec<AgentPhaseResult>,
}

impl CohortPhaseResult {
    pub fn into_operations(self) -> Vec<OperationResult> {
        self.agents
            .into_iter()
            .flat_map(|agent| agent.operations)
            .collect()
    }
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

    pub fn execute_schedule(
        &mut self,
        phase_id: PhaseId,
        phase_start_unix_ns: u64,
        operations: Vec<ScheduledOperation>,
    ) -> Result<CohortPhaseResult, CohortError> {
        if !self.initialized {
            return Err(CohortError::NotInitialized);
        }

        let mut operation_ids = HashSet::with_capacity(operations.len());
        for operation in &operations {
            if !operation_ids.insert(operation.id) {
                return Err(CohortError::DuplicateOperation(operation.id.0));
            }
        }
        let mut assignments = vec![Vec::new(); self.agents.len()];
        let mut next_agent_by_variant = BTreeMap::new();
        for operation in operations {
            let variant = (operation.operation.clone(), operation.arguments.clone());
            let next_agent = next_agent_by_variant.entry(variant).or_insert(0_usize);
            assignments[*next_agent].push(operation);
            *next_agent = (*next_agent + 1) % self.agents.len();
        }

        let results = thread::scope(|scope| {
            let mut calls = Vec::with_capacity(self.agents.len());
            for (agent, assignment) in self.agents.iter_mut().zip(assignments) {
                let descriptor = agent.descriptor().clone();
                let call = scope.spawn(move || {
                    agent
                        .execute_schedule(phase_id, phase_start_unix_ns, assignment)
                        .map(|operations| AgentPhaseResult {
                            agent: descriptor.clone(),
                            operations,
                        })
                        .map_err(|source| CohortError::AgentFailed {
                            id: descriptor.id,
                            source,
                        })
                });
                calls.push(call);
            }
            calls
                .into_iter()
                .map(|call| call.join().map_err(|_| CohortError::AgentPanicked)?)
                .collect::<Result<Vec<_>, CohortError>>()
        })?;

        Ok(CohortPhaseResult { agents: results })
    }

    pub fn cancel(&mut self, phase_id: PhaseId) -> Result<(), CohortError> {
        self.for_each_concurrently(move |agent| agent.cancel(phase_id))
    }

    pub fn shutdown(&mut self) -> Result<(), CohortError> {
        self.for_each_concurrently(|agent| agent.shutdown())
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
    DuplicateOperation(u64),
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
            Self::DuplicateOperation(id) => {
                write!(
                    formatter,
                    "operation {id} appears more than once in the global schedule"
                )
            }
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
    use crate::protocol::{
        AdapterMessage, ControllerMessage, LoadModel, OperationId, OperationKind, OperationStatus,
        PROTOCOL_VERSION,
    };

    struct FakeAgent {
        descriptor: AgentDescriptor,
        ready: AdapterReady,
        assignments: Arc<Mutex<Vec<Vec<u64>>>>,
        fail_phase: bool,
    }

    impl WorkloadAgent for FakeAgent {
        fn descriptor(&self) -> &AgentDescriptor {
            &self.descriptor
        }

        fn initialize(&mut self, _run_id: RunId, _config: Value) -> Result<AgentReady, AgentError> {
            Ok(AgentReady {
                agent: self.descriptor.clone(),
                adapter: self.ready.clone(),
            })
        }

        fn execute_schedule(
            &mut self,
            _phase_id: PhaseId,
            _phase_start_unix_ns: u64,
            operations: Vec<ScheduledOperation>,
        ) -> Result<Vec<OperationResult>, AgentError> {
            self.assignments
                .lock()
                .unwrap()
                .push(operations.iter().map(|operation| operation.id.0).collect());
            if self.fail_phase {
                return Err(AgentError::Unavailable("lost worker".into()));
            }
            Ok(operations
                .into_iter()
                .map(|operation| OperationResult {
                    id: operation.id,
                    operation: operation.operation,
                    arguments: operation.arguments,
                    intended_start_offset_ns: operation.start_offset_ns,
                    actual_start_offset_ns: operation.start_offset_ns,
                    client_latency_ns: 1,
                    status: OperationStatus::Ok,
                })
                .collect())
        }

        fn cancel(&mut self, _phase_id: PhaseId) -> Result<(), AgentError> {
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
                scheduled_operations: true,
                adapter_managed_phases: false,
                load_models: vec![LoadModel::OpenLoop],
                max_batch_size: None,
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

    fn fake_agent(
        id: &str,
        operation: &str,
        assignments: Arc<Mutex<Vec<Vec<u64>>>>,
    ) -> Box<dyn WorkloadAgent> {
        Box::new(FakeAgent {
            descriptor: AgentDescriptor {
                id: AgentId(id.into()),
                instance_id: AgentInstanceId(format!("{id}-instance")),
                placement: AgentPlacement::Colocated,
            },
            ready: ready(operation),
            assignments,
            fail_phase: false,
        })
    }

    fn scheduled(id: u64) -> ScheduledOperation {
        ScheduledOperation {
            id: OperationId(id),
            operation: "read".into(),
            start_offset_ns: id * 100,
            arguments: Default::default(),
        }
    }

    #[test]
    fn cohort_distributes_a_global_schedule_deterministically() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let mut cohort = AgentCohort::new(vec![
            fake_agent("local-0", "read", Arc::clone(&first)),
            fake_agent("local-1", "read", Arc::clone(&second)),
        ])
        .unwrap();
        cohort.initialize(RunId(1), Value::Null).unwrap();

        let result = cohort
            .execute_schedule(PhaseId(1), 100, (0..6).map(scheduled).collect::<Vec<_>>())
            .unwrap();

        assert_eq!(first.lock().unwrap().as_slice(), &[vec![0, 2, 4]]);
        assert_eq!(second.lock().unwrap().as_slice(), &[vec![1, 3, 5]]);
        assert_eq!(result.agents.len(), 2);
        assert_eq!(result.into_operations().len(), 6);
    }

    #[test]
    fn cohort_balances_each_bound_variant_across_agents() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(Mutex::new(Vec::new()));
        let mut cohort = AgentCohort::new(vec![
            fake_agent("local-0", "read", Arc::clone(&first)),
            fake_agent("local-1", "read", Arc::clone(&second)),
        ])
        .unwrap();
        cohort.initialize(RunId(1), Value::Null).unwrap();
        let operations = ["read", "write", "read", "write"]
            .into_iter()
            .enumerate()
            .map(|(id, operation)| ScheduledOperation {
                id: OperationId(id as u64),
                operation: operation.into(),
                start_offset_ns: id as u64 * 100,
                arguments: Default::default(),
            })
            .collect();

        cohort
            .execute_schedule(PhaseId(1), 100, operations)
            .unwrap();

        assert_eq!(first.lock().unwrap().as_slice(), &[vec![0, 1]]);
        assert_eq!(second.lock().unwrap().as_slice(), &[vec![2, 3]]);
    }

    #[test]
    fn mismatched_operation_schemas_are_rejected_before_a_phase() {
        let assignments = Arc::new(Mutex::new(Vec::new()));
        let mut cohort = AgentCohort::new(vec![
            fake_agent("local-0", "read", Arc::clone(&assignments)),
            fake_agent("local-1", "write", assignments),
        ])
        .unwrap();

        assert!(matches!(
            cohort.initialize(RunId(1), Value::Null),
            Err(CohortError::OperationSchemaMismatch { .. })
        ));
    }

    #[test]
    fn mismatched_adapter_identities_are_rejected_before_a_phase() {
        let assignments = Arc::new(Mutex::new(Vec::new()));
        let mut different = ready("read");
        different.identity.name = "different-adapter".into();
        let second = FakeAgent {
            descriptor: AgentDescriptor {
                id: AgentId("local-1".into()),
                instance_id: AgentInstanceId("local-1-instance".into()),
                placement: AgentPlacement::Colocated,
            },
            ready: different,
            assignments: Arc::clone(&assignments),
            fail_phase: false,
        };
        let mut cohort = AgentCohort::new(vec![
            fake_agent("local-0", "read", Arc::clone(&assignments)),
            Box::new(second),
        ])
        .unwrap();

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
        let assignments = Arc::new(Mutex::new(Vec::new()));
        assert!(matches!(
            AgentCohort::new(vec![
                fake_agent("worker", "read", Arc::clone(&assignments)),
                fake_agent("worker", "read", assignments),
            ]),
            Err(CohortError::DuplicateAgent(AgentId(id))) if id == "worker"
        ));
    }

    #[test]
    fn duplicate_global_operation_ids_are_rejected_before_fanout() {
        let assignments = Arc::new(Mutex::new(Vec::new()));
        let mut cohort = AgentCohort::new(vec![fake_agent(
            "local-0",
            "read",
            Arc::clone(&assignments),
        )])
        .unwrap();
        cohort.initialize(RunId(1), Value::Null).unwrap();

        assert!(matches!(
            cohort.execute_schedule(PhaseId(1), 100, vec![scheduled(7), scheduled(7)]),
            Err(CohortError::DuplicateOperation(7))
        ));
        assert!(assignments.lock().unwrap().is_empty());
    }

    #[test]
    fn failed_agent_invalidates_the_phase_without_redistribution() {
        let healthy_assignments = Arc::new(Mutex::new(Vec::new()));
        let failed_assignments = Arc::new(Mutex::new(Vec::new()));
        let failed = FakeAgent {
            descriptor: AgentDescriptor {
                id: AgentId("local-1".into()),
                instance_id: AgentInstanceId("local-1-instance".into()),
                placement: AgentPlacement::Colocated,
            },
            ready: ready("read"),
            assignments: Arc::clone(&failed_assignments),
            fail_phase: true,
        };
        let mut cohort = AgentCohort::new(vec![
            fake_agent("local-0", "read", Arc::clone(&healthy_assignments)),
            Box::new(failed),
        ])
        .unwrap();
        cohort.initialize(RunId(1), Value::Null).unwrap();

        assert!(matches!(
            cohort.execute_schedule(
                PhaseId(1),
                100,
                (0..4).map(scheduled).collect::<Vec<_>>()
            ),
            Err(CohortError::AgentFailed {
                id: AgentId(id),
                ..
            }) if id == "local-1"
        ));
        assert_eq!(
            healthy_assignments.lock().unwrap().as_slice(),
            &[vec![0, 2]]
        );
        assert_eq!(failed_assignments.lock().unwrap().as_slice(), &[vec![1, 3]]);
    }
}
