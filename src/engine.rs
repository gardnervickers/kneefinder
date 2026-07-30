//! Frontend-neutral command, snapshot, and event boundary for the run engine.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvError, RecvTimeoutError, TryRecvError},
    },
    thread,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    agent::{AgentCohort, CohortReady},
    config::RunConfig,
    executor::{ExecutionSink, ExecutorCompletion, RunExecutor},
    measurement::{RunEvent, RunState, TransitionError},
    protocol::{PhaseId, RunId},
    stats::PhaseReport,
    strategy::StrategyDecision,
    workload::{WorkloadError, normalize_operation_mix, resolve_operation_mix},
};

/// A state-changing request accepted from any frontend.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum EngineCommand {
    Configure {
        config: Box<RunConfig>,
    },
    UpdateConfigured {
        run_id: RunId,
        config: Box<RunConfig>,
    },
    PrepareAgents {
        run_id: RunId,
    },
    Start {
        run_id: RunId,
    },
    Stop {
        run_id: RunId,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunSnapshot {
    /// Monotonically increasing run revision used to de-duplicate UI updates.
    pub revision: u64,
    pub run_id: RunId,
    pub config: RunConfig,
    pub state: RunState,
    #[serde(default)]
    pub preparation: AgentPreparation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AgentPreparation {
    #[default]
    Unprepared,
    Preparing,
    Ready {
        catalog: CohortReady,
    },
    Failed {
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum EngineEvent {
    RunConfigured {
        snapshot: RunSnapshot,
    },
    RunConfigurationUpdated {
        snapshot: RunSnapshot,
    },
    RunPreparationChanged {
        snapshot: RunSnapshot,
    },
    RunStateChanged {
        previous: RunState,
        snapshot: RunSnapshot,
    },
    PhaseStats {
        run_id: RunId,
        phase_id: PhaseId,
        report: PhaseReport,
    },
    StrategyDecision {
        run_id: RunId,
        decision: StrategyDecision,
    },
}

/// The core runtime owns this value and uses it to report adapter and
/// measurement events. Frontends receive only an [`EngineHandle`].
pub struct Engine {
    inner: Arc<EngineInner>,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(EngineInner::new(true)),
        }
    }

    #[cfg(test)]
    fn new_manual() -> Self {
        Self {
            inner: Arc::new(EngineInner::new(false)),
        }
    }

    pub fn handle(&self) -> EngineHandle {
        EngineHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    /// Records a lifecycle event produced by the adapter or measurement
    /// runtime, then broadcasts the resulting snapshot to every frontend.
    pub fn record_run_event(
        &self,
        run_id: RunId,
        event: RunEvent,
    ) -> Result<RunSnapshot, EngineError> {
        self.inner.transition(run_id, event)
    }

    /// Publishes a completed phase report to every frontend and artifact sink.
    pub fn record_phase_stats(
        &self,
        run_id: RunId,
        phase_id: PhaseId,
        report: PhaseReport,
    ) -> Result<(), EngineError> {
        self.inner.phase_stats(run_id, phase_id, report)
    }

    /// Transfers the initialized cohort to the runtime that executes the run.
    /// Its discovery catalog remains attached to the run snapshot.
    pub fn take_prepared_cohort(&self, run_id: RunId) -> Result<AgentCohort, EngineError> {
        self.inner
            .prepared
            .lock()
            .expect("prepared cohort mutex poisoned")
            .remove(&run_id)
            .ok_or(EngineError::PreparedCohortUnavailable(run_id))
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

/// Cloneable API shared by CLI, TUI, web, tests, and other frontends.
#[derive(Clone)]
pub struct EngineHandle {
    inner: Arc<EngineInner>,
}

impl EngineHandle {
    pub fn execute(&self, command: EngineCommand) -> Result<RunSnapshot, EngineError> {
        match command {
            EngineCommand::Configure { config } => self.inner.configure(*config),
            EngineCommand::UpdateConfigured { run_id, config } => {
                self.inner.update_configured(run_id, *config)
            }
            EngineCommand::PrepareAgents { run_id } => self.inner.prepare_agents(run_id),
            EngineCommand::Start { run_id } => self.inner.start(run_id),
            EngineCommand::Stop { run_id } => self.inner.stop(run_id),
        }
    }

    /// Subscribes before taking a snapshot to avoid missing updates. Consumers
    /// may de-duplicate a snapshot and a concurrent event by revision.
    pub fn subscribe(&self) -> EventSubscription {
        let (sender, receiver) = mpsc::channel();
        self.inner
            .subscribers
            .lock()
            .expect("engine subscriber mutex poisoned")
            .push(sender);
        EventSubscription { receiver }
    }

    pub fn snapshot(&self, run_id: RunId) -> Result<RunSnapshot, EngineError> {
        self.inner
            .registry
            .lock()
            .expect("engine registry mutex poisoned")
            .runs
            .get(&run_id)
            .cloned()
            .ok_or(EngineError::RunNotFound(run_id))
    }

    pub fn snapshots(&self) -> Vec<RunSnapshot> {
        self.inner
            .registry
            .lock()
            .expect("engine registry mutex poisoned")
            .runs
            .values()
            .cloned()
            .collect()
    }
}

/// Blocking event receiver deliberately hides the transport implementation.
/// An async web frontend can bridge it onto its runtime without changing core.
pub struct EventSubscription {
    receiver: Receiver<EngineEvent>,
}

impl EventSubscription {
    pub fn recv(&self) -> Result<EngineEvent, RecvError> {
        self.receiver.recv()
    }

    pub fn recv_timeout(&self, timeout: Duration) -> Result<EngineEvent, RecvTimeoutError> {
        self.receiver.recv_timeout(timeout)
    }

    pub fn try_recv(&self) -> Result<EngineEvent, TryRecvError> {
        self.receiver.try_recv()
    }
}

struct EngineInner {
    execution_enabled: bool,
    mutation: Mutex<()>,
    registry: Mutex<Registry>,
    prepared: Mutex<BTreeMap<RunId, AgentCohort>>,
    executions: Mutex<BTreeMap<RunId, ExecutionControl>>,
    subscribers: Mutex<Vec<mpsc::Sender<EngineEvent>>>,
}

impl EngineInner {
    fn new(execution_enabled: bool) -> Self {
        Self {
            execution_enabled,
            mutation: Mutex::new(()),
            registry: Mutex::new(Registry::default()),
            prepared: Mutex::new(BTreeMap::new()),
            executions: Mutex::new(BTreeMap::new()),
            subscribers: Mutex::new(Vec::new()),
        }
    }

    fn configure(&self, config: RunConfig) -> Result<RunSnapshot, EngineError> {
        let _mutation = self
            .mutation
            .lock()
            .expect("engine mutation mutex poisoned");
        let snapshot = {
            let mut registry = self
                .registry
                .lock()
                .expect("engine registry mutex poisoned");
            let run_id = RunId(registry.next_run_id);
            registry.next_run_id = registry
                .next_run_id
                .checked_add(1)
                .ok_or(EngineError::RunIdExhausted)?;
            let snapshot = RunSnapshot {
                revision: 1,
                run_id,
                config,
                state: RunState::Configured,
                preparation: AgentPreparation::Unprepared,
            };
            registry.runs.insert(run_id, snapshot.clone());
            snapshot
        };

        self.publish(EngineEvent::RunConfigured {
            snapshot: snapshot.clone(),
        });
        Ok(snapshot)
    }

    fn transition(&self, run_id: RunId, event: RunEvent) -> Result<RunSnapshot, EngineError> {
        let _mutation = self
            .mutation
            .lock()
            .expect("engine mutation mutex poisoned");
        let (previous, snapshot) = {
            let mut registry = self
                .registry
                .lock()
                .expect("engine registry mutex poisoned");
            if matches!(&event, RunEvent::StartRequested)
                && let Some(active) = active_run_id(&registry, run_id)
            {
                return Err(EngineError::RunAlreadyActive {
                    requested: run_id,
                    active,
                });
            }
            let run = registry
                .runs
                .get_mut(&run_id)
                .ok_or(EngineError::RunNotFound(run_id))?;
            let previous = run.state.clone();
            let next = previous.clone().transition(event).map_err(|source| {
                EngineError::InvalidTransition {
                    run_id,
                    source: Box::new(source),
                }
            })?;
            run.revision = run
                .revision
                .checked_add(1)
                .ok_or(EngineError::RevisionExhausted(run_id))?;
            run.state = next;
            (previous, run.clone())
        };

        self.publish(EngineEvent::RunStateChanged {
            previous,
            snapshot: snapshot.clone(),
        });
        Ok(snapshot)
    }

    fn update_configured(
        &self,
        run_id: RunId,
        mut config: RunConfig,
    ) -> Result<RunSnapshot, EngineError> {
        let _mutation = self
            .mutation
            .lock()
            .expect("engine mutation mutex poisoned");
        let (snapshot, endpoints_changed) = {
            let mut registry = self
                .registry
                .lock()
                .expect("engine registry mutex poisoned");
            let run = registry
                .runs
                .get_mut(&run_id)
                .ok_or(EngineError::RunNotFound(run_id))?;
            if run.state != RunState::Configured {
                return Err(EngineError::ConfigurationLocked(run_id));
            }
            let endpoints_changed = run.config.agents != config.agents;
            if !endpoints_changed && let AgentPreparation::Ready { catalog } = &run.preparation {
                let operations = normalize_operation_mix(&config.workload, &catalog.operations)
                    .map_err(|source| EngineError::InvalidWorkload { run_id, source })?;
                config.workload.operations =
                    crate::config::OperationSelection::Selected { operations };
            }
            run.revision = run
                .revision
                .checked_add(1)
                .ok_or(EngineError::RevisionExhausted(run_id))?;
            run.config = config;
            if endpoints_changed {
                run.preparation = AgentPreparation::Unprepared;
            }
            (run.clone(), endpoints_changed)
        };

        if endpoints_changed
            && let Some(mut cohort) = self
                .prepared
                .lock()
                .expect("prepared cohort mutex poisoned")
                .remove(&run_id)
        {
            let _ = cohort.disconnect();
        }
        self.publish(EngineEvent::RunConfigurationUpdated {
            snapshot: snapshot.clone(),
        });
        Ok(snapshot)
    }

    fn prepare_agents(&self, run_id: RunId) -> Result<RunSnapshot, EngineError> {
        let _mutation = self
            .mutation
            .lock()
            .expect("engine mutation mutex poisoned");
        let endpoints = {
            let mut registry = self
                .registry
                .lock()
                .expect("engine registry mutex poisoned");
            if let Some(active) = active_run_id(&registry, run_id) {
                return Err(EngineError::RunAlreadyActive {
                    requested: run_id,
                    active,
                });
            }
            let run = registry
                .runs
                .get_mut(&run_id)
                .ok_or(EngineError::RunNotFound(run_id))?;
            if run.state != RunState::Configured {
                return Err(EngineError::ConfigurationLocked(run_id));
            }
            if run.config.agents.is_empty() {
                return Err(EngineError::NoAgentsConfigured(run_id));
            }
            run.revision = run
                .revision
                .checked_add(1)
                .ok_or(EngineError::RevisionExhausted(run_id))?;
            run.preparation = AgentPreparation::Preparing;
            let snapshot = run.clone();
            self.publish(EngineEvent::RunPreparationChanged { snapshot });
            run.config.agents.clone()
        };

        if let Some(mut cohort) = self
            .prepared
            .lock()
            .expect("prepared cohort mutex poisoned")
            .remove(&run_id)
        {
            let _ = cohort.disconnect();
        }

        let prepared =
            AgentCohort::from_endpoints(&endpoints, Default::default()).and_then(|mut cohort| {
                cohort
                    .initialize(run_id, Value::Object(Default::default()))
                    .map(|catalog| (cohort, catalog))
            });

        match prepared {
            Ok((cohort, catalog)) => {
                self.prepared
                    .lock()
                    .expect("prepared cohort mutex poisoned")
                    .insert(run_id, cohort);
                self.finish_preparation(run_id, AgentPreparation::Ready { catalog })
            }
            Err(source) => {
                let error = EngineError::AgentPreparationFailed {
                    run_id,
                    message: source.to_string(),
                };
                self.fail_preparation(run_id, error.to_string())?;
                Err(error)
            }
        }
    }

    fn fail_preparation(&self, run_id: RunId, message: String) -> Result<(), EngineError> {
        self.finish_preparation(run_id, AgentPreparation::Failed { message })
            .map(|_| ())
    }

    fn finish_preparation(
        &self,
        run_id: RunId,
        preparation: AgentPreparation,
    ) -> Result<RunSnapshot, EngineError> {
        let snapshot = {
            let mut registry = self
                .registry
                .lock()
                .expect("engine registry mutex poisoned");
            let run = registry
                .runs
                .get_mut(&run_id)
                .ok_or(EngineError::RunNotFound(run_id))?;
            if let AgentPreparation::Ready { catalog } = &preparation
                && let Ok(operations) =
                    normalize_operation_mix(&run.config.workload, &catalog.operations)
            {
                run.config.workload.operations =
                    crate::config::OperationSelection::Selected { operations };
            }
            run.revision = run
                .revision
                .checked_add(1)
                .ok_or(EngineError::RevisionExhausted(run_id))?;
            run.preparation = preparation;
            run.clone()
        };
        self.publish(EngineEvent::RunPreparationChanged {
            snapshot: snapshot.clone(),
        });
        Ok(snapshot)
    }

    fn start(self: &Arc<Self>, run_id: RunId) -> Result<RunSnapshot, EngineError> {
        if !self.execution_enabled {
            return self.transition(run_id, RunEvent::StartRequested);
        }
        let (config, catalog) = {
            let registry = self
                .registry
                .lock()
                .expect("engine registry mutex poisoned");
            let run = registry
                .runs
                .get(&run_id)
                .ok_or(EngineError::RunNotFound(run_id))?;
            if run.config.agents.is_empty() {
                return Err(EngineError::NoAgentsConfigured(run_id));
            }
            let AgentPreparation::Ready { catalog } = &run.preparation else {
                return Err(EngineError::AgentsNotPrepared(run_id));
            };
            resolve_operation_mix(&run.config.workload, &catalog.operations)
                .map_err(|source| EngineError::InvalidWorkload { run_id, source })?;
            if !self
                .prepared
                .lock()
                .expect("prepared cohort mutex poisoned")
                .contains_key(&run_id)
            {
                return Err(EngineError::PreparedCohortUnavailable(run_id));
            }
            (run.config.clone(), catalog.clone())
        };

        let snapshot = self.transition(run_id, RunEvent::StartRequested)?;
        let Some(mut cohort) = self
            .prepared
            .lock()
            .expect("prepared cohort mutex poisoned")
            .remove(&run_id)
        else {
            let _ = self.transition(
                run_id,
                RunEvent::Failed {
                    message: EngineError::PreparedCohortUnavailable(run_id).to_string(),
                },
            );
            return Err(EngineError::PreparedCohortUnavailable(run_id));
        };
        let stop = Arc::new(AtomicBool::new(false));
        self.executions
            .lock()
            .expect("execution registry mutex poisoned")
            .insert(
                run_id,
                ExecutionControl {
                    stop: Arc::clone(&stop),
                },
            );
        let inner = Arc::clone(self);
        let spawn = thread::Builder::new()
            .name(format!("kneefinder-run-{}", run_id.0))
            .spawn(move || {
                let mut sink = EngineExecutionSink {
                    inner: Arc::clone(&inner),
                    run_id,
                };
                let result = RunExecutor::default().execute(
                    &config,
                    &catalog,
                    &mut cohort,
                    &stop,
                    &mut sink,
                );
                let disconnect = cohort.disconnect();
                if let Err(error) = disconnect {
                    let state = inner
                        .registry
                        .lock()
                        .expect("engine registry mutex poisoned")
                        .runs
                        .get(&run_id)
                        .map(|run| run.state.clone());
                    if let Some(state) = state
                        && !state.is_terminal()
                    {
                        let _ = inner.transition(
                            run_id,
                            RunEvent::Failed {
                                message: format!("failed to disconnect workload agents: {error}"),
                            },
                        );
                    }
                } else if let Err(error) = result {
                    let state = inner
                        .registry
                        .lock()
                        .expect("engine registry mutex poisoned")
                        .runs
                        .get(&run_id)
                        .map(|run| run.state.clone());
                    match state {
                        Some(RunState::Stopping { .. }) => {
                            let _ = inner.transition(run_id, RunEvent::AdapterStopped);
                        }
                        Some(state) if !state.is_terminal() => {
                            let _ = inner.transition(
                                run_id,
                                RunEvent::Failed {
                                    message: error.to_string(),
                                },
                            );
                        }
                        _ => {}
                    }
                } else if let Ok(completion) = result {
                    let state = inner
                        .registry
                        .lock()
                        .expect("engine registry mutex poisoned")
                        .runs
                        .get(&run_id)
                        .map(|run| run.state.clone());
                    match (state, completion) {
                        (Some(RunState::Stopping { .. }), _) => {
                            let _ = inner.transition(run_id, RunEvent::AdapterStopped);
                        }
                        (Some(state), ExecutorCompletion::Completed(outcome))
                            if !state.is_terminal() =>
                        {
                            let _ = inner.transition(run_id, RunEvent::RunCompleted { outcome });
                        }
                        (Some(state), ExecutorCompletion::Stopped) if !state.is_terminal() => {
                            let _ = inner.transition(
                                run_id,
                                RunEvent::Failed {
                                    message: "executor stopped without a frontend stop request"
                                        .into(),
                                },
                            );
                        }
                        _ => {}
                    }
                }
                inner
                    .executions
                    .lock()
                    .expect("execution registry mutex poisoned")
                    .remove(&run_id);
            });
        if let Err(source) = spawn {
            self.executions
                .lock()
                .expect("execution registry mutex poisoned")
                .remove(&run_id);
            let message = source.to_string();
            let _ = self.transition(
                run_id,
                RunEvent::Failed {
                    message: message.clone(),
                },
            );
            return Err(EngineError::ExecutionSpawnFailed { run_id, message });
        }
        Ok(snapshot)
    }

    fn stop(&self, run_id: RunId) -> Result<RunSnapshot, EngineError> {
        let snapshot = self.transition(run_id, RunEvent::StopRequested)?;
        if let Some(execution) = self
            .executions
            .lock()
            .expect("execution registry mutex poisoned")
            .get(&run_id)
        {
            execution.stop.store(true, Ordering::Release);
        }
        Ok(snapshot)
    }

    fn publish(&self, event: EngineEvent) {
        self.subscribers
            .lock()
            .expect("engine subscriber mutex poisoned")
            .retain(|subscriber| subscriber.send(event.clone()).is_ok());
    }

    fn phase_stats(
        &self,
        run_id: RunId,
        phase_id: PhaseId,
        report: PhaseReport,
    ) -> Result<(), EngineError> {
        let _mutation = self
            .mutation
            .lock()
            .expect("engine mutation mutex poisoned");
        if !self
            .registry
            .lock()
            .expect("engine registry mutex poisoned")
            .runs
            .contains_key(&run_id)
        {
            return Err(EngineError::RunNotFound(run_id));
        }
        self.publish(EngineEvent::PhaseStats {
            run_id,
            phase_id,
            report,
        });
        Ok(())
    }

    fn strategy_decision(
        &self,
        run_id: RunId,
        decision: StrategyDecision,
    ) -> Result<(), EngineError> {
        let _mutation = self
            .mutation
            .lock()
            .expect("engine mutation mutex poisoned");
        if !self
            .registry
            .lock()
            .expect("engine registry mutex poisoned")
            .runs
            .contains_key(&run_id)
        {
            return Err(EngineError::RunNotFound(run_id));
        }
        self.publish(EngineEvent::StrategyDecision { run_id, decision });
        Ok(())
    }
}

struct ExecutionControl {
    stop: Arc<AtomicBool>,
}

struct EngineExecutionSink {
    inner: Arc<EngineInner>,
    run_id: RunId,
}

impl ExecutionSink for EngineExecutionSink {
    fn record_run_event(&mut self, event: RunEvent) -> Result<(), String> {
        self.inner
            .transition(self.run_id, event)
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn record_phase_stats(&mut self, phase_id: PhaseId, report: PhaseReport) -> Result<(), String> {
        self.inner
            .phase_stats(self.run_id, phase_id, report)
            .map_err(|error| error.to_string())
    }

    fn record_strategy_decision(&mut self, decision: StrategyDecision) -> Result<(), String> {
        self.inner
            .strategy_decision(self.run_id, decision)
            .map_err(|error| error.to_string())
    }
}

struct Registry {
    next_run_id: u64,
    runs: BTreeMap<RunId, RunSnapshot>,
}

impl Default for Registry {
    fn default() -> Self {
        Self {
            next_run_id: 1,
            runs: BTreeMap::new(),
        }
    }
}

fn active_run_id(registry: &Registry, excluding: RunId) -> Option<RunId> {
    registry.runs.iter().find_map(|(run_id, snapshot)| {
        (*run_id != excluding
            && matches!(
                &snapshot.state,
                RunState::Starting | RunState::Measuring { .. } | RunState::Stopping { .. }
            ))
        .then_some(*run_id)
    })
}

#[derive(Debug)]
pub enum EngineError {
    RunNotFound(RunId),
    RunIdExhausted,
    RevisionExhausted(RunId),
    ConfigurationLocked(RunId),
    NoAgentsConfigured(RunId),
    AgentsNotPrepared(RunId),
    RunAlreadyActive {
        requested: RunId,
        active: RunId,
    },
    PreparedCohortUnavailable(RunId),
    AgentPreparationFailed {
        run_id: RunId,
        message: String,
    },
    ExecutionSpawnFailed {
        run_id: RunId,
        message: String,
    },
    InvalidWorkload {
        run_id: RunId,
        source: WorkloadError,
    },
    InvalidTransition {
        run_id: RunId,
        source: Box<TransitionError>,
    },
}

impl fmt::Display for EngineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RunNotFound(run_id) => write!(formatter, "run {} does not exist", run_id.0),
            Self::RunIdExhausted => write!(formatter, "run identifier space exhausted"),
            Self::RevisionExhausted(run_id) => {
                write!(formatter, "revision space exhausted for run {}", run_id.0)
            }
            Self::ConfigurationLocked(run_id) => write!(
                formatter,
                "run {} configuration is immutable after the run starts",
                run_id.0
            ),
            Self::NoAgentsConfigured(run_id) => {
                write!(formatter, "run {} has no configured agents", run_id.0)
            }
            Self::AgentsNotPrepared(run_id) => write!(
                formatter,
                "run {} agents must be queried before the run starts",
                run_id.0
            ),
            Self::RunAlreadyActive { requested, active } => write!(
                formatter,
                "run {} cannot use workload agents while run {} is active",
                requested.0, active.0
            ),
            Self::PreparedCohortUnavailable(run_id) => write!(
                formatter,
                "run {} prepared agent sessions are no longer available",
                run_id.0
            ),
            Self::AgentPreparationFailed { run_id, message } => {
                write!(
                    formatter,
                    "failed to prepare agents for run {}: {message}",
                    run_id.0
                )
            }
            Self::ExecutionSpawnFailed { run_id, message } => {
                write!(
                    formatter,
                    "failed to start run {} executor: {message}",
                    run_id.0
                )
            }
            Self::InvalidWorkload { run_id, source } => {
                write!(formatter, "invalid workload for run {}: {source}", run_id.0)
            }
            Self::InvalidTransition { run_id, source } => {
                write!(
                    formatter,
                    "invalid transition for run {}: {source}",
                    run_id.0
                )
            }
        }
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidWorkload { source, .. } => Some(source),
            Self::InvalidTransition { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        io::{BufRead, BufReader, BufWriter, Write},
        net::TcpListener,
        path::PathBuf,
        thread,
    };

    use super::*;
    use crate::config::{
        AgentEndpointConfig, AgentTransportConfig, LoadConfig, OperationSelection, PhaseConfig,
        Preset, Strategy, WeightedOperation, WorkloadConfig,
    };
    use crate::protocol::{
        AdapterIdentity, AdapterMessage, ArgumentKind, ArgumentValue, Capabilities,
        ControllerMessage, LoadModel, OperationArgument, OperationDescriptor, OperationKind,
        PROTOCOL_VERSION,
    };
    use crate::stats::{PhaseReport, summarize_results};

    fn config() -> RunConfig {
        RunConfig {
            preset: Preset::Quick,
            strategy: Strategy::Adaptive,
            phases: PhaseConfig {
                warmup_ms: 1_000,
                measurement_ms: 5_000,
                recovery_ms: 1_000,
                repetitions: 1,
            },
            load: LoadConfig {
                initial_rate: 100.0,
                maximum_rate: 1_000.0,
                growth_factor: 1.5,
                explicit_levels: Vec::new(),
                cycles: 1,
            },
            analysis: Default::default(),
            workload: WorkloadConfig {
                operations: OperationSelection::AdapterDefaults,
            },
            output_directory: PathBuf::from("results"),
            agents: Vec::new(),
        }
    }

    fn tcp_ready_server() -> (String, thread::JoinHandle<()>) {
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
            let message = AdapterMessage::Ready {
                protocol_version: PROTOCOL_VERSION,
                identity: AdapterIdentity {
                    name: "engine-test-adapter".into(),
                    version: Some("1.0.0".into()),
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
                        description: None,
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
                        name: "write".into(),
                        description: None,
                        kind: OperationKind::Write,
                        enabled_by_default: false,
                        default_weight: 1.0,
                        arguments: vec![OperationArgument {
                            name: "value".into(),
                            description: None,
                            kind: ArgumentKind::String,
                            values: Vec::new(),
                            required: true,
                            default: None,
                        }],
                    },
                ],
            };
            let mut output = BufWriter::new(stream);
            serde_json::to_writer(&mut output, &message).unwrap();
            output.write_all(b"\n").unwrap();
            output.flush().unwrap();
        });
        (endpoint, server)
    }

    fn remote_config(endpoint: String) -> RunConfig {
        let mut config = config();
        config.agents.push(AgentEndpointConfig {
            id: "tcp-0".into(),
            transport: AgentTransportConfig::Tcp { address: endpoint },
        });
        config
    }

    #[test]
    fn multiple_frontends_receive_the_same_ordered_events() {
        let engine = Engine::new_manual();
        let handle = engine.handle();
        let cli_events = handle.subscribe();
        let web_events = handle.subscribe();

        let configured = handle
            .execute(EngineCommand::Configure {
                config: Box::new(config()),
            })
            .unwrap();
        let starting = handle
            .execute(EngineCommand::Start {
                run_id: configured.run_id,
            })
            .unwrap();

        assert_eq!(configured.revision, 1);
        assert_eq!(starting.revision, 2);
        assert_eq!(cli_events.recv().unwrap(), web_events.recv().unwrap());
        assert_eq!(cli_events.recv().unwrap(), web_events.recv().unwrap());
    }

    #[test]
    fn runtime_events_update_snapshots_visible_to_frontends() {
        let engine = Engine::new_manual();
        let handle = engine.handle();
        let run = handle
            .execute(EngineCommand::Configure {
                config: Box::new(config()),
            })
            .unwrap();
        handle
            .execute(EngineCommand::Start { run_id: run.run_id })
            .unwrap();

        let baseline = engine
            .record_run_event(run.run_id, RunEvent::AdapterReady)
            .unwrap();

        assert_eq!(
            baseline.state,
            RunState::Measuring {
                stage: crate::measurement::MeasurementStage::Baseline
            }
        );
        assert_eq!(handle.snapshot(run.run_id).unwrap(), baseline);
    }

    #[test]
    fn only_one_run_can_own_workload_agents_at_a_time() {
        let engine = Engine::new_manual();
        let handle = engine.handle();
        let first = handle
            .execute(EngineCommand::Configure {
                config: Box::new(config()),
            })
            .unwrap();
        let second = handle
            .execute(EngineCommand::Configure {
                config: Box::new(config()),
            })
            .unwrap();
        handle
            .execute(EngineCommand::Start {
                run_id: first.run_id,
            })
            .unwrap();

        assert!(matches!(
            handle.execute(EngineCommand::Start {
                run_id: second.run_id,
            }),
            Err(EngineError::RunAlreadyActive {
                requested,
                active,
            }) if requested == second.run_id && active == first.run_id
        ));
        assert!(matches!(
            handle.execute(EngineCommand::PrepareAgents {
                run_id: second.run_id,
            }),
            Err(EngineError::RunAlreadyActive {
                requested,
                active,
            }) if requested == second.run_id && active == first.run_id
        ));
    }

    #[test]
    fn invalid_frontend_command_does_not_publish_an_event() {
        let engine = Engine::new_manual();
        let handle = engine.handle();
        let events = handle.subscribe();

        assert!(
            handle
                .execute(EngineCommand::Start { run_id: RunId(99) })
                .is_err()
        );
        assert!(matches!(events.try_recv(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn a_configured_run_can_be_adjusted_but_a_started_run_cannot() {
        let engine = Engine::new_manual();
        let handle = engine.handle();
        let events = handle.subscribe();
        let run = handle
            .execute(EngineCommand::Configure {
                config: Box::new(config()),
            })
            .unwrap();
        events.recv().unwrap();

        let mut adjusted = config();
        adjusted.load.initial_rate = 250.0;
        let updated = handle
            .execute(EngineCommand::UpdateConfigured {
                run_id: run.run_id,
                config: Box::new(adjusted),
            })
            .unwrap();
        assert_eq!(updated.revision, 2);
        assert_eq!(updated.config.load.initial_rate, 250.0);
        assert!(matches!(
            events.recv().unwrap(),
            EngineEvent::RunConfigurationUpdated { .. }
        ));

        handle
            .execute(EngineCommand::Start { run_id: run.run_id })
            .unwrap();
        assert!(matches!(
            handle.execute(EngineCommand::UpdateConfigured {
                run_id: run.run_id,
                config: Box::new(config()),
            }),
            Err(EngineError::ConfigurationLocked(id)) if id == run.run_id
        ));
    }

    #[test]
    fn phase_stats_are_broadcast_to_frontends() {
        let engine = Engine::new_manual();
        let handle = engine.handle();
        let events = handle.subscribe();
        let run = handle
            .execute(EngineCommand::Configure {
                config: Box::new(config()),
            })
            .unwrap();
        assert!(matches!(
            events.recv().unwrap(),
            EngineEvent::RunConfigured { .. }
        ));

        let report = PhaseReport {
            offered_rate: 100.0,
            goodput_rate: 99.0,
            elapsed_ns: 1_000_000_000,
            in_flight_high_water: 1,
            stats: summarize_results(&[]).unwrap(),
            quality: Default::default(),
        };
        engine
            .record_phase_stats(run.run_id, PhaseId(7), report.clone())
            .unwrap();

        assert_eq!(
            events.recv().unwrap(),
            EngineEvent::PhaseStats {
                run_id: run.run_id,
                phase_id: PhaseId(7),
                report,
            }
        );
    }

    #[test]
    fn preparation_queries_agents_and_retains_the_initialized_cohort() {
        let (endpoint, server) = tcp_ready_server();
        let engine = Engine::new_manual();
        let handle = engine.handle();
        let events = handle.subscribe();
        let configured = handle
            .execute(EngineCommand::Configure {
                config: Box::new(remote_config(endpoint)),
            })
            .unwrap();
        assert!(matches!(
            events.recv().unwrap(),
            EngineEvent::RunConfigured { .. }
        ));

        let prepared = handle
            .execute(EngineCommand::PrepareAgents {
                run_id: configured.run_id,
            })
            .unwrap();
        server.join().unwrap();

        assert_eq!(prepared.revision, 3);
        let AgentPreparation::Ready { catalog } = &prepared.preparation else {
            panic!("expected a ready agent catalog");
        };
        assert_eq!(catalog.agents.len(), 1);
        assert_eq!(
            catalog
                .operations
                .iter()
                .map(|operation| operation.name.as_str())
                .collect::<Vec<_>>(),
            ["read", "write"]
        );
        assert!(matches!(
            events.recv().unwrap(),
            EngineEvent::RunPreparationChanged {
                snapshot: RunSnapshot {
                    preparation: AgentPreparation::Preparing,
                    ..
                },
            }
        ));
        assert!(matches!(
            events.recv().unwrap(),
            EngineEvent::RunPreparationChanged {
                snapshot: RunSnapshot {
                    preparation: AgentPreparation::Ready { .. },
                    ..
                },
            }
        ));

        let started = handle
            .execute(EngineCommand::Start {
                run_id: configured.run_id,
            })
            .unwrap();
        assert_eq!(started.revision, 4);
        drop(engine.take_prepared_cohort(configured.run_id).unwrap());
        assert!(matches!(
            engine.take_prepared_cohort(configured.run_id),
            Err(EngineError::PreparedCohortUnavailable(id)) if id == configured.run_id
        ));
    }

    #[test]
    fn prepared_catalog_validates_required_types_and_duplicate_variants() {
        let (endpoint, server) = tcp_ready_server();
        let engine = Engine::new_manual();
        let handle = engine.handle();
        let configured = handle
            .execute(EngineCommand::Configure {
                config: Box::new(remote_config(endpoint)),
            })
            .unwrap();
        handle
            .execute(EngineCommand::PrepareAgents {
                run_id: configured.run_id,
            })
            .unwrap();
        server.join().unwrap();

        let mut missing = configured.config.clone();
        missing.workload.operations = OperationSelection::Selected {
            operations: vec![WeightedOperation {
                name: "write".into(),
                weight: 1.0,
                arguments: BTreeMap::new(),
            }],
        };
        assert!(matches!(
            handle.execute(EngineCommand::UpdateConfigured {
                run_id: configured.run_id,
                config: Box::new(missing),
            }),
            Err(EngineError::InvalidWorkload {
                source: WorkloadError::MissingArgument { .. },
                ..
            })
        ));

        let numeric_string = WeightedOperation {
            name: "write".into(),
            weight: 2.0,
            arguments: BTreeMap::from([("value".into(), ArgumentValue::String("007".into()))]),
        };
        let mut valid = configured.config.clone();
        valid.workload.operations = OperationSelection::Selected {
            operations: vec![numeric_string.clone()],
        };
        let normalized = handle
            .execute(EngineCommand::UpdateConfigured {
                run_id: configured.run_id,
                config: Box::new(valid.clone()),
            })
            .unwrap();
        let OperationSelection::Selected { operations } = normalized.config.workload.operations
        else {
            panic!("prepared workloads must be materialized");
        };
        assert_eq!(operations[0].weight, 1.0);
        assert_eq!(
            operations[0].arguments.get("value"),
            Some(&ArgumentValue::String("007".into()))
        );

        valid.workload.operations = OperationSelection::Selected {
            operations: vec![numeric_string.clone(), numeric_string],
        };
        assert!(matches!(
            handle.execute(EngineCommand::UpdateConfigured {
                run_id: configured.run_id,
                config: Box::new(valid),
            }),
            Err(EngineError::InvalidWorkload {
                source: WorkloadError::DuplicateSelectedVariant { .. },
                ..
            })
        ));
    }

    #[test]
    fn probe_failure_is_visible_in_the_run_snapshot() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        drop(listener);
        let engine = Engine::new_manual();
        let handle = engine.handle();
        let configured = handle
            .execute(EngineCommand::Configure {
                config: Box::new(remote_config(endpoint)),
            })
            .unwrap();

        assert!(matches!(
            handle.execute(EngineCommand::PrepareAgents {
                run_id: configured.run_id,
            }),
            Err(EngineError::AgentPreparationFailed { .. })
        ));
        assert!(matches!(
            handle.snapshot(configured.run_id).unwrap().preparation,
            AgentPreparation::Failed { .. }
        ));
    }
}
