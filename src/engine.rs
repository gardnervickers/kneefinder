//! Frontend-neutral command, snapshot, and event boundary for the run engine.

use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, RecvError, RecvTimeoutError, TryRecvError},
    },
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{
    config::RunConfig,
    measurement::{RunEvent, RunState, TransitionError},
    protocol::{PhaseId, RunId},
    stats::PhaseReport,
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
    RunStateChanged {
        previous: RunState,
        snapshot: RunSnapshot,
    },
    PhaseStats {
        run_id: RunId,
        phase_id: PhaseId,
        report: PhaseReport,
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
            inner: Arc::new(EngineInner::default()),
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
            EngineCommand::Start { run_id } => {
                self.inner.transition(run_id, RunEvent::StartRequested)
            }
            EngineCommand::Stop { run_id } => {
                self.inner.transition(run_id, RunEvent::StopRequested)
            }
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

#[derive(Default)]
struct EngineInner {
    mutation: Mutex<()>,
    registry: Mutex<Registry>,
    subscribers: Mutex<Vec<mpsc::Sender<EngineEvent>>>,
}

impl EngineInner {
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
        config: RunConfig,
    ) -> Result<RunSnapshot, EngineError> {
        let _mutation = self
            .mutation
            .lock()
            .expect("engine mutation mutex poisoned");
        let snapshot = {
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
            run.revision = run
                .revision
                .checked_add(1)
                .ok_or(EngineError::RevisionExhausted(run_id))?;
            run.config = config;
            run.clone()
        };

        self.publish(EngineEvent::RunConfigurationUpdated {
            snapshot: snapshot.clone(),
        });
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

#[derive(Debug)]
pub enum EngineError {
    RunNotFound(RunId),
    RunIdExhausted,
    RevisionExhausted(RunId),
    ConfigurationLocked(RunId),
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
            Self::InvalidTransition { source, .. } => Some(source.as_ref()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use crate::config::{
        LoadConfig, OperationSelection, PhaseConfig, Preset, Strategy, WorkloadConfig,
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
            workload: WorkloadConfig {
                operations: OperationSelection::AdapterDefaults,
            },
            output_directory: PathBuf::from("results"),
            adapter: None,
        }
    }

    #[test]
    fn multiple_frontends_receive_the_same_ordered_events() {
        let engine = Engine::new();
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
        let engine = Engine::new();
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
    fn invalid_frontend_command_does_not_publish_an_event() {
        let engine = Engine::new();
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
        let engine = Engine::new();
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
        let engine = Engine::new();
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
            stats: summarize_results(&[]).unwrap(),
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
}
