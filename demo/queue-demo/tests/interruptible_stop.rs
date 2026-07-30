use std::{
    collections::BTreeMap,
    path::PathBuf,
    time::{Duration, Instant},
};

use kneefinder::{
    config::{
        AdapterCommand, AgentEndpointConfig, AgentTransportConfig, LoadConfig, OperationSelection,
        PhaseConfig, Preset, RunConfig, Strategy, WeightedOperation, WorkloadConfig,
    },
    engine::{Engine, EngineCommand, EngineEvent},
    measurement::RunState,
    protocol::ArgumentValue,
};

#[test]
fn stop_forces_a_hung_colocated_adapter_after_the_cancellation_deadline() {
    let engine = Engine::new();
    let handle = engine.handle();
    let events = handle.subscribe();
    let configured = handle
        .execute(EngineCommand::Configure {
            config: Box::new(RunConfig {
                preset: Preset::Quick,
                strategy: Strategy::Sweep,
                phases: PhaseConfig {
                    warmup_ms: 0,
                    measurement_ms: 5_000,
                    recovery_ms: 0,
                    repetitions: 1,
                },
                load: LoadConfig {
                    initial_rate: 100.0,
                    maximum_rate: 100.0,
                    growth_factor: 2.0,
                    explicit_levels: vec![100.0],
                    cycles: 1,
                },
                analysis: Default::default(),
                workload: WorkloadConfig {
                    operations: OperationSelection::Selected {
                        operations: vec![WeightedOperation {
                            name: "read".into(),
                            weight: 1.0,
                            arguments: BTreeMap::from([("key".into(), ArgumentValue::Integer(0))]),
                        }],
                    },
                },
                output_directory: PathBuf::from("results/interruptible-stop"),
                agents: vec![AgentEndpointConfig {
                    id: "hung-local".into(),
                    transport: AgentTransportConfig::Subprocess {
                        command: AdapterCommand {
                            program: env!("CARGO_BIN_EXE_kneefinder-queue-demo").into(),
                            arguments: vec!["adapter-hang".into()],
                        },
                    },
                }],
            }),
        })
        .unwrap();
    handle
        .execute(EngineCommand::PrepareAgents {
            run_id: configured.run_id,
        })
        .unwrap();
    handle
        .execute(EngineCommand::Start {
            run_id: configured.run_id,
        })
        .unwrap();
    wait_for_state(
        &events,
        configured.run_id,
        Duration::from_secs(2),
        |state| matches!(state, RunState::Measuring { .. }),
    );
    std::thread::sleep(Duration::from_millis(250));

    let stop_started = Instant::now();
    handle
        .execute(EngineCommand::Stop {
            run_id: configured.run_id,
        })
        .unwrap();
    let stopped = wait_for_state(
        &events,
        configured.run_id,
        Duration::from_secs(2),
        |state| matches!(state, RunState::Stopped),
    );

    assert_eq!(stopped, RunState::Stopped);
    assert!(
        stop_started.elapsed() >= Duration::from_millis(900),
        "the adapter should remain hung until the cancellation deadline"
    );
    assert!(
        stop_started.elapsed() < Duration::from_secs(2),
        "the forced subprocess cleanup must bound Stop latency"
    );
}

fn wait_for_state(
    events: &kneefinder::engine::EventSubscription,
    run_id: kneefinder::protocol::RunId,
    timeout: Duration,
    predicate: impl Fn(&RunState) -> bool,
) -> RunState {
    let deadline = Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let event = events
            .recv_timeout(remaining)
            .expect("run should reach the expected state before the deadline");
        if let EngineEvent::RunStateChanged { snapshot, .. } = event
            && snapshot.run_id == run_id
            && predicate(&snapshot.state)
        {
            return snapshot.state;
        }
    }
}
