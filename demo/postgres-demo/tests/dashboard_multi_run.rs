#![cfg(feature = "web")]

use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use kneefinder::{
    artifact::load_artifact,
    config::RunConfig,
    engine::{AgentPreparation, EngineCommand, RunSnapshot},
    frontends::web::ApiSnapshot,
    measurement::RunState,
    protocol::{ArgumentKind, RunId},
};

struct DemoProcess(Child);

impl Drop for DemoProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
#[ignore = "requires PostgreSQL; run with KNEEFINDER_POSTGRES_URL set"]
fn dashboard_completes_adjusts_stops_and_reruns_on_persistent_agents() {
    let port = unused_loopback_port();
    let address = format!("127.0.0.1:{port}");
    let _demo = DemoProcess(
        Command::new(env!("CARGO_BIN_EXE_kneefinder-postgres-demo"))
            .args(["e2e-tcp-web", &address])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("dashboard demo should start"),
    );

    let ready = wait_for_snapshot(&address, RunId(1), Duration::from_secs(10), |run| {
        run.state == RunState::Configured
            && matches!(&run.preparation, AgentPreparation::Ready { .. })
    });
    let AgentPreparation::Ready { catalog } = &ready.preparation else {
        unreachable!("wait predicate requires a ready catalog");
    };
    let size = catalog
        .operations
        .iter()
        .find(|operation| operation.name == "transfer")
        .and_then(|operation| {
            operation
                .arguments
                .iter()
                .find(|argument| argument.name == "route")
        })
        .expect("PostgreSQL demo should advertise its transfer route argument");
    assert_eq!(size.kind, ArgumentKind::Enum);
    assert_eq!(size.values, ["hot", "cold"]);
    thread::sleep(Duration::from_millis(300));
    let still_idle = snapshot(&address)
        .runs
        .into_iter()
        .find(|run| run.run_id == ready.run_id)
        .expect("initial configured run should remain visible");
    assert_eq!(still_idle.state, RunState::Configured);
    assert!(
        snapshot(&address)
            .results
            .iter()
            .all(|result| result.run_id != ready.run_id),
        "the dashboard must not execute a workload before Start"
    );
    command(
        &address,
        &EngineCommand::Start {
            run_id: ready.run_id,
        },
    );

    let first = wait_for_run(&address, ready.run_id, Duration::from_secs(20), |state| {
        matches!(state, RunState::Completed { .. })
    });
    let first_results = run_result(&snapshot(&address), first.run_id);
    assert_eq!(first_results.phases.len(), 7);
    assert_artifact(&first, 7..=7, |state| {
        matches!(state, RunState::Completed { .. })
    });

    let mut adjusted = first.config.clone();
    adjusted.load.explicit_levels = vec![120.0, 180.0];
    adjusted.load.initial_rate = 120.0;
    adjusted.load.maximum_rate = 180.0;
    adjusted.phases.warmup_ms = 50;
    adjusted.phases.measurement_ms = 300;
    adjusted.phases.recovery_ms = 25;
    let adjusted = configure_prepare_start(&address, adjusted);
    let adjusted = wait_for_run(
        &address,
        adjusted.run_id,
        Duration::from_secs(10),
        |state| matches!(state, RunState::Completed { .. }),
    );
    let adjusted_results = run_result(&snapshot(&address), adjusted.run_id);
    assert_eq!(adjusted_results.phases.len(), 2);
    assert_eq!(
        adjusted_results
            .phases
            .iter()
            .map(|phase| phase.report.offered_rate)
            .collect::<Vec<_>>(),
        [120.0, 180.0]
    );

    let mut stoppable = adjusted.config.clone();
    stoppable.load.explicit_levels = vec![140.0];
    stoppable.load.initial_rate = 140.0;
    stoppable.load.maximum_rate = 140.0;
    stoppable.phases.warmup_ms = 0;
    stoppable.phases.measurement_ms = 5_000;
    stoppable.phases.recovery_ms = 0;
    let stoppable = configure_prepare_start(&address, stoppable);
    let stopped = wait_for_run(
        &address,
        stoppable.run_id,
        Duration::from_secs(5),
        |state| matches!(state, RunState::Measuring { .. }),
    );
    let stop_started = Instant::now();
    command(
        &address,
        &EngineCommand::Stop {
            run_id: stoppable.run_id,
        },
    );
    wait_for_run(
        &address,
        stoppable.run_id,
        Duration::from_secs(2),
        |state| matches!(state, RunState::Stopped),
    );
    assert!(
        stop_started.elapsed() < Duration::from_secs(2),
        "graceful stop should not wait for the five-second measurement"
    );
    assert_artifact(&stopped, 0..=1, |state| matches!(state, RunState::Stopped));

    let mut rerun = adjusted.config;
    rerun.load.explicit_levels = vec![160.0];
    rerun.load.initial_rate = 160.0;
    rerun.load.maximum_rate = 160.0;
    rerun.phases.warmup_ms = 0;
    rerun.phases.measurement_ms = 300;
    rerun.phases.recovery_ms = 0;
    let rerun = configure_prepare_start(&address, rerun);
    wait_for_run(&address, rerun.run_id, Duration::from_secs(10), |state| {
        matches!(state, RunState::Completed { .. })
    });
    let rerun_results = run_result(&snapshot(&address), rerun.run_id);
    assert_eq!(rerun_results.phases.len(), 1);
    assert_eq!(rerun_results.phases[0].report.offered_rate, 160.0);
}

fn assert_artifact(
    run: &RunSnapshot,
    expected_phases: std::ops::RangeInclusive<usize>,
    state_matches: impl Fn(&RunState) -> bool,
) {
    let directory = run
        .artifact_directory
        .as_ref()
        .expect("started dashboard run should expose its artifact directory");
    for name in [
        "summary.json",
        "config.json",
        "measurements.ndjson",
        "report.svg",
        "adapter.log",
    ] {
        assert!(directory.join(name).is_file(), "missing {name}");
    }
    let artifact = load_artifact(directory).expect("dashboard artifact should be inspectable");
    assert!(state_matches(&artifact.state));
    assert!(expected_phases.contains(&artifact.phases.len()));
    assert_eq!(artifact.agents.len(), 2);
}

fn configure_prepare_start(address: &str, config: RunConfig) -> RunSnapshot {
    let configured = command(
        address,
        &EngineCommand::Configure {
            config: Box::new(config),
        },
    );
    command(
        address,
        &EngineCommand::PrepareAgents {
            run_id: configured.run_id,
        },
    );
    command(
        address,
        &EngineCommand::Start {
            run_id: configured.run_id,
        },
    )
}

fn wait_for_run(
    address: &str,
    run_id: RunId,
    timeout: Duration,
    predicate: impl Fn(&RunState) -> bool,
) -> RunSnapshot {
    wait_for_snapshot(address, run_id, timeout, |run| predicate(&run.state))
}

fn wait_for_snapshot(
    address: &str,
    run_id: RunId,
    timeout: Duration,
    predicate: impl Fn(&RunSnapshot) -> bool,
) -> RunSnapshot {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(run) = snapshot(address)
            .runs
            .into_iter()
            .find(|run| run.run_id == run_id)
            && predicate(&run)
        {
            return run;
        }
        assert!(
            Instant::now() < deadline,
            "run {} did not reach the expected state within {timeout:?}",
            run_id.0
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn run_result(snapshot: &ApiSnapshot, run_id: RunId) -> kneefinder::frontends::web::RunResults {
    snapshot
        .results
        .iter()
        .find(|result| result.run_id == run_id)
        .cloned()
        .expect("run should have retained phase results")
}

fn snapshot(address: &str) -> ApiSnapshot {
    let body = http(address, "GET", "/api/v1/snapshot", None);
    serde_json::from_str(&body).expect("snapshot response should be valid JSON")
}

fn command(address: &str, command: &EngineCommand) -> RunSnapshot {
    let body = serde_json::to_string(command).unwrap();
    let response = http(address, "POST", "/api/v1/commands", Some(&body));
    serde_json::from_str(&response).unwrap_or_else(|error| {
        panic!("command response should be a run snapshot: {error}: {response}")
    })
}

fn http(address: &str, method: &str, path: &str, body: Option<&str>) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut stream = loop {
        match TcpStream::connect(address) {
            Ok(stream) => break stream,
            Err(error) if Instant::now() < deadline => {
                let _ = error;
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => panic!("dashboard did not accept connections: {error}"),
        }
    };
    let body = body.unwrap_or("");
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    stream.flush().unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("HTTP response should contain headers");
    assert!(
        headers.starts_with("HTTP/1.1 200"),
        "unexpected HTTP response: {headers}\n{body}"
    );
    body.into()
}

fn unused_loopback_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}
