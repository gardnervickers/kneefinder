# Kneefinder queue demo

This is a separate program for exercising kneefinder end to end. The primary
demo uses Docker Compose to run a web coordinator and two workload agents:

```text
browser -> web coordinator -> TCP agent A -> fixed-worker queue
                           \-> TCP agent B -> fixed-worker queue
```

From the repository root, start the complete demo:

```console
docker compose -f demo/queue-demo/compose.yaml up --build
```

Open <http://127.0.0.1:8080>. The coordinator waits for both agent containers,
opens both TCP sessions, discovers and validates their shared operation schema,
and retains the initialized cohort. It runs a seven-level aggregate sweep and
streams the real phase results to the dashboard. The browser workload editor
shows the typed four-variant catalog discovered from the agents.

Both agents are isolated services on the private Compose network. The
coordinator is always the side that connects, and the dashboard is published
to host loopback only. The containers use the same queue-demo binary with
different commands, so this is also an executable example of independently
deploying the coordinator and workload agents.

After stopping the foreground process with `Ctrl+C`, remove the containers and
network:

```console
docker compose -f demo/queue-demo/compose.yaml down
```

The first image build compiles the release binary with the web feature and can
take a few minutes. Later starts reuse the local image layers.

## Workload

The service has four workers and exposes two operations:

- `read`: safe default operation, weight 9
- `write`: explicit opt-in operation, weight 1

`read` advertises an integer `key` argument. `key=0` costs 10 ms and `key=1`
costs 20 ms, with a 3:1 workload ratio. `write` advertises a string `value`;
`small` costs 20 ms and `large` costs 40 ms, also with a 3:1 ratio. The adapter
validates every concrete variant before enqueueing it.

All four variants share the same bounded worker queue. Combining the 90/10
operation ratio and 3:1 argument ratios gives an average service time of 13.75
ms and a theoretical knee of about 291 requests per second.

## Local modes

The colocated mode remains available as a quick smoke test without Docker:

```console
cargo run --manifest-path demo/queue-demo/Cargo.toml --release -- e2e
```

The command creates a coordinator-owned colocated agent, launches the combined
adapter/service as its supervised child, drives a range of offered loads, and
prints the overall curve followed by counts and p50/p95/p99 latency for every
fully bound variant. It cleans up the child when finished.

Run the process-local multi-client transport E2E:

```console
cargo run --manifest-path demo/queue-demo/Cargo.toml -- e2e-tcp
```

This command starts two independent listener processes on ephemeral loopback
ports. The coordinator connects to both, validates their schemas, splits an
80-operation schedule round-robin, and verifies 40 successful operations per
agent plus 80 successful aggregate operations.

Run the same full multi-client sweep and web UI without Docker:

```console
cargo run --manifest-path demo/queue-demo/Cargo.toml --features web -- e2e-tcp-web
```

Open <http://127.0.0.1:8080>. The dashboard receives the real phase reports from
both TCP agents through the shared engine and remains available after the sweep
completes. Before the sweep, the engine itself connects to both agents, queries
their shared operation catalog, retains the initialized cohort, and exposes the
typed four-variant workload in the browser editor.

The adapter/service can also be run directly:

```console
cargo run --manifest-path demo/queue-demo/Cargo.toml --release -- adapter
```

Or expose the same protocol over a persistent TCP connection:

```console
cargo run --manifest-path demo/queue-demo/Cargo.toml --release -- adapter-tcp 127.0.0.1:9000
```

The TCP agent listens; the coordinator is always the side that connects.

The adapter expects an `initialize` message containing its queue configuration:

```json
{"type":"initialize","protocol_version":2,"run_id":1,"config":{"workers":4,"read_service_ms":10,"write_service_ms":20,"queue_capacity":4096}}
```

Its `ready` response advertises adapter identity, capabilities, and both
operations. The E2E controller verifies the discovery response, schedules the
90/10 mix by operation name, and reports both blended and per-variant
statistics.

The small coordinator in `e2e` exists only to make the demonstration runnable
while kneefinder's full experiment engine is being built. It uses the reusable
`AgentCohort`, `ColocatedAgent`, `TcpAgent`, and shared adapter session from the
main crate rather than maintaining another protocol.
