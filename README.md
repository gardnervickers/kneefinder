# Kneefinder

Kneefinder drives an arbitrary system at increasing offered load and finds the
point where throughput stops scaling normally and latency begins reflecting
queueing delay. That point is the **knee**.

The benchmark driver is generic. A small external adapter describes the
operations it supports and calls the native client for the system being tested.
The transport-independent adapter protocol is versioned newline-delimited JSON,
so an agent can be written in any language and reached through either a
supervised stdio process or a persistent TCP connection.

![Kneefinder dashboard showing a throughput knee](docs/images/dashboard-knee.png)

## What it measures

- offered load and successful goodput
- p50, p95, and p99 client and total latency
- load-generator dispatch lag
- failures, timeouts, unsuccessful rate, and failures grouped by stable error code
- overall results and independent series for every bound operation variant
- a knee estimate, confidence bounds, and a conservative operating rate

An operation plus its concrete arguments is one workload variant. For example,
`lookup(account=1)` and `lookup(account=2)` receive independent weights and
independent statistics.

Long runs expose their current phase, warmup/measurement/recovery segment,
elapsed time, scheduled and reported operations, and an estimated remaining
time. Completed points stream into the charts as preliminary evidence; the knee
remains hidden until the run finishes statistical validation.

![Kneefinder dashboard showing live hysteresis progress](docs/images/dashboard-progress.png)

## Try the containerized multi-agent demo

The included demo runs a real PostgreSQL 18.3 database, two independent
workload agents, and the browser coordinator:

```text
browser -> web coordinator -> TCP agent A -\
                           \-> TCP agent B ---> PostgreSQL
```

Start all four containers with either Docker Compose:

```console
docker compose -f demo/postgres-demo/compose.yaml up --build
```

or Podman Compose:

```console
podman-compose -f demo/postgres-demo/compose.yaml up --build
```

Open <http://127.0.0.1:8080>. The coordinator waits for both agents, initiates
both TCP sessions, queries their operation catalogs, validates that their
schemas match, and exposes the discovered workload in the browser. It does not
start workload traffic automatically. Review or edit the prepared seven-level
plan, then press Start. The dashboard updates with a live run timeline that
plots goodput and p95 client latency in completed-phase order, plus throughput
and latency capacity curves sorted by offered load, reliability, per-variant
results, and live warmup, measurement, and recovery progress with elapsed time
and operation activity. Knee estimates remain hidden until final validation.
Around the knee, goodput flattens while latency rises sharply. After that run,
edit the load levels, timings, strategy, operation variants, or remote-agent
list. The dashboard saves the form and checks agent connectivity automatically;
press Start when it reports ready. The browser walks through runner mode,
traversal strategy, strategy-relevant presets and parameters, and discovered
operations in order. It explains each strategy in place and keeps irrelevant
controls hidden; preset descriptions explain whether they favor fast feedback,
stable estimates, or hysteresis detection.
The seven-level hysteresis preset traverses 13 phases per cycle for three
cycles: 39 phases total. At 10 seconds of warmup, 20 seconds of measurement,
and 15 seconds of recovery per phase, final knee validation takes roughly 29
minutes. The dashboard shows that phase count and a live ETA throughout.
Stop ends the active run while leaving both agents available for the next one.
Run artifacts are retained in the `postgres-demo-results` named volume. To copy
them into the current directory before tearing the demo down:

```console
docker compose -f demo/postgres-demo/compose.yaml cp web:/demo/results ./results
```

Use `podman-compose` in the command above when applicable.

The agents only listen on the private Compose network; they never dial or
register with the coordinator. They remain available for subsequent runs
configured in the dashboard, even after a prior run disconnects its sessions.
The dashboard is published on loopback only. When finished, stop the foreground
process with `Ctrl+C` and remove the demo containers:

```console
docker compose -f demo/postgres-demo/compose.yaml down
```

Use `podman-compose` in the command above if that is how the demo was started.

The workload performs real MVCC lookups and transactional account transfers.
The hot transfer holds a PostgreSQL row lock for 10 ms, making its serialization
ceiling easy to see: 16% of traffic takes that path, so the controlled component
predicts a knee near 625 offered operations per second. PostgreSQL and host
overhead move the fitted result somewhat; the E2E accepts 450–850 ops/s rather
than asserting one wall-clock value.

For local Rust runs, start only PostgreSQL:

```console
docker compose -f demo/postgres-demo/compose.yaml up -d postgres
```

Then exercise the release-mode adaptive traversal:

```console
KNEEFINDER_POSTGRES_URL=postgres://kneefinder:kneefinder@127.0.0.1:55432/kneefinder \
  cargo run --release --manifest-path demo/postgres-demo/Cargo.toml -- e2e-adaptive
```

Exercise the normal `kneefinder run` CLI against the bundled adapter with a
short, complete sweep:

```console
cargo build --release --manifest-path demo/postgres-demo/Cargo.toml
KNEEFINDER_POSTGRES_URL=postgres://kneefinder:kneefinder@127.0.0.1:55432/kneefinder \
  cargo run --release -- run \
  --strategy sweep \
  --levels 150,300,450,600,750,1000,1400 \
  --warmup 0ms \
  --measurement 1s \
  --recovery 0ms \
  --operation 'lookup:account=1@32' \
  --operation 'lookup:account=2@8' \
  --operation 'transfer:route=hot@8' \
  --operation 'transfer:route=cold@2' \
  -- demo/postgres-demo/target/release/kneefinder-postgres-demo adapter
```

This takes about seven seconds of configured measurement time and should report
a knee around PostgreSQL's hot-row contention limit. See the
[demo guide](demo/postgres-demo/README.md) for fixed, adaptive, TCP, browser,
and integration-test commands.

## Browser UI and API

Build and serve the optional web frontend:

```console
cargo run --features web -- serve
```

Open <http://127.0.0.1:8080>. The server exposes:

| Endpoint | Purpose |
| --- | --- |
| `GET /api/v1/snapshot` | Current run state, live phase progress, and retained results |
| `POST /api/v1/commands` | Submit a serialized engine command |
| `GET /api/v1/ws` | Full-duplex commands, acknowledgements, and streamed events |
| `GET /healthz` | Process and API-version health |

The WebSocket sends an initial snapshot, then lifecycle and phase events. Slow
clients receive an explicit resynchronization snapshot rather than silently
continuing with gaps. The server binds to loopback by default; remote binding
requires `--allow-remote` and should be placed behind authenticated TLS.

The browser first saves the configured colocated and TCP endpoints, then sends
`prepare_agents`. The coordinator opens every session, performs the normal
`initialize`/`ready` handshake, validates that the cohort shares one schema,
and retains those initialized sessions for the run. The resulting operation
catalog updates the browser dynamically. Its structured editor uses typed
integer and text inputs plus dropdowns for advertised enum choices, supports
multiple bound variants of one operation, and keeps operations that are not
safe defaults behind an explicit add action.

## Configure a workload

The same resolved configuration is shared by CLI, web, and future TUI
frontends. The PostgreSQL demo's 80/20 lookup/transfer mix becomes four flat
weights:

```console
cargo run -- run \
  --operation 'lookup:account=1@32' \
  --operation 'lookup:account=2@8' \
  --operation 'transfer:route=hot@8' \
  --operation 'transfer:route=cold@2' \
  --print-config
```

Remove `--print-config` and put a colocated adapter command after `--` to
execute the resolved plan. The CLI prepares the adapter, streams one progress
line per measured phase, and prints the terminal run snapshot as JSON:

```console
cargo run -- run \
  --strategy sweep \
  --levels 100,200,400 \
  --measurement 10s \
  --latency-slo-ms 25 \
  --maximum-unsuccessful-rate 0.01 \
  --safety-factor 0.8 \
  --operation 'read:key=0@1' \
  -- ./adapter
```

A run may combine the colocated `-- ./adapter` mode with explicitly addressed
remote agents:

```console
cargo run -- run \
  --agent-endpoint client-a=tcp://10.0.0.10:9000 \
  --agent-endpoint client-b=tcp://10.0.0.11:9000 \
  --print-config \
  -- ./local-adapter
```

The coordinator always initiates these TCP sessions. Agents listen on the
configured endpoints and never dial or register with the coordinator.

## Inspect and render run artifacts

Every started CLI or browser run creates a unique directory below its resolved
output directory. The directory remains useful after completion, Stop, a
frontend disconnect, or an interrupted write:

```text
summary.json
config.json
measurements.ndjson
report.svg
adapter.log
```

`measurements.ndjson` is flushed incrementally. The other files are replaced
atomically, and inspection recovers records that are newer than a stale
`summary.json` while ignoring a truncated final NDJSON line. Adapter subprocess
commands are redacted from persisted configuration.

Inspect a run directory or its `summary.json` directly:

```console
cargo run -- inspect results/run-123-1
cargo run -- inspect results/run-123-1/summary.json --json
cargo run -- render results/run-123-1 --output report-copy.svg
```

Batch exit statuses are stable: `0` for a completed result, `2` for stopped,
`3` for invalid or generator-limited measurements, `4` for a failed run, and
`1` for CLI or artifact-reading errors.

Durations and load traversal are configurable without a YAML file. This command
prints the complete seven-level hysteresis plan without executing its 39
phases:

```console
cargo run -- run \
  --preset hysteresis \
  --levels 150,300,450,600,750,1000,1400 \
  --print-config
```

## Write an adapter

An agent implements one protocol regardless of transport. The zero-setup mode
uses a child process whose stdin and stdout carry one JSON message per line;
logs go to stderr. The remote mode listens for a coordinator-initiated TCP
connection and carries the same newline-delimited messages over that stream.
Its lifecycle is:

1. Receive `initialize` and reply with `ready`, including adapter identity,
   capabilities, and operation descriptors.
2. Receive batches of scheduled operations with absolute phase time and relative
   per-operation deadlines.
3. Call the target's native client and return measured operation results.
4. Echo the operation name and concrete arguments in every result.
5. Return stable, low-cardinality error codes and represent timeouts explicitly.

The complete stdio Rust example is [examples/rust-adapter.rs](examples/rust-adapter.rs),
and the PostgreSQL demo provides both stdio and TCP implementations against a
real native client.
The only target-specific part is the function that calls the system under test:

```rust
fn call_target(
    operation: &str,
    arguments: &BTreeMap<String, ArgumentValue>,
) -> Result<(), &'static str> {
    match operation {
        "get" => my_client.get(integer_argument(arguments, "key")?)
            .map(|_| ())
            .map_err(|_| "get_failed"),
        "put" => my_client.put(
            integer_argument(arguments, "key")?,
            string_argument(arguments, "value")?,
        ).map_err(|_| "put_failed"),
        _ => Err("unknown_operation"),
    }
}
```

Compile the example adapter with:

```console
cargo build --no-default-features --example rust-adapter
```

The production direction is to provide reusable language runtimes that own the
protocol, deadline dispatch, batching, and measurement, leaving adapter authors
with only an async callback like `call_target`.

## Error reporting

Errors are never discarded from latency data or hidden inside throughput.
Every overall and per-variant summary contains:

- attempts, successes, failures, and timeouts
- counts grouped by the adapter's error code
- latency and dispatch-lag distributions

The Rust API and dashboard derive error, timeout, and combined unsuccessful
rates from those counts without losing the exact underlying totals.

This makes a broken client, invalid workload, overloaded generator, and
saturated target visibly different failure modes.

## Current status

The adapter protocol, supervised subprocess and persistent TCP transports,
coordinator-owned colocated and remote session agents, fixed multi-agent cohort,
workload model, measurement types, statistics, lifecycle engine, PostgreSQL
demonstration, CLI configuration, HTTP/WebSocket control plane, and browser UI
are implemented. The engine and browser also implement coordinator-owned agent
preparation, retained initialized cohorts, discovery events, and a typed
workload editor. The PostgreSQL demo exercises both the colocated path and a
two-agent TCP cohort, including a Docker/Podman Compose deployment with the web
coordinator and one shared PostgreSQL instance. Its real MVCC lookup and hot-row
transaction workload can execute repeated browser-configured runs and
gracefully stop an active run while retaining its agents. The production engine now turns
prepared cohorts and `RunConfig` values into bounded, deterministic scheduled
operation batches for CLI and browser runs, including warmup, measurement,
recovery, repetitions, sweep/up-down traversal, per-phase statistics, and
bounded interruptible stop. Adaptive runs now establish a stable baseline,
discover a healthy/saturated bracket geometrically, and refine it with
geometric midpoints. Fixed time buckets can reject non-stationary phases;
adaptive runs repeat them within the configured repetition budget. Every load
selection, acceptance, repetition, rejection, and recovery interval is emitted
through the shared engine API and retained in browser run results. The executor
also emits frontend-neutral live phase progress, which the browser retains for
reconnects and renders with segment timing, operation activity, and fixed-plan
ETA. Completed
runs fit a continuous two-segment goodput model, compare it with a single-line
null model, validate the candidate using latency, reliability, in-flight
growth, and dispatch lag, and estimate deterministic confidence bounds from
measurement buckets. Resolved configuration records optional latency/error
SLOs, the safety factor, bootstrap count, and seed. The terminal result retains
both model parameters, every validity signal, the SLO capacity, the knee
interval, and the conservative operating recommendation. Stop preserves
results already received, sends
`CancelPhase` to active agents, and force-closes an unresponsive session after
the cancellation deadline. Every started run now persists schema-versioned,
redacted configuration and provenance, incremental phase/decision records,
terminal analysis, adapter diagnostics, and a regenerable SVG report. The CLI
can inspect completed or partial runs as text or JSON and regenerate reports.

See [docs/design.md](docs/design.md) for the measurement and knee-finding design.
