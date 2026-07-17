# Kneefinder

Kneefinder drives an arbitrary system at increasing offered load and finds the
point where throughput stops scaling normally and latency begins reflecting
queueing delay. That point is the **knee**.

The benchmark driver is generic. A small external adapter describes the
operations it supports and calls the native client for the system being tested.
The adapter protocol is versioned newline-delimited JSON, so the adapter can be
written in any language.

![Kneefinder dashboard showing a throughput knee](docs/images/dashboard-knee.png)

## What it measures

- offered load and successful goodput
- p50, p95, and p99 client and total latency
- load-generator dispatch lag
- failures, timeouts, unsuccessful rate, and failures grouped by stable error code
- overall results and independent series for every bound operation variant
- a knee estimate, confidence bounds, and a conservative operating rate

An operation plus its concrete arguments is one workload variant. For example,
`read(key=0)` and `read(key=1)` receive independent weights and independent
statistics.

![Kneefinder reliability and per-variant reporting](docs/images/dashboard-reliability.png)

The reliability view above uses fault-injected high-load phases to demonstrate
error-code grouping and explicit timeout reporting.

## Try the queue demo

The included demo combines an adapter with a four-worker FIFO service. Its
weighted service time gives it a theoretical knee near 291 requests per second.

```console
cargo run --release --manifest-path demo/queue-demo/Cargo.toml -- e2e
```

The demonstration starts the adapter as a child process, performs the protocol
handshake, schedules seven offered-load levels, and prints the throughput,
latency, and per-variant results. Around the knee, goodput flattens while latency
rises sharply.

## Browser UI and API

Build and serve the optional web frontend:

```console
cargo run --features web -- serve
```

Open <http://127.0.0.1:8080>. The server exposes:

| Endpoint | Purpose |
| --- | --- |
| `GET /api/v1/snapshot` | Current run state and retained phase results |
| `POST /api/v1/commands` | Submit a serialized engine command |
| `GET /api/v1/ws` | Full-duplex commands, acknowledgements, and streamed events |
| `GET /healthz` | Process and API-version health |

The WebSocket sends an initial snapshot, then lifecycle and phase events. Slow
clients receive an explicit resynchronization snapshot rather than silently
continuing with gaps. The server binds to loopback by default; remote binding
requires `--allow-remote` and should be placed behind authenticated TLS.

## Configure a workload

The same resolved configuration is shared by CLI, web, and future TUI
frontends. A 90/10 read/write mix with 3:1 argument ratios becomes four flat
weights:

```console
cargo run -- run \
  --operation 'read:key=0@27' \
  --operation 'read:key=1@9' \
  --operation 'write:value=small@3' \
  --operation 'write:value=large@1' \
  --print-config
```

Durations and load traversal are configurable without a YAML file:

```console
cargo run -- run \
  --preset careful \
  --strategy up-down \
  --levels 100,200,300,400 \
  --warmup 10s \
  --measurement 30s \
  --recovery 15s \
  --cycles 3 \
  --print-config
```

## Write an adapter

An adapter is a process whose stdin and stdout carry one JSON message per line.
Logs go to stderr. Its lifecycle is:

1. Receive `initialize` and reply with `ready`, including operation descriptors.
2. Receive batches of scheduled operations with absolute phase time and relative
   per-operation deadlines.
3. Call the target's native client and return measured operation results.
4. Echo the operation name and concrete arguments in every result.
5. Return stable, low-cardinality error codes and represent timeouts explicitly.

The complete runnable Rust example is [examples/rust-adapter.rs](examples/rust-adapter.rs).
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

The adapter protocol, workload model, measurement types, statistics, lifecycle
engine, queue demonstration, CLI configuration, HTTP/WebSocket control plane,
and browser UI are implemented. The generic executor that connects `kneefinder
run` and the web Start button to an arbitrary adapter is the next major piece;
until then, use the queue demo for the complete measured flow.

See [docs/design.md](docs/design.md) for the measurement and knee-finding design.
