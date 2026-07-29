# Kneefinder agent context

This file is the durable handoff for agents working in this repository. Read it
before making design or implementation changes, then inspect the relevant code,
README, design document, and GitHub issue because implementation status can
move ahead of this summary.

## Project goal

Kneefinder is a generic load-testing utility that drives a system at increasing
offered load and finds the point where throughput stops scaling normally and
latency begins reflecting queueing delay. That point is the knee.

Kneefinder must not know how to call a particular database, service, queue, or
client library. A user-provided adapter describes its operations and invokes the
target's native client. The adapter may be written in any language.

The product must work headlessly as well as interactively:

- CLI for configuration, automation, inspection, and rendering
- optional TUI
- HTTP/WebSocket API and browser GUI
- durable JSON/NDJSON and graphical artifacts that remain useful without a UI

## Non-negotiable design decisions

### Kneefinder owns scheduling

Do not push rate selection, ramp traversal, or knee-finding into every adapter
language. Kneefinder selects offered load and schedules operations. Adapters
should remain thin; reusable language runtimes should eventually reduce the
user-facing implementation to operation discovery plus an async callback.

The default execution mode sends scheduled operations ahead in batches. An
adapter-managed whole-phase mode is only a high-throughput escape hatch and
must preserve equivalent measurement semantics and pass conformance tests.

### The adapter protocol is cross-language and transport-independent

Protocol messages are typed and versioned in `src/protocol.rs`. The first and
default transport is newline-delimited JSON over a child process's stdin and
stdout. Human-readable logs go only to stderr. Never let adapter logs corrupt
stdout framing.

Keep message/session logic independent of subprocess I/O. Persistent TCP is an
additional transport under issue #10, not a replacement for zero-setup stdio.
Remote multi-client work must account for client identity, aggregate load
allocation, start barriers and clock skew, per-client attribution, disconnects,
and fan-out cancellation rather than treating networking as a transparent byte
pipe.

The coordinator is always the initiating side of the workload control plane.
Remote agents expose explicitly configured endpoints; they never dial,
register with, or otherwise initiate calls to the coordinator. An agent may
reply or stream results on a coordinator-established session, but every
session and unit of work originates from the coordinator. Preserve this call
direction in local, Docker, Kubernetes, and EC2 deployments.

Normal run completion or stop disconnects the coordinator-owned session and
leaves a remote agent listening for another session. Sending the protocol
`Shutdown` command is an explicit agent-process termination action, not normal
run cleanup. Do not add a one-shot TCP agent mode.

Do not include adapter IPC time in native client latency. Batching and dispatch
lag exist so the load generator cannot silently become the bottleneck.

### A bound operation variant is the atomic workload unit

An operation name plus its complete concrete argument map is one schedulable
and statistical operation. For example, `read(key=0)` and `read(key=1)` have
independent weights and independent statistics.

- concrete argument values are deliberately limited to signed integers and
  strings; enum descriptors add an ordered set of allowed string values
- adapter discovery declares required/default arguments and safe defaults
- weights apply to flat variants; nested ratios are multiplied by the caller
- `all operations` is always explicit because advertised operations may mutate
  data or be administrative
- results echo the operation and concrete arguments
- bound the number of distinct variants to prevent unbounded series

### Measurement validity is more important than producing a knee

Offered load is the independent variable. Achieved throughput alone hides
saturation. For each operation retain these separate measurements:

```text
dispatch lag = actual call start - intended start
client latency = completion - actual call start
total latency = completion - intended start
```

Client latency describes the native client and target. Dispatch lag describes
the generator. Total latency prevents coordinated omission from hiding the
experience of traffic offered at the intended time.

Never discard errors or timeouts. Report attempts, successes, failures,
timeouts, unsuccessful rate, and low-cardinality stable error-code counts both
overall and per variant. Timeouts remain censored latency observations, not
missing samples.

Growing dispatch lag or exhausted generator capacity invalidates a target-knee
claim. Noisy, unstable, linear, budget-limited, and stopped experiments should
receive explicit classifications; do not manufacture a knee.

### All frontends share one engine

The core exposes frontend-neutral `EngineHandle`, `EngineCommand`,
`RunSnapshot`, and `EngineEvent` types. CLI, TUI, web, artifact writers, and
tests must consume the same lifecycle and phase reports rather than duplicating
measurement, grouping, percentile, or fitting logic.

Subscribe before taking an initial snapshot. Snapshot revisions are monotonic
and let reconnecting or lagged clients de-duplicate/resynchronize. A run's
configuration is immutable after start; a changed configuration becomes a new
run so provenance remains clear.

Frontend dependencies stay feature-gated. The core must continue to build with
no default features. The web server binds to loopback by default; non-loopback
control requires an explicit opt-in and authenticated TLS deployment. The
browser is a client of the engine, not the owner of a run.

### Configuration is CLI-first and not YAML

Preserve the pleasant CLI surface and resolved, frontend-independent
`RunConfig`. Presets and explicit overrides cover warmup, measurement,
recovery, repetitions, load levels, growth, traversal strategy, cycles,
operation variants, weights, and output location.

`--print-config` emits the fully resolved plan as JSON without running. If a
reusable human-authored config file is added, use TOML rather than YAML.

### Results are reproducible artifacts

The intended run directory contains at least:

```text
summary.json
config.json
measurements.ndjson
report.svg
adapter.log
```

Write observations incrementally and finalize atomically so stopped or crashed
runs remain inspectable. Store schema/protocol versions, non-secret resolved
configuration, adapter/tool identity, environment metadata, measurement
decisions, validity warnings, fit parameters, knee bounds, SLO capacity, and
terminal classification. Do not persist secrets from argv or environment.

## Knee-finding model

The intended adaptive flow is:

1. Establish a stable low-load baseline.
2. Increase offered load geometrically until healthy and saturated points form
   a bracket. Treat generator saturation separately.
3. Refine with geometric midpoints because load is multiplicative.
4. Fit a continuous two-segment goodput model and compare it with a single-line
   null model.
5. Validate the candidate with latency, errors, timeouts, in-flight growth, and
   dispatch lag.
6. Estimate confidence from repeated phases or time-bucket block bootstrap and
   derive a conservative operating rate and optional SLO-limited capacity.

Warmup, measurement, recovery, repetitions, and up/down cycles are configurable
because real systems can need time to warm, drain, or expose hysteresis. Fixed
time buckets should support stationarity checks, repeats, and uncertainty.

## Repository map

- `src/protocol.rs`: versioned controller/adapter messages and capability model
- `src/adapter_session.rs`: protocol-state validation plus supervised stdio and
  coordinator-initiated persistent TCP transports
- `src/agent.rs`: fixed cohorts and generic transport-backed session agents
- `src/workload.rs`: operation discovery validation and flat variant resolution
- `src/stats.rs`: overall/per-variant counts, error codes, and distributions
- `src/measurement.rs`: pure run lifecycle and outcome types
- `src/engine.rs`: shared commands, snapshots, revisions, and event broadcast
- `src/config.rs`: resolved frontend-independent run configuration
- `src/frontends/cli.rs`: CLI parsing and presets
- `src/frontends/web.rs`: optional HTTP/WebSocket control plane
- `src/frontends/web/`: dependency-free browser assets
- `examples/rust-adapter.rs`: runnable adapter-authoring example
- `demo/queue-demo`: separate combined adapter/service and E2E controller
- `docs/design.md`: architecture and measurement design, including aspirational
  sections that are not necessarily implemented
- `docs/images`: README dashboard screenshots

## Current implementation boundary

At the time this handoff was written, these pieces exist:

- protocol and operation discovery types
- integer, string, and enum arguments plus flat weighted variants
- per-variant latency, error, timeout, and error-code statistics
- lifecycle reducer and frontend-neutral engine API
- CLI configuration and `--print-config`
- HTTP/WebSocket API and browser dashboard
- coordinator-owned agent preparation with discovery events and retained
  initialized cohorts
- discovery-driven browser workload editor with typed arguments and enum
  dropdowns, safe defaults, explicit opt-in operations, and multiple bound
  variants
- Rust adapter example
- supervised subprocess adapter session, bounded stderr diagnostics, and TCP
  transport
- fixed agent cohort with stable identities plus colocated and remote session
  implementations
- runnable queue demonstrations using the colocated path and two TCP clients,
  including repeated browser-configured runs and graceful stop/rerun

The generic executor now turns a prepared cohort and `RunConfig` into bounded,
deterministic scheduled-operation batches for CLI and browser runs. It supports
warmup, measured intervals, recovery, repetitions, fixed sweep/up-down plans,
per-phase statistics, generator-saturation invalidation, and cooperative stop.
The queue demo exercises this production path for both colocated and
multi-client web runs. Adaptive bracketing/refinement, statistical knee fitting,
durable artifacts, and an in-flight transport cancellation deadline remain
roadmap work. Keep README's Current status section honest as work progresses.

`docs/design.md` is the contract and direction, not proof of implementation.
Inspect source and current GitHub issue state before relying on a described
command or component.

## Queue demo contract

`demo/queue-demo` is a separate Rust package that combines a protocol adapter
and a fixed-worker FIFO service. Its coordinator uses the real `AgentCohort`,
`ColocatedAgent`, `TcpAgent`, and adapter session. It is the current complete
measured E2E path for both colocated and multi-client transport modes.

The default workload has four workers and four variants with weights 27:9:3:1:

- `read(key=0)`: 10 ms
- `read(key=1)`: 20 ms
- `write(value=small)`: 20 ms
- `write(value=large)`: 40 ms

The weighted mean service time is 13.75 ms, so the theoretical shared-queue
knee is about 291 operations/second. The demo should show goodput flattening
and p95 latency rising near/after that point. Avoid fragile assertions on exact
wall-clock numbers.

Run it with:

```console
cargo run --release --manifest-path demo/queue-demo/Cargo.toml -- e2e
```

Run the two-client TCP transport E2E with:

```console
cargo run --manifest-path demo/queue-demo/Cargo.toml -- e2e-tcp
```

Keep the demo external to the main crate. When the generic session/executor is
implemented, use the demo as a black-box compatibility/E2E fixture instead of
maintaining a divergent controller protocol.

## Roadmap issues

GitHub issues are the source of truth for backlog state. The initial dependency
chain is #1 -> #2 -> #3 -> #4, with artifact and frontend work building on it.

- [#1](https://github.com/gardnervickers/kneefinder/issues/1): supervised NDJSON subprocess adapter session
- [#2](https://github.com/gardnervickers/kneefinder/issues/2): generic run executor
- [#3](https://github.com/gardnervickers/kneefinder/issues/3): sweep, up-down, and adaptive strategies
- [#4](https://github.com/gardnervickers/kneefinder/issues/4): statistical knee fitting and validation
- [#5](https://github.com/gardnervickers/kneefinder/issues/5): reproducible artifacts and inspect/render commands
- [#6](https://github.com/gardnervickers/kneefinder/issues/6): terminal progress and SVG reports
- [#7](https://github.com/gardnervickers/kneefinder/issues/7): interactive TUI
- [#8](https://github.com/gardnervickers/kneefinder/issues/8): reusable Rust adapter runtime and conformance suite
- [#9](https://github.com/gardnervickers/kneefinder/issues/9): adapter probing and discovery-driven workload editor
- [#10](https://github.com/gardnervickers/kneefinder/issues/10): remote transport and multi-client coordination
- [#11](https://github.com/gardnervickers/kneefinder/issues/11): persisted browser run history and comparison
- [#12](https://github.com/gardnervickers/kneefinder/issues/12): adapter-managed high-throughput phases
- [#13](https://github.com/gardnervickers/kneefinder/issues/13): CI across feature sets and the queue demo

Update or close the applicable issue when implementation lands. Do not create a
parallel TODO list in code when an existing issue already captures the work.

## Development and verification

This is Rust 2024. The root crate's default feature is `cli`; `web` is optional,
and the core supports `--no-default-features`. The queue demo has its own
manifest and lockfile.

Use the repository's locked dependencies. Prefer offline checks when the Cargo
cache is available:

```console
cargo fmt --check
cargo test --offline
cargo test --offline --no-default-features
cargo test --offline --features web
cargo clippy --offline --all-targets -- -D warnings
cargo clippy --offline --no-default-features --all-targets -- -D warnings
cargo clippy --offline --features web --all-targets -- -D warnings
cargo build --offline --no-default-features --example rust-adapter
cargo test --offline --manifest-path demo/queue-demo/Cargo.toml
cargo clippy --offline --manifest-path demo/queue-demo/Cargo.toml --all-targets -- -D warnings
```

Run the release queue E2E for changes to protocol, scheduling, measurement,
statistics, or the demo. For web changes, also launch the server and verify the
HTTP endpoints, WebSocket snapshot/event flow, and dashboard visually.

Keep changes scoped and preserve unrelated work in a dirty checkout. Update
README/docs when user-visible behavior or design contracts change. Do not
weaken measurement invariants merely to make a graph look cleaner or force a
knee classification.
