# Kneefinder design

## Purpose

Kneefinder finds the load at which a system stops scaling normally. It drives a
user-provided workload, measures offered load, goodput, latency, errors, and
load-generator health, and reports both a statistical knee and a conservative
operating limit.

The target system and its client library are deliberately outside kneefinder.
A workload adapter is the boundary between the generic experiment engine and a
specific database, service, queue, or other system.

## Design principles

- Offered load is the independent variable. Achieved throughput alone hides
  overload once a system has saturated.
- Scheduling policy belongs to kneefinder. Adapter authors should normally
  provide only the operation that calls their native client.
- Adapter IPC must not be included in target latency and must not silently
  become the limiting resource.
- The interactive UI, terminal UI, and non-interactive CLI all control the same
  run engine and consume the same event stream.
- Results are reproducible artifacts. A completed run must remain useful
  without the UI that created it.
- A noisy or invalid experiment is reported as such; kneefinder does not always
  manufacture a knee.

## Components

```text
                           commands
  CLI / TUI / web UI  ----------------->  run engine
         ^                                    |
         | events                             | phase plan
         |                                    v
    artifact writer                     agent cohort
         ^                               /          \
         | observations       stdio session       TCP session
         |                         |                  |
         +----------------- colocated agent     remote agent
                                   \                /
                                    \ native calls /
                                     target system
```

### Run engine

The run engine owns configuration, lifecycle, load selection, measurement
validation, knee fitting, and artifact generation. It exposes typed commands
and events rather than UI-specific callbacks.

Commands include:

- configure a new run
- query and retain its configured agent cohort
- start a configured run
- request graceful stop
- force cancellation after a deadline
- subscribe to observations and lifecycle events

A run configuration is immutable after start. Changing configuration creates a
new run, which keeps result provenance unambiguous.

The public frontend boundary consists of a cloneable `EngineHandle`, typed
`EngineCommand` values, immutable `RunSnapshot` values, and broadcast
`EngineEvent` subscriptions. Snapshots carry monotonically increasing revisions
so a reconnecting browser or a frontend racing with an update can de-duplicate
state. The core serializes mutations and event publication; renderers never
mutate measurement state directly.

The core runtime retains a separate owner handle used to record adapter and
measurement events. This keeps runtime-only transitions out of frontend APIs.
Blocking event subscriptions are transport abstractions rather than a promise
that every frontend uses blocking I/O: a future web frontend can bridge a
subscription into its async runtime.

Completed phases publish a frontend-neutral `PhaseStats` event containing the
overall summary and every bound operation variant. CLI artifact writers, TUIs,
and web graphing layers therefore consume identical series without performing
their own grouping or percentile calculations.

Frontend dependencies are feature-gated. The core library builds with no
frontend features, while the `cli` feature supplies the command-line frontend
and binary. TUI and web implementations can live in separate packages, depend
on the feature-free core, and choose their own runtime and rendering stack.

### Workload agents and placement

The run engine coordinates a fixed cohort of workload agents. An agent owns an
adapter session, executes its assigned portion of the global schedule, and
returns results with stable agent attribution. The coordinator validates that
every cohort member advertises compatible capabilities and the same operation
schema before starting a phase.

The default mode is **colocated**: the agent runs inside the coordinator process
and supervises the adapter as a child. This preserves the zero-setup
`kneefinder run -- ./adapter` experience and is also useful when one machine can
generate enough load. Multiple colocated agents may be used to isolate several
client runtimes on the same host.

In distributed mode, separately deployed agents implement the same typed
adapter protocol over persistent NDJSON/TCP. There is no second
coordinator/agent protocol and no mandatory sidecar. A client program may expose
the TCP listener and call its target's native client directly. Containers,
Kubernetes, and EC2 are responsible only for starting those programs; they do
not own experiment scheduling. Each agent exposes an explicitly configured
endpoint and the coordinator establishes the persistent session. Agents never
dial or register with the coordinator; they only answer requests or stream
results on a session the coordinator opened. Docker DNS names, EC2 private
addresses, or Kubernetes headless-service/pod addresses may supply those
endpoints without introducing scheduler-specific discovery into the core.

Completing or stopping a run disconnects its coordinator-owned sessions; it
does not terminate separately deployed agent processes. A remote agent returns
to listening for the next coordinator session. The protocol `Shutdown` message
is reserved for an explicit agent-termination action rather than ordinary run
cleanup.

Cohort membership is frozen for a run. The coordinator divides aggregate load
deterministically and sends every member the same absolute phase start. A lost
or late agent invalidates the phase; its allocation is never silently moved to
a surviving agent mid-measurement. The cohort result retains per-agent
attribution before aggregate statistics are computed. Explicit clock-skew
estimation and richer per-agent artifact reports remain required before remote
measurements can support strong target-knee claims.

### Web control plane

The optional `web` feature serves a dependency-free browser client and a
versioned API:

```console
cargo run --features web -- serve --bind 127.0.0.1:8080
```

`GET /api/v1/snapshot` returns all run snapshots and retained phase results.
`POST /api/v1/commands` accepts the same serialized `EngineCommand` used by
other frontends. `GET /api/v1/ws` upgrades to a full-duplex WebSocket: clients
send commands with request IDs and receive acknowledgements, lifecycle events,
per-phase statistics, and completed knee estimates. A bounded broadcast buffer
prevents slow clients from impeding the engine; a lagged client receives a
fresh snapshot and an explicit resynchronization notice.

The browser GUI uses that API for configuration, agent discovery, structured
workload editing, start/stop controls, run history, throughput and latency
curves, knee markers, and per-variant tables. Agent preparation has its own
`unprepared`/`preparing`/`ready`/`failed` snapshot state so discovery does not
pretend that measurement has started. Configuration may be edited while a run
is still configured. Once started it is immutable, so a changed workload
becomes a new run rather than silently altering the experiment being measured.

The server binds only to loopback by default. Non-loopback binding is rejected
unless `--allow-remote` is supplied because this first API version has no
authentication or TLS termination; remote deployments should put it behind an
authenticated reverse proxy. Browser command and WebSocket requests also
require their `Origin` to match the server host, while non-browser API clients
may omit `Origin`.

### Workload-agent protocol and transports

In colocated mode, the coordinator-owned agent launches the adapter and passes
its executable and arguments as an argv array:

```console
kneefinder run [kneefinder options] -- ./my-adapter --endpoint db.example.com
```

The protocol consists only of the versioned `ControllerMessage` and
`AdapterMessage` types. Session state, discovery validation, scheduling,
cancellation, and result validation are independent of I/O. Two transport
bindings currently carry those exact messages:

- **Subprocess:** the agent reads NDJSON from stdin, writes NDJSON to stdout,
  and writes human-readable logs only to stderr.
- **TCP:** the agent listens on an explicitly configured address and the
  coordinator opens one persistent full-duplex stream carrying the same NDJSON
  frames.

TCP was selected before HTTP because a persistent byte stream preserves the
existing framing and naturally supports asynchronous results and cancellation.
HTTP can be added as another transport if deployment evidence warrants it; it
does not require a new workload protocol.

The initial TCP transport provides neither authentication nor encryption. Do
not expose it directly to an untrusted network. Cross-host deployments should
use private networking plus restrictive security groups/network policies, or a
mutually authenticated TLS tunnel, until native authenticated TLS is added.

Remote endpoints may be combined with the colocated child syntax:

```console
kneefinder run \
  --agent-endpoint client-a=tcp://10.0.0.10:9000 \
  --agent-endpoint client-b=tcp://10.0.0.11:9000 \
  -- ./local-adapter
```

Secrets should be supplied through inherited environment variables, files, or
the initialization message rather than command-line arguments visible in a
process listing.

### Operation discovery

After initialization, the adapter's `ready` message advertises its name,
optional implementation version, capabilities, and supported operations. Each
operation has a stable name, description, broad kind (`read`, `write`,
`administrative`, or `other`), default participation and weight, and a list of
simple named arguments. Concrete arguments are either signed integers or
strings and may be required or provide a default. An enum argument advertises
an ordered, non-empty set of allowed string values, so interactive frontends
can render a dropdown while scheduled operations continue to carry ordinary
string values. This intentionally small type system is easy to implement
consistently across languages and straightforward for CLI, TUI, and browser
frontends to render.

The run configuration selects one of three workload forms:

- adapter defaults
- every advertised operation, explicitly requested
- a weighted list of fully bound operation variants

`all` is never implicit because an adapter may advertise mutating or
administrative operations. The driver validates names and weights after the
handshake. An operation name plus one concrete argument set is the atomic
schedulable and statistical unit: `read(key=0)` and `read(key=1)` are distinct
variants with independent weights and results. This flat model avoids separate
and potentially ambiguous operation and argument ratios.

Operation results retain the concrete arguments. Reports contain an overall
summary plus one summary per distinct bound variant, including attempt/success/
error/timeout counts and p50/p95/p99 client latency, total latency, and dispatch
lag. The reporter enforces a configurable variant-cardinality limit so dynamic
IDs cannot accidentally create an unbounded number of series.

Failures retain the adapter's optional stable error code. Phase summaries expose
error, timeout, and combined unsuccessful rates plus counts per error code both
overall and for each bound variant. Error codes are expected to be
low-cardinality classifications such as `overloaded` or `connection_reset`, not
request-specific messages. Reliability is graphed against offered load so a
broken workload or client is visible alongside the throughput and latency
curves rather than being mistaken for a target knee.

Each completed phase wraps those statistics with offered rate, successful
goodput, elapsed time, fixed measurement buckets, and a stationarity decision.
This shared `PhaseReport` is the graph point consumed by artifact writers and
streaming frontends, keeping graph semantics out of the HTTP/WebSocket layer.
Traversal choices are separate `StrategyDecision` events, including their
stage, action, selected and next rate, and reason. Browser run results retain
those decisions so reconnecting clients do not lose provenance.

CLI examples:

```console
kneefinder run --operation read -- ./adapter
kneefinder run --operation read=9 --operation write=1 -- ./adapter
kneefinder run --operation 'read:key=0@27' --operation 'read:key=1@9' \
  --operation 'write:value=small@3' --operation 'write:value=large@1' -- ./adapter
kneefinder run --all-operations -- ./adapter
```

CLI values that parse as integers are represented as integers; other values are
strings. A numeric-looking string can be forced with a `str:` prefix, for
example `write:value=str:123@1`. `@` specifies the relative variant weight;
the older argument-free `read=9` form remains accepted. The driver validates
configured names, required values, defaults, and types against discovery before
scheduling work.

Because weights apply to flat variants, nested ratios multiply. A 90/10
read/write mix with 3:1 argument ratios becomes weights `27, 9, 3, 1` for the
four variants shown above.

An interactive frontend prepares a configured run before starting it. The
coordinator establishes each configured subprocess or TCP session, performs the
normal initialization/ready handshake, validates the cohort schema, publishes
the catalog in the run snapshot, and retains those initialized sessions for the
executor. Discovery is not a separate adapter-specific API and agents never
register with or call the coordinator.

The browser renders that catalog as a structured workload editor. It
materializes safe defaults, uses the advertised argument type for each input,
renders enum choices as dropdowns, preserves numeric-looking strings as
strings, permits several concrete variants of one operation, and validates
required arguments, weights, and duplicates.
Resolved relative weights are normalized to sum to one in `RunConfig`.
Operations not marked as defaults require an explicit add action.

## Adapter execution modes

### Scheduled operations (default)

Kneefinder computes operation deadlines and sends them ahead of time in
batches. A reusable language runtime dispatches the supplied user callback at
those deadlines, measures the native client call, and returns batched results.
Adapter authors do not implement the ramp algorithm or choose rates.

Each result carries:

- operation identifier
- advertised operation name
- intended start offset
- actual start offset
- completion offset or client latency
- success, error, or timeout status

Batching keeps IPC off the critical scheduling path. Dispatch lag reveals when
the adapter runtime or load generator cannot keep up.

Official language runtimes should make the user-facing adapter approximately an
async callback. The runtime owns protocol handling, timers, concurrency,
measurement, batching, and error normalization.

### Adapter-managed phase (escape hatch)

An adapter may advertise a capability to execute a complete phase and return
histograms. This supports extremely high-throughput systems for which even
batched operation dispatch is material. It is more difficult to compare across
languages, so it is not the default and must pass protocol conformance tests.

## Measurement model

For every operation:

```text
dispatch lag = actual call start - intended start
client latency = completion - actual call start
total latency = completion - intended start
```

Client latency describes the native client and target. Dispatch lag describes
the generator. Total latency represents the experience of traffic offered at
the requested time and prevents coordinated omission from hiding overload.

Each phase records:

- requested and actual duration
- offered, started, completed, successful, failed, and timed-out counts
- offered throughput and successful goodput
- client-latency, total-latency, and dispatch-lag histograms
- in-flight request high-water mark
- fixed-duration time buckets for stationarity and confidence analysis

Timeouts are never discarded. They remain explicit counts and contribute at
least the timeout duration to censored latency statistics.

A phase has a warm-up interval followed by a measurement interval. Warm-up
results are intentionally discarded. Fixed-duration measurement buckets compare
goodput over the phase; material drift marks the phase non-stationary. Adaptive
runs repeat a rejected phase within the configured repetition budget and
terminate as `unstable_measurement` when that budget is exhausted. Fixed
traversals retain rejected points but classify the run as unstable. Configured
recovery intervals execute between visits and are emitted as strategy
provenance.

## Knee-finding algorithm

### 1. Baseline

Measure a deliberately low offered load to establish latency, errors,
throughput efficiency, and normal variance. Repeat or extend the phase until it
is stable or the configured budget is exhausted.

### 2. Discovery

Increase offered load geometrically, initially by a configurable factor such as
1.5. Continue until there is evidence that the target saturated:

- goodput stops tracking offered load
- latency slope increases materially
- errors or timeouts increase
- in-flight work accumulates

Growing dispatch lag instead classifies the generator as saturated and makes
the target knee invalid at that rate.

Discovery produces a bracket consisting of the last clearly healthy load and
the first clearly saturated load.

### 3. Refinement

Measure points between the bracket bounds. Because load is multiplicative, use
the geometric midpoint:

```text
next load = sqrt(lower load * upper load)
```

Repeat ambiguous points and stop when the bracket is sufficiently narrow, the
statistical uncertainty dominates further refinement, or the run budget is
exhausted.

### 4. Fit and validation

Fit a continuous two-segment model to goodput versus offered load. The proposed
knee is the breakpoint at which the second slope becomes meaningfully smaller
than the first. Compare this model with a single straight line; if the segmented
model is not a meaningful improvement, report that no knee was observed.

Validate the candidate using latency, errors, in-flight accumulation, and
dispatch lag. Estimate an interval for the breakpoint using time-bucket block
bootstrap or repeated phases. Store the exact fitting method and parameters in
the result for reproducibility.

The result distinguishes:

- estimated statistical knee
- confidence interval or healthy/saturated bracket
- maximum load satisfying user-provided latency and error SLOs
- recommended operating maximum, optionally applying a safety factor

Possible terminal classifications include `target_saturated`,
`generator_saturated`, `slo_exceeded`, `unstable_measurement`,
`no_knee_observed`, `maximum_load_reached`, `stopped`, and `failed`.

## Run lifecycle

```text
configured -> starting -> baseline -> discovery -> refinement -> validation
     |            |          |           |            |             |
     +------------+----------+-----------+------------+-------> stopping
                                                                  |
                                                     stopped <----+

validation -> completed
any nonterminal state -> failed
```

Stopping is cooperative first: stop scheduling operations, retain results
already received, send `CancelPhase` for in-flight work, and finalize a partial
artifact. After a deadline, kneefinder kills a colocated adapter subprocess or
drops an unresponsive remote session. Normal Stop never sends the explicit
`Shutdown` command to a remote agent process. A stopped run remains inspectable
but is never presented as a completed result.

## Frontends and UI

The engine is a library. Frontends communicate with an engine handle using
typed commands and receive a broadcast stream of typed events. This prevents
the web UI, terminal UI, and batch CLI from growing separate measurement logic.

Frontends implement a deliberately small lifecycle interface that receives an
`EngineHandle`. It does not prescribe terminal access, an HTTP framework, an
async runtime, or a rendering library. New frontends should subscribe before
taking their initial snapshot and use snapshot revisions to ignore duplicate or
stale updates.

### Headless CLI

`kneefinder run` works without a terminal and uses exit status plus machine-
readable artifacts. While attached to a terminal it shows progress and a live
compact chart; redirected output remains stable and script-friendly.

Suggested commands:

```console
kneefinder run --config run.toml -- ./adapter
kneefinder inspect results/run-id/summary.json
kneefinder render results/run-id/summary.json --format terminal
kneefinder render results/run-id/summary.json --format svg
```

Run configuration is CLI-first and does not use YAML. The initial interface
provides `quick`, `careful`, and `hysteresis` presets with explicit overrides:

```console
kneefinder run --preset quick --maximum-rate 5000 -- ./adapter
kneefinder run --preset hysteresis --levels 100,200,400,800 --cycles 3 -- ./adapter
kneefinder run --warmup 5s --measurement 20s --recovery 5s -- ./adapter
```

`--print-config` emits the fully resolved plan as JSON without running it. If a
reusable human-authored configuration file is added later, it will use TOML.

The first terminal renderer should use Unicode/Braille cells when supported and
fall back to plain ASCII. It plots offered load against goodput and latency,
marks measured points, shades the knee interval, and labels the selected knee.
Static SVG is preferable as the primary graphical artifact because it is easy
to generate, scales cleanly, and opens in a browser; PNG can be an optional
render target.

### Interactive terminal UI

A TUI can configure, start, and stop local runs and show:

- current lifecycle stage and offered load
- throughput and latency time series
- goodput/latency curve with the evolving bracket and candidate knee
- errors, timeouts, dispatch lag, and adapter logs
- prior measured points and phase stability

Keyboard and screen-reader-friendly textual summaries must accompany graphical
symbols. The TUI sends the same start and stop commands as other frontends.

### Browser UI

A later `kneefinder serve` command can expose the engine over loopback HTTP and
a live event stream. The browser UI provides richer configuration, zoomable
graphs, comparison of runs, and artifact inspection. Binding beyond loopback
requires explicit configuration and authentication; arbitrary adapter commands
must not be exposed by default.

The browser is a client of the engine API, not the owner of a run. Closing or
reloading the page does not terminate the experiment. It exposes resolved
configuration values and traversal strategies directly rather than presenting
CLI presets whose only purpose is to select starting values. Strategy help
distinguishes the up/down traversal from the hysteresis it is intended to
detect. Run progress is based on completed planned phases for fixed traversals
and lifecycle stages for adaptive runs.

The current traversal engine implements baseline, geometric discovery, and
geometric midpoint refinement using conservative throughput-efficiency,
latency, and unsuccessful-rate evidence. It keeps generator saturation and
phase instability as separate terminal classifications. The statistical model
comparison, confidence interval, and numerical knee estimate described below
remain the fitter's responsibility; traversal alone reports no fabricated knee.

## Events and persistence

Useful engine events include:

- run state changed
- adapter ready or exited
- phase started, progress updated, and phase completed
- measurement point accepted, rejected, or scheduled for repetition
- bracket changed
- candidate knee changed
- warning or failure recorded
- artifacts finalized

High-frequency operation results need not be broadcast to every frontend. The
engine aggregates them into bounded-rate snapshots while the artifact writer
retains the data required for later analysis.

Each run receives a unique identifier and an artifact directory containing at
least:

```text
summary.json          stable machine-readable result
config.json           fully resolved non-secret configuration
measurements.ndjson   phase and time-bucket observations
report.svg            portable graph and result summary
adapter.log           captured adapter diagnostics
```

Optional `report.png` and raw operation samples may be enabled. Artifacts are
written incrementally and finalized atomically so a crash or manual stop still
leaves recoverable measurements.

The JSON summary contains schema and protocol versions, timestamps, tool and
adapter identity, configuration, environment metadata, all measured load
points, validity warnings, fitting parameters, knee interval, SLO capacity,
and terminal classification.

## Initial implementation slices

1. Typed protocol messages and a lifecycle reducer with transition tests.
2. Transport-independent adapter session with supervised NDJSON subprocess and
   coordinator-initiated persistent TCP bindings.
3. Fixed agent cohort, colocated/remote session agents, deterministic schedule
   fan-out, phase aggregation, and generator-lag checks.
4. Baseline, geometric discovery, and bracket refinement.
5. Segmented fit, confidence interval, and JSON artifacts.
6. Terminal progress and static SVG report.
7. Interactive TUI.
8. Browser UI and run comparison.

The fake adapter should simulate configurable latency curves, saturation,
errors, and generator lag. It will make the search algorithm and every frontend
testable without a real target system.

The first external fixture is maintained as the separate Rust package under
`demo/queue-demo`. It combines a real fixed-worker queue with its agent in one
external process. Its small end-to-end coordinator exercises both the real
colocated stdio path and a two-process TCP cohort before the full kneefinder
executor is available.
