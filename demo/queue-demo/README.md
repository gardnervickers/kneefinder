# Kneefinder queue demo

This is a separate program for exercising kneefinder end to end:

```text
demo controller -> external adapter process -> fixed-worker internal queue
```

The service has four workers and exposes two operations by default:

- `read`: default operation weight 9
- `write`: default operation weight 1

`read` advertises an integer `key` argument. `key=0` costs 10 ms and `key=1`
costs 20 ms, with a 3:1 workload ratio. `write` advertises a string `value`;
`small` costs 20 ms and `large` costs 40 ms, also with a 3:1 ratio. The adapter
validates every concrete variant before enqueueing it.

All four variants share the same bounded worker queue. Combining the 90/10
operation ratio and 3:1 argument ratios gives an average service time of 13.75
ms and a theoretical knee of about 291 requests per second.

Run the complete demonstration:

```console
cargo run --manifest-path demo/queue-demo/Cargo.toml --release -- e2e
```

The command launches the combined adapter/service as a child process, drives a
range of offered loads through the kneefinder protocol, and prints the overall
curve followed by counts and p50/p95/p99 latency for every fully bound variant.
It cleans up the child when finished.

The adapter/service can also be run directly:

```console
cargo run --manifest-path demo/queue-demo/Cargo.toml --release -- adapter
```

The adapter expects an `initialize` message containing its queue configuration:

```json
{"type":"initialize","protocol_version":1,"run_id":1,"config":{"workers":4,"read_service_ms":10,"write_service_ms":20,"queue_capacity":4096}}
```

Its `ready` response advertises both operations. The E2E controller verifies
the discovery response, schedules the 90/10 mix by operation name, and reports
both blended and per-variant statistics.

The small controller in `e2e` exists only to make the demonstration runnable
while kneefinder's full experiment engine is being built. It deliberately uses
the same controller and adapter messages as that engine.
