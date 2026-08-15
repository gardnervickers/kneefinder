# Managed-phase throughput benchmark

## Goal

- Target: remove coordinator and protocol overhead as the limiting factor for high-throughput open-loop workloads
- Primary metric: highest offered rate with complete accounting and valid dispatch lag
- Direction: higher is better
- Correctness checks: requested offers reconcile with terminal outcomes, histogram samples and variant totals; generator-limited runs are rejected

## Environment

- Machine: local Codex host
- Execution mode: local
- OS: Linux 6.18.39 x86_64
- CPU: AMD Ryzen 9 5950X, 16 cores / 32 threads, frequency boost enabled
- Memory: 62 GiB, no swap
- Runtime/toolchain: rustc 1.97.1, cargo 1.97.1
- Commit/worktree state: rebased onto `635d4f8` plus the changes documented below

## Methodology

- Benchmark command: release `kneefinder run` through a colocated synthetic adapter
- Workload: environment-free no-op operations through the real CLI, session, and adapter protocol
- Warmup: 2 seconds
- Measurement: 10 seconds
- Repetitions: five around each observed healthy ceiling
- Summary statistic: highest offered rate accepted without incomplete accounting or generator saturation
- Noise threshold: a ceiling is trusted only when all five repetitions agree on validity and achieved goodput remains within 1% of offered load
- Notes: the scheduled path is the pre-change baseline; managed phases are the sole final execution path

## Baseline

- Date/time: 2026-08-15T12:01:42-04:00
- Command: `target/release/kneefinder run --strategy sweep --levels RATE --maximum-rate RATE --warmup 2s --measurement 10s --recovery 0ms --repetitions 1 --output /tmp/kneefinder-baseline-RATE -- /tmp/kneefinder-baseline-adapter/target/release/kneefinder-baseline-adapter`
- Raw output: `/tmp/kneefinder-baseline-{10k,25k,50k,100k}`

| Offered ops/s | Goodput ops/s | Attempts | Dispatch p99 | Dispatch max |
| ---: | ---: | ---: | ---: | ---: |
| 10,000 | 10,000.0 | 100,000 | 4.30 ms | not recorded |
| 25,000 | 24,985.6 | 250,000 | 6.59 ms | 10.50 ms |
| 50,000 | 50,000.0 | 500,000 | 7.63 ms | 15.61 ms |
| 100,000 | 100,000.0 | 1,000,000 | 8.80 ms | 11.98 ms |

- Summary: scheduled mode remained complete through 100,000 ops/s on this fixture.
- Interpretation: 100,000 ops/s is the configured ceiling for a 10-second phase because the executor rejects more than 1,000,000 retained results. A 100,001 ops/s trial failed before measurement with `a measurement phase exceeds the retained result limit of 1000000 operations`. The baseline therefore establishes a lower bound on healthy scheduled throughput and a confirmed result-retention ceiling; it does not locate the machine's actual scheduled-mode capacity.

## Attempts

### 1. Incremental scheduled aggregation (superseded)

- Change: replace the per-phase `Vec<OperationResult>` with the same bounded
  histogram/count/bucket accumulator used for managed summaries.
- Result: accepted. The one-million-result rejection is removed and scheduled
  mode no longer retains one object per completed operation.
- Five-trial boundary: 150,000 ops/s remained within 0.1% of offered load;
  175,000 ops/s delivered 166,298-167,936 in-window successes/s with 407-503 ms
  dispatch p99.
- Decision: remove the scheduled path after the managed contract proved both
  faster and bounded; retain these measurements only as historical comparison.

### 2. Adapter-managed phases and mergeable histograms

- Change: protocol v4 capability negotiation, prepare/start barrier with one
  absolute coordinator-owned start time, fixed global-stream sharding, exact
  counts/buckets, and base64-encoded HdrHistogram V2 summaries.
- Result: accepted. All five 2.25M ops/s trials had exact
  offered=started=completed accounting, essentially full goodput, and 0.25-2.55
  ms dispatch p99.
- Adjacent point: all five 2.5M ops/s trials completed their attempts, but only
  2.419-2.446M successes/s completed in-window and dispatch p99 grew to 217-328
  ms. This point is generator-limited under the benchmark's 1% criterion.

### 3. Saturated-generator accounting

- Observation: an exploratory 5M ops/s managed trial initially exposed a
  mismatch between starts after the measurement window and fixed bucket totals.
- Change: allow in-window bucket starts to be a subset of overall starts and
  record every missed intended deadline explicitly as an unstarted offer.
- Result: accepted. A repeated 5M trial reconciled all 10,000,000 offers,
  reported 9,469,082 starts/completions, and classified the generator as
  saturated without fabricating latency samples.

## Final repeated results

- Date/time: 2026-08-15T12:41:24-04:00
- Commands: the methodology command with `--repetitions 5`, using rates 150k
  and 175k for scheduled mode and 2.25M and 2.5M for adapter-managed mode
- Raw output: `/tmp/kneefinder-final-{scheduled-150k,scheduled-175k,managed-2250k,managed-2500k}`

| Mode | Offered ops/s | Trials | Goodput range | Dispatch p99 range | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| Legacy scheduled | 150,000 | 5 | 149,913.5 | 23.63-24.02 ms | accepted |
| Legacy scheduled | 175,000 | 5 | 166,297.5-167,935.9 | 407.37-503.32 ms | generator-limited |
| Managed | 2,250,000 | 5 | 2,249,999.3-2,250,000.0 | 0.25-2.55 ms | accepted |
| Managed | 2,500,000 | 5 | 2,418,818.0-2,445,890.5 | 217.32-328.47 ms | generator-limited |

After resolving the rebase onto `main`, one final release confirmation at
2.25M ops/s again reported exactly 2.25M successful operations/s for the full
ten-second measurement. Its artifact is
`/tmp/kneefinder-exact-head-2250k/run-1786814773946390203-1`.

## Cumulative result

- Accepted changes: bounded HdrHistogram V2 codec; managed prepare/start
  protocol; fixed sharding; exact aggregate validation; explicit
  unstarted-offer accounting; removal of the lower-throughput execution path
- Baseline summary: 100,000 ops/s was the highest representable ten-second
  phase because 1,000,000 raw results were retained
- Final summary: 2.25M ops/s is the repeated managed ceiling on this host and
  fixture
- Cumulative delta: 22.5x over the former configured baseline ceiling; 15x over
  the post-change scheduled ceiling
- Confidence: high for this machine/fixture boundary because both accepted and
  adjacent rejected rates were repeated five times with exact counts
- Remaining ideas: parallel adapter scheduler implementations and compressed
  histogram encoding
