# Kneefinder PostgreSQL demo

This package exercises kneefinder end to end against PostgreSQL 18.3. The
primary demo runs a browser coordinator, two remote workload agents, and one
shared database:

```text
browser -> web coordinator -> TCP agent A -\
                           \-> TCP agent B ---> PostgreSQL
```

From the repository root, start the complete demo with Docker Compose:

```console
docker compose -f demo/postgres-demo/compose.yaml up --build
```

With Podman, use:

```console
podman-compose -f demo/postgres-demo/compose.yaml up --build
```

Open <http://127.0.0.1:8080>. The coordinator connects to both agents, verifies
their shared operation schema, and prepares the default sweep without sending
workload traffic. Press Start to run it. Completed phases stream into the
timeline and capacity charts; the knee appears only after terminal validation.

![Live three-cycle hysteresis progress](../../docs/images/dashboard-progress.png)

The agents only listen on the private Compose network. Both connect to the same
PostgreSQL instance, while every protocol session and operation batch still
originates from the coordinator. The agents remain available after a run, so
the browser can change the strategy, levels, timings, and workload before
starting another run.

Run artifacts are retained in the `postgres-demo-results` volume. Docker
Compose users can copy them out before teardown:

```console
docker compose -f demo/postgres-demo/compose.yaml cp web:/demo/results ./results
docker compose -f demo/postgres-demo/compose.yaml down
```

Use `podman-compose` in those commands when applicable. `down` preserves the
database and result volumes; add `--volumes` only when you intentionally want
to delete both.

## Why this workload has a knee

The adapter uses the synchronous Rust PostgreSQL client and a four-connection
pool. It exposes four concrete variants with a 32:8:8:2 ratio:

- `lookup(account=1)`: read the hot account using a normal MVCC `SELECT`
- `lookup(account=2)`: read another account
- `transfer(route=hot)`: update accounts 1 and 2 in one transaction
- `transfer(route=cold)`: update accounts 3 and 4 in one transaction

Transfers are 20% of offered traffic, and 80% of transfers use the hot route.
The first `UPDATE` takes PostgreSQL's row lock; the transaction deliberately
holds it for 10 ms before updating the destination and committing. That makes
the hot row's serialization ceiling easy to see in a short demo. It is real
database contention: requests execute real SQL through the native client, and
the latency includes pool wait, PostgreSQL execution, lock wait, and commit.

The controlled component predicts a knee near
`1000 ms / 10 ms / 0.16 = 625` offered operations per second. PostgreSQL and
host overhead move the fitted value somewhat; the E2E accepts a 450–850 ops/s
range rather than asserting an exact wall-clock result. After saturation,
goodput flattens while latency rises as work waits for a client connection and
the hot row lock.

## Local modes

The Rust commands need a PostgreSQL server. Start only the database container:

```console
docker compose -f demo/postgres-demo/compose.yaml up -d postgres
```

Then run the colocated fixed sweep:

```console
KNEEFINDER_POSTGRES_URL=postgres://kneefinder:kneefinder@127.0.0.1:55432/kneefinder \
  cargo run --release --manifest-path demo/postgres-demo/Cargo.toml -- e2e
```

Run adaptive baseline, discovery, refinement, fitting, and validation against
the same database:

```console
KNEEFINDER_POSTGRES_URL=postgres://kneefinder:kneefinder@127.0.0.1:55432/kneefinder \
  cargo run --release --manifest-path demo/postgres-demo/Cargo.toml -- e2e-adaptive
```

Exercise two persistent TCP agents sharing PostgreSQL:

```console
KNEEFINDER_POSTGRES_URL=postgres://kneefinder:kneefinder@127.0.0.1:55432/kneefinder \
  cargo run --release --manifest-path demo/postgres-demo/Cargo.toml -- e2e-tcp
```

Or run the multi-agent browser locally:

```console
KNEEFINDER_POSTGRES_URL=postgres://kneefinder:kneefinder@127.0.0.1:55432/kneefinder \
  cargo run --release --manifest-path demo/postgres-demo/Cargo.toml \
  --features web -- e2e-tcp-web
```

Open <http://127.0.0.1:8080>. Started runs write unique artifact directories
below `results/postgres-demo`.

## Normal CLI path

Build the adapter, then run the main kneefinder CLI against it:

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

The adapter also supports a persistent TCP listener:

```console
KNEEFINDER_POSTGRES_URL=postgres://kneefinder:kneefinder@127.0.0.1:55432/kneefinder \
  cargo run --release --manifest-path demo/postgres-demo/Cargo.toml \
  -- adapter-tcp 127.0.0.1:9000
```

Database credentials stay in `KNEEFINDER_POSTGRES_URL`, not in persisted run
configuration. An `initialize` message can tune only non-secret demo controls:

```json
{"type":"initialize","protocol_version":3,"run_id":1,"config":{"connections":4,"lock_hold_ms":10}}
```

The `e2e` commands are black-box fixtures around the production `RunExecutor`,
`AgentCohort`, `ColocatedAgent`, `TcpAgent`, and shared adapter session. They do
not maintain a divergent demo-only controller or protocol.

The default test suite keeps PostgreSQL-dependent tests ignored. Run them
explicitly while the database container is healthy:

```console
KNEEFINDER_POSTGRES_URL=postgres://kneefinder:kneefinder@127.0.0.1:55432/kneefinder \
  cargo test --offline --manifest-path demo/postgres-demo/Cargo.toml \
  --features web -- --ignored
```
