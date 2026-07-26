# Multi-agent dashboard friction log

This log records usability problems found while exercising the containerized
queue demo as an operator would. It is intentionally scoped to the demo; the
production generic executor remains tracked by GitHub issue #2.

| Friction | Resolution |
| --- | --- |
| A browser-started second run stayed in `Starting` because the demo executed only its initial hard-coded sweep. | The dashboard coordinator now consumes every `Starting` event and executes the prepared cohort repeatedly. |
| Stop moved a run to `Stopping` but no runtime acknowledged it. | The executor checks for Stop between one-second scheduling buckets, disconnects the run session, and records `AdapterStopped`. |
| The initial sweep ignored edited phase timings, load levels, cycles, strategy, repetitions, and workload variants. | The demo executor builds its plan from the prepared run's `RunConfig` and schedules its concrete weighted variants. |
| Starting or querying another run while one owned the agents could fail later as an opaque connection timeout. | The engine rejects concurrent Start and agent preparation with a stable `run_already_active` conflict, and the browser disables those actions while another run is active. |
| Normal run completion could terminate remote agents, preventing reruns. | Normal completion and Stop disconnect sessions. Only the explicit protocol `Shutdown` command terminates an agent. |
| Instructions assumed Docker even when a Docker-compatible Podman setup was available. | The README now gives both Docker Compose and Podman Compose commands, plus a container-free colocated smoke test. |

The required regression scenario is: complete the default two-agent run,
create a run with changed settings and complete it, stop a long run promptly,
then create and complete another run against the same two agent processes.
