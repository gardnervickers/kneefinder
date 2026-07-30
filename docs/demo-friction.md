# Multi-agent dashboard friction log

This log records usability problems found while exercising the containerized
queue demo as an operator would. The demo now runs through the production
generic executor, including its interruptible Stop path.

| Friction | Resolution |
| --- | --- |
| A browser-started second run stayed in `Starting` because the demo executed only its initial hard-coded sweep. | The engine now owns every execution and can run each newly prepared cohort. |
| Stop moved a run to `Stopping` but no runtime acknowledged it. | The generic executor interrupts response collection, retains partial results, sends `CancelPhase`, and force-closes an unresponsive session after a deadline before recording `AdapterStopped`. |
| The initial sweep ignored edited phase timings, load levels, cycles, strategy, repetitions, and workload variants. | The generic executor builds its plan from the prepared run's `RunConfig` and schedules its concrete weighted variants. |
| Starting or preparing another run while one owned the agents could fail later as an opaque connection timeout. | The engine rejects concurrent Start and agent preparation with a stable `run_already_active` conflict, and the browser disables Start while another run is active. |
| Normal run completion could terminate remote agents, preventing reruns. | Normal completion and Stop disconnect sessions. Only the explicit protocol `Shutdown` command terminates an agent. |
| A forced TCP-session abort surfaced as a write error and terminated the demo agent process. | TCP agent loops treat connection-scoped failures as recoverable, log them, and return to accepting coordinator sessions. |
| Instructions assumed Docker even when a Docker-compatible Podman setup was available. | The README now gives both Docker Compose and Podman Compose commands, plus a container-free colocated smoke test. |
| Opening the demo immediately launched the default workload before the operator could review it. | Startup now prepares and discovers the initial run only; workload traffic begins exclusively after an explicit Start command. |
| Separate Apply and Query buttons made saving, discovery, and readiness look like unrelated manual steps. | Form edits are saved automatically, endpoint changes trigger automatic agent preparation, connection failures are shown inline, and Start is enabled only after the agents are ready. |

The required regression scenario is: verify the prepared default run remains
idle, explicitly start and complete it, create a run with changed settings and
complete it, stop a long run promptly, then create and complete another run
against the same two agent processes.
