# Mission: Prefect — production-grade local data pipelines

## Why

Turn ordinary Python data scripts into **reliable, observable pipelines** with
Prefect 3. The goal: a job that fetches → processes → writes data should run on a
schedule, retry on transient failure, and be *visible* when it breaks — instead of
being a fragile `cron` + `print()` setup that fails silently at 3am. Running locally
for now, learning toward eventual production.

**Scope expanded (2026-06-21):** after completing the original local-reliability
arc (L1–L7), the mission now extends into **how and where flow runs execute** —
work pools, workers, and distributed execution. The goal shifts from "make one
machine's pipelines reliable" toward "decouple the *what* from the *where*", the
foundation for scaling out and eventual production deployment.

**Driving scenario (the real target, 2026-06-21):** the concrete workload behind
"scaling out" is a **media-processing fleet** — two job types: light I/O
*unpack/extract*, and CPU-heavy **AV1 encodes** (ffmpeg / SVT-AV1 / aomenc) that run
as external *subprocesses*, each multi-threaded (3–4 threads). It runs across
**heterogeneous VMs** (some many-core, some few), growing from one machine to many,
maybe a GPU box later. The question that now grounds every scale-arc lesson: *keep
each machine busy without oversubscribing its CPU — even though the heavy work hides
inside a subprocess Prefect can't see.*

## Success looks like

Original arc (done, L1–L7):

- Start a local Prefect server and watch a flow run appear in the UI, with
  per-task states and logs.
- Make a flow resilient: retries, timeouts, and caching, so a transient blip
  doesn't sink the whole run.
- Turn a plain script into a **deployment** that runs on a schedule (cron / interval).
- Read run history after the fact to diagnose *why* a past run failed, and get
  *alerted* automatically when it does.

Expanded arc (in progress):

- Understand why `serve()` isn't enough at scale, and stand up a **work pool** with
  a **worker** that pulls and executes scheduled runs (`flow.deploy()`).
- Run the worker locally and watch it execute deployed runs in separate processes.
- **Distribute execution within a run**: use task runners to run a flow's tasks
  concurrently (`ThreadPoolTaskRunner`) or in true parallel across cores
  (`ProcessPoolTaskRunner`), and know which fits I/O- vs CPU-bound work.
- **Cap per-machine CPU saturation** when the heavy work is an invisible subprocess:
  pool/queue limits (fleet-global) vs a **per-worker Global Concurrency Limit** with
  `occupy` — `limit = cores`, `occupy = threads/encode` — so a fleet of mismatched
  VMs never oversubscribes. ✓ L11.
- **Route work by type, fan out across the fleet**: separate `extract` (I/O) and
  `encode` (CPU) pools/queues, a worker per type per box, and dispatch encodes as
  independent runs with `run_deployment`. (next)

## Constraints

- No longer brand new: comfortable through deployments, scheduling, caching,
  object storage, diagnosis, and alerting (L1–L7).
- Local-first (uv-managed, `prefect==3.7.5.dev4`, Dockerized server + MinIO). Prefer
  work-pool / worker / executor types that run **locally** (Process pool, local
  workers, local Dask/Ray) before any remote infra.
- Short lessons; learning spread over multiple sessions.

## Out of scope (for now)

- Prefect Cloud (the hosted control plane).
- Comparisons against Airflow / Dagster / Luigi.
- Remote/managed infra: Kubernetes, ECS, cloud-hosted Docker workers. (Containers
  *locally* are fair game if a lesson needs them; remote clusters are not — yet.)
  K8s is the *named* eventual graduation for the encode fleet — resource-aware
  bin-packing by CPU request — but deferred until the local concurrency patterns
  (per-worker limits, pool topology) are solid.
- **Dask / Ray task runners** (`prefect-dask`, `prefect-ray`) — deferred for now.
  Focus is the built-in thread/process runners first; revisit cluster executors
  once those are solid.
