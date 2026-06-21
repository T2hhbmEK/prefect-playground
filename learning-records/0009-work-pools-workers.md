# Work pools & workers: decoupling what-runs from where-it-runs (lesson 0008)

Eighth session, first lesson of the **expanded mission** ([[0008-mission-expanded-scale]],
[[MISSION.md]]). Resolves the ceiling that [[0005-deployments-schedules]] and
[[0007-failure-alerts]] both hit: `serve()` is one process that dies with its
terminal. The worker model splits it apart.

**What was covered (lesson 0008 + `reference/work-pools-cheatsheet.html`):**

+ **Three pieces, one direction:** deployment (the *what*) → work pool (the *queue*)
  → worker (the *where*, the process that executes). `serve()` was all three fused.
+ **Process work pool** = runs each flow run as a local subprocess; no Docker/k8s.
  `prefect work-pool create local-process --type process`.
+ **`flow.from_source(source=dir, entrypoint="file.py:flow").deploy(work_pool_name=…)`** —
  registers and **exits** (vs serve() which stays attached & runs). `from_source` is
  required because the worker is a separate process and must locate the code.
+ **`prefect worker start --pool local-process`** — the long-lived executor; runs
  several for throughput.
+ **The core insight / desirable difficulty: `deploy()` ≠ run.** A triggered run
  sits Scheduled forever until a worker polls the pool. The #1 beginner trap — it's
  quiz Q1 and a `.why` callout.
+ serve() vs worker decision table: terminal-survival, scale, path to remote infra.

**Verified end-to-end on the live stack:** created `local-process` pool; ran
`08_workers.py` → deployment `worker-pipeline/worker-pipeline` registered to the
pool and the script exited; started a worker in the background; triggered a run
(`furry-pigeon`); the worker's log showed the deployment pull steps
(set_working_directory), then executed the flow and logged `[from-a-worker]
processed 42 rows — executed by a worker, not by me`, Completed. Docs fetched fresh
via ctx7 (process pool + from_source confirmed as the no-image local path).

**ZPD note:** learner moved at a faster clip than L1 — the lesson assumes L1–L7
(deployments, schedules, states). Good sign the expansion is well-pitched.

**Implication for next session:** **L9 — distributing the work (task runners):** the
*other* axis. Work pools = where the *flow* runs; task runners = how a flow's
*tasks* spread out. Default `ThreadPoolTaskRunner` (concurrency) →
`DaskTaskRunner`/`RayTaskRunner` (true parallelism). Must verify the locally-runnable
path and flag integration installs (`prefect-dask`/`prefect-ray`) — those may not be
in the current env, so confirm before claiming "verified."

**Open threads (ask-me, not drilled):** multiple workers sharing a pool; pool
concurrency limits; default `job_variables`; what a Docker work pool changes;
how workers make L7 alerting crash-resilient (server schedules independently).
