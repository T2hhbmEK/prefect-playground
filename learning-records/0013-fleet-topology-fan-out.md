# Fleet topology: pools by job type + run_deployment fan-out (lesson 0012)

Twelfth lesson, fourth of the expanded scale arc ([[0008-mission-expanded-scale]]),
and the second grounded in the real **AV1 encode fleet** ([[0012-concurrency-limits-saturation]]).
Answers the "structure pools/queues/workers/deployments" half of the original
architecture conversation; L11 had answered the "don't oversubscribe" half.

**The spine (one win):** turn a one-machine batch into a fleet-wide one — split work
by job type into separate pools, and dispatch each encode as its **own** flow run with
`run_deployment` so every machine pitches in, while light jobs never starve behind
heavy ones.

**What was covered (lesson 0012 + `reference/topology-cheatsheet.html`):**

+ **Three axes of spreading work** (the framing): task runner = tasks within one run,
  one machine (L9); worker `--limit`/GCL = which machine + how much (L11); **work
  pool/queue by job type = which *kind* of work runs where** (new).
+ **Split pools by job type** — `extract` (I/O, generous limit) vs `encode` (CPU,
  GCL-capped). Rationale: **head-of-line blocking** in a shared pool. Two workers per
  box, one per pool. Queues-in-one-pool noted as the lighter alternative; separate
  pools preferred (cleaner + future GPU pool).
+ **The crux — two meanings of "subflow":** a direct call `encode(seg)` runs inline on
  the caller's machine; `run_deployment("encode/encode", timeout=0)` creates a
  **separate flow run** on the `encode` pool → any worker, any machine. `.map` (L9)
  also stays on one machine — corrects the natural over-reach from L9.
+ New reusable **`.fleet`** widget in `lesson.css` (toggle subflow-on-one-box vs
  fan-out-across-3-machines; reuses the L11 slot/heterogeneity visual).

**Verified API (docs, for `3.7.5.dev4`):** `from prefect.deployments import
run_deployment`; `run_deployment(name="flow/deployment", parameters={…}, timeout=0)`
returns FlowRun metadata immediately (`timeout=0`), blocks by default, or waits N s.
**`as_subflow=True` is lineage-only — it does NOT co-locate execution** (the run still
executes on the pool). Also: `idempotency_key`, `work_queue_name`, `arun_deployment`
(async). Source: [Run deployments](https://docs.prefect.io/v3/how-to-guides/deployments/run-deployments)
and the [flow-runs API](https://docs.prefect.io/v3/api-ref/python/prefect-deployments-flow_runs).

**Scope boundary:** stayed local/Prefect-native. K8s named only as the horizon
(when static limits strand cores). Async fan-out (`arun_deployment` + `asyncio.gather`)
mentioned but not taught — async is still a standing thread.

**Status:** lesson delivered with the `.fleet` widget, 3 quizzes, 1 bridge recall.
Not verified end-to-end on the learner's stack (no runnable multi-pool demo built —
needs ≥2 pools + workers running; offered, not built).

**Open threads / candidate next lessons (ask the learner):**

+ **Gather the fan-out** — wait on / collect encode results, handle partial failure,
  `idempotency_key` for safe coordinator retries. Natural follow-on to L12.
+ **Route by machine class** — a GPU pool (GPU-session-bound, not core-bound); queues
  + `work_queue_name`.
+ A **runnable end-to-end demo** on the Docker stack (extract + encode pools, two
  workers, a fan-out coordinator).
+ Still pending: the overdue **cold redo of L10** (spacing — now 4 lessons stale).
+ Standing: `max_workers`; async / `ConcurrentTaskRunner`; (much later) the K8s
  graduation; Dask/Ray.
