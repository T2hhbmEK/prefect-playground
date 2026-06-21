# Task runners: thread vs process, the I/O-vs-CPU decision (lesson 0009)

Ninth lesson, second of the expanded scale arc ([[0008-mission-expanded-scale]]).
The *other* axis from [[0009-work-pools-workers]]: work pools spread whole flow
runs; task runners spread the tasks *within* one run. Triggered by the learner's
explicit conceptual question ("difference between distributed task runners and
distributed work pools") — answered with a two-axes diagram, then taught here.

**Scope decision (learner's call):** Dask/Ray cluster runners are **out of scope
for now** — focus on the built-in `ThreadPoolTaskRunner` / `ProcessPoolTaskRunner`
first. `MISSION.md` updated to match (Dask/Ray moved to out-of-scope).

**What was covered (lesson 0009 + `reference/task-runners-cheatsheet.html`):**

+ Task runner set via `@flow(task_runner=…)`; **only affects `.submit()`/`.map()`ed
  tasks** — plain calls stay sequential. The #1 gotcha (quiz Q1).
+ `ThreadPoolTaskRunner` (default) = concurrency in one process → wins for
  **I/O-bound** work (waiting releases the GIL, so waits overlap).
+ `ProcessPoolTaskRunner` = true multi-core parallelism → wins for **CPU-bound**
  work (separate interpreters dodge the GIL).
+ **The GIL** explained in one line: only one thread runs Python bytecode at a time.
+ **The nuance that makes it honest:** processes aren't free — spawn + pickle
  overhead means ProcessPool can be *slower* for light CPU tasks. Match the runner
  to the work; measure.

**Verified end-to-end (12-core Mac, `09_task_runners.py`, 4 mapped tasks):**

+ I/O (4×`sleep(0.5)`): ThreadPool **1.1s** vs ~2s sequential.
+ CPU (4×40M-loop): ThreadPool 4.2s (GIL serializes) → ProcessPool **2.4s** (parallel).
+ CPU (4×tiny-loop, probe): ProcessPool *slower* than threads (spawn/pickle overhead).

The cheap-CPU probe (ProcessPool *slower*) was deliberately surfaced as the
cautionary nuance, not hidden — first measurement gave the "wrong" textbook answer
and that became the teaching point.

**Status:** the current scope (original arc L1–L7 + scale arc L8–L9) is fully
covered and all examples verified locally. Natural stopping/consolidation point.

**Implication for next session:** likely a **spaced-retrieval / interleaved review**
across L1–L9 (fluency → storage strength) — now overdue with 9 lessons banked.
Otherwise remaining optional threads: `max_workers` tuning; async tasks &
`ConcurrentTaskRunner`; combining a task runner (how a run spreads) with a work
pool (where it runs); and eventually revisiting Dask/Ray if the learner reopens
that scope. Ask the learner.

**Open threads (ask-me, not drilled):** async/await task behavior; per-runner
`max_workers` sizing; mixing task runner + work pool in one deployment.
