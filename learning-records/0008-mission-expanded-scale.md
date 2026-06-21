# Mission expanded: work pools, workers, distributed execution (2026-06-21)

**Mission change**, made at the learner's explicit request after finishing the
original arc (L1–L7 — all four original success criteria + alerting). Work pools,
workers, and distributed execution were previously listed **out of scope** in
[[MISSION.md]]; they are now **in scope**. `MISSION.md` updated accordingly (new
"Scope expanded" note, expanded success criteria, revised out-of-scope list).

**Why this is a clean next step, not a detour:** L5 taught `flow.serve()` and
flagged its ceiling — "no process, no runs", serve() *is* the scheduler and dies
with its terminal. L7 reinforced it (in-process hooks can't catch a crashed
process). The natural resolution to both is the **worker** model: decouple *what*
runs (the deployment) from *where* it runs (a work pool that workers pull from).
So the expansion picks up exactly the thread L5/L7 left dangling.

**Still out of scope:** Prefect Cloud; remote/managed infra (k8s, ECS,
cloud-hosted workers). Local containers are allowed if a lesson needs them. Bias
toward locally-runnable pool/executor types first (Process work pool, local
workers, local Dask/Ray).

**Planned expanded arc:**

+ **L8 — Work pools & workers.** Why serve() isn't enough; create a **Process**
  work pool, `flow.deploy()` to it, start a local worker, watch it pull & execute a
  scheduled run in its own process. The serve() → worker graduation.
+ **L9 — Distributing the work.** Task runners: the default `ThreadPoolTaskRunner`
  (concurrency within a run) → `DaskTaskRunner` / `RayTaskRunner` for true
  parallelism. (Separate axis from work pools: *where the flow runs* vs *how its
  tasks spread out*.) Verify the locally-runnable path; flag integration installs.
+ (later) multiple workers / pool concurrency limits; deploying from remote source
  storage; the bridge toward real production infra.

**ZPD note:** the learner is no longer brand-new — they have deployments,
scheduling, caching, storage, diagnosis, alerting. L8 can assume all of that and
move at a faster clip than L1.

**Status:** mission updated + this record written. L8 build follows (verify
work-pool + worker end-to-end on the dev build *before* writing lesson claims).
