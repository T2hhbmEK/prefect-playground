# Run the fleet: the end-to-end demo, executed for real (lesson 0014)

Fourteenth lesson, sixth of the expanded scale arc ([[0008-mission-expanded-scale]]),
and the payoff the last three records all flagged as the biggest gap: L11–L13 built a
lot of theory with **nothing actually run**. L14 stands the whole fan-out fleet up on
the Docker stack and executes it — the first lesson verified end-to-end, not just by
API `inspect`.

**The spine (one win):** one runnable script (`10_fleet.py`) + a handful of commands
stand up two pools, two workers, and a per-machine GCL; fire one archive and watch six
encodes gate themselves to two-at-a-time even though all six fired at once. L11 (GCL +
`occupy`), L12 (split pools + `run_deployment` fan-out), L13 (gather + partial failure +
`idempotency_key`) made real, on one host standing in for the VM fleet.

**What was built (`10_fleet.py`, runnable):**

+ `encode-segment/encode` flow (CPU job) — `with concurrency(f"encode:{WORKER_ID}",
  occupy=2)` around a `subprocess.run(["sleep","4"])` standing in for ffmpeg; `persist_result=True`
  so the coordinator can read the return from another process; returns the output **path**,
  not bytes (L4/L13). Optional `FAIL_SEGMENT` env to force one segment to raise.
+ `process-archive/extract` flow (coordinator) — `unpack` task, then fan out
  `run_deployment("encode-segment/encode", timeout=0, idempotency_key=f"{archive}:{s}")`
  per segment, then gather.
+ `__main__` deploys BOTH via `from_source().deploy()` (process pools, no image build).

**Verified END-TO-END on `3.7.5.dev4` (live runs, not just `inspect`):**

+ **Happy path** — `6 ok, 0 failed`; encode log showed **three clean waves of two**
  (GCL `limit 4 / occupy 2` = `⌊4/2⌋ = 2`), proving the gate holds despite all six
  firing at once with `timeout=0`. Worker `--limit 6` lets all six be *picked up*; the
  GCL, not the worker limit, caps concurrency (the parked-process gotcha, real).
+ **Partial failure** — `FAIL_SEGMENT=3` on a fresh archive → `5 ok, 1 failed: [3]`;
  coordinator did **not** crash (`state.result(raise_on_failure=False)`).
+ **Idempotency** — rerun same archive → `6 ok, 0 failed` with **zero new encodes**
  (counted: encode-starts before == after). `idempotency_key` reused existing runs.

**The real finding (the reason "run it" matters):** L13 (and the topology cheatsheet)
taught `wait_for_flow_run(r.id)` in a **sync** coordinator. It does **not** work — in
`3.7.5.dev4` `wait_for_flow_run` is a **coroutine function** with no sync wrapper, so a
sync flow gets `'coroutine' object has no attribute 'state'`. Confirmed
`inspect.iscoroutinefunction(wait_for_flow_run) is True`, while `run_deployment` is
sync-compatible (`.aio` present, not a coroutine fn), and `State.result()` /
`State.is_final()` / `get_client(sync_client=True).read_flow_run()` are all sync-callable.
**Fix shipped:** the sync coordinator gathers by polling state via the sync client; lesson
§5 + the topology cheatsheet gotchas now carry the correction. The async path
(`await wait_for_flow_run`) is deferred to the next lesson.

**New reusable `.runboard` widget** in `lesson.css`: a GCL gauge (2 busy slots) + 6
segment chips flowing queued → encoding (2 at a time) → done/failed, plus a **Rerun
(same archive)** button showing `idempotency_key` reuse and a segment-3-fails toggle.
Mirrors the real wave order observed (`[4,1,2,0,3,5]`).

**Status:** lesson delivered with the `.runboard` widget, 3 quizzes, 1 bridge recall;
topology cheatsheet corrected. Script lints clean (`ruff`). Pools `extract`/`encode`,
GCL `encode:local-1` (limit 4), and both deployments are **live on the local server**
from the verification runs.

**Open threads / candidate next lessons (ask the learner):**

+ **The async coordinator** — `arun_deployment` + `await wait_for_flow_run` +
  `asyncio.gather`; now doubly motivated (L13 noted serial polling; L14 *proved*
  `wait_for_flow_run` is async-only). Strongest next candidate — it closes the sync/async
  loose end this lesson exposed.
+ **Bounded re-dispatch loop** — retry only failed segments N times (L14 records failures
  but doesn't redo them; needs a fresh `idempotency_key` per attempt).
+ **Route by machine class** — a GPU pool / `work_queue_name` (untouched since L12).
+ Still pending: a **cold redo of L10** (spacing: L11–L14 now four lessons of new
  material since the last review).
+ Standing: `max_workers`; (much later) the K8s graduation; Dask/Ray.
