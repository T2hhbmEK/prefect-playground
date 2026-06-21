# The async coordinator: arun_deployment + await wait_for_flow_run + asyncio.gather (lesson 0014)

> **Note (reorg, see [[0017-async-native-reorg]]):** lessons 14↔15 were later swapped so
> the end-to-end demo is async-native. This record's session is now **lesson 0014** (the
> async coordinator, taught before running the fleet). The async coordinator described
> here was merged into `10_fleet.py` (the canonical end-to-end script) and the separate
> `11_fleet_async.py` was deleted. Findings below all still hold — but the narrative uses
> the **original** numbering (it was lesson 0015 when written).

Fifteenth lesson, seventh of the expanded scale arc ([[0008-mission-expanded-scale]]),
and the direct close of the loose end [[0015-run-the-fleet]] exposed: L14 *proved*
`wait_for_flow_run` is async-only in `3.7.5.dev4`, forcing the sync coordinator into a
hand-rolled poll loop. L15 makes the coordinator `async` and deletes that loop.

**The spine (one win):** rewrite L14's sync coordinator as one `async def` flow —
`arun_deployment` to fire, `await wait_for_flow_run` to gather, `asyncio.gather` to run
both phases concurrently — and be honest about what async does and doesn't buy. New
script `11_fleet_async.py` (deploys *alongside* the sync `process-archive/extract`,
reusing L14's unchanged `encode-segment/encode`).

**What was covered (lesson 0015 + extended `reference/topology-cheatsheet.html`):**

+ **The sync→async diff** — `run_deployment → await arun_deployment`; the 15-line sync
  poll loop → `await asyncio.gather(*(wait_for_flow_run(r.id) ...))`; `state.result() →
  await state.aresult()`. Two `asyncio.gather`s: one over the creates (phase 1), one
  over the waits (phase 2).
+ **What async actually buys (the first-principles honesty beat, [[challenge-me-first-principles]]):**
  the encodes *already* ran in parallel — that's `timeout=0` + the GCL, in BOTH versions.
  Async buys (a) the documented `wait_for_flow_run` API (async-only here), (b) **concurrent
  dispatch** — N create-calls at once, not back-to-back (invisible at 6 segments, the whole
  cost at 600), (c) deleting the poll loop, (d) cheap waiting (a coroutine per run, not a
  blocked thread). NOT a wall-clock win on small fan-outs. New reusable `.race` widget
  drives this home: a slider scales N; sync's dispatch bar stretches, async's stays pinned,
  the wait block (identical both ways) dominates until N is large.
+ **Async flow mechanics** — you don't call `asyncio.run` (Prefect runs the `async def`
  flow in its own loop); an async flow can only `await` coroutines (so `unpack` is async
  too); results via `await fr.state.aresult(...)`.

**Verified END-TO-END on `3.7.5.dev4` (live deployed runs, not just `inspect`):**
`arun_deployment` importable + a true coroutine, same signature as `run_deployment`
(incl. `timeout`, `idempotency_key`); `await asyncio.gather(*(wait_for_flow_run(id)…))`
works; `await fr.state.aresult(raise_on_failure=False)` returns the persisted
cross-process result. Deployed `process-archive-async/extract` ran `6 ok, 0 failed`,
same three waves of two (GCL gates identically regardless of sync/async coordinator).

**The real finding (why "run it" still earns its keep, mirrors L14):** the first live run
reported `5 ok, 1 failed: [3]` — but segment 3's encode had **completed** (worker logged
`done → …/seg-3.ivf`; run `Completed` on the server). Cause, from the source:
`wait_for_flow_run` here is **event-driven** (its `poll_interval` is deprecated/ignored) —
it returns on the first final-state *event*, then re-reads the run, and that re-read can
race the state commit, handing back a still-`RUNNING` `FlowRun` for a finished run.
Reproduced **~1 run in 5** (saw a `RUNNING` come back for a `Completed` run). **Fix
shipped:** re-read each run's authoritative state before classifying
(`await asyncio.gather(*(client.read_flow_run(fr.id) …))`) — one read settles it; **8/8**
reruns then clean. Also pinned: `fr.state.result.aio(...)` drops `self` (the bound-method
`.aio` quirk) — use `aresult`. Both gotchas added to the cheatsheet.

**Status:** lesson delivered with the new `.race` widget, 3 quizzes, 1 bridge recall;
topology cheatsheet extended with an "Async coordinator" section + 2 new gotchas. Script
`11_fleet_async.py` lints clean; deployment `process-archive-async/extract` live on the
server from the verification runs.

**Open threads / candidate next lessons (ask the learner):**

+ **Bounded re-dispatch loop** — retry only the failed segments N times with a *fresh*
  `idempotency_key` per attempt (L13/L14 record failures but never redo them). The
  natural next step now that fan-out + gather + async are all solid.
+ **Route by machine class** — a GPU pool / `work_queue_name` (untouched since L12).
+ **Cold review of L11–L15** — spacing is now overdue: **five** lessons of new material
  (L11 GCL, L12 fan-out, L13 gather, L14 run-it, L15 async) since the last review (L10).
  Trigger is elapsed time + volume, not lesson count. Strong candidate.
+ Standing: `max_workers`; (much later) the K8s graduation; Dask/Ray.
