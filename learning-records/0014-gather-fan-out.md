# Gather the fan-out: wait, partial failure, idempotency_key (lesson 0013)

Thirteenth lesson, fifth of the expanded scale arc ([[0008-mission-expanded-scale]]),
and the direct second half of [[0013-fleet-topology-fan-out]]: L12 *scattered* encodes
fire-and-forget (`timeout=0`); L13 *picks them back up*. Closes the "gather the
fan-out" thread that L12, NOTES, and the topology cheatsheet ("Fire all, gather later")
all explicitly deferred.

**The spine (one win):** turn a fire-and-forget batch into one a coordinator can come
back to — fire all encodes at once, gather them as a batch, keep the good results when
some fail, and make a coordinator rerun safe so it never double-encodes.

**What was covered (lesson 0013 + extended `reference/topology-cheatsheet.html`):**

+ **Fire all, then gather** — `wait_for_flow_run(id)` (from `prefect.flow_runs`) polls a
  run to a final state and returns the finished `FlowRun`. The ordering rule: create
  *every* run with `timeout=0` first, *then* wait — blocking as you create makes the
  waits serial and throws away the L12 fleet parallelism.
+ **Partial failure** — `state.result(raise_on_failure=False)` returns the exception as
  a value instead of re-raising, so one bad segment doesn't sink the coordinator. Filter
  `is_completed()` / `is_failed()`, re-dispatch only the failures.
+ **Safe retry** — `idempotency_key` from *stable* inputs (`f"{archive}:{s}"`): a
  re-created run with a seen key returns the existing run, so a crashed-and-retried
  coordinator reuses finished encodes instead of doubling CPU. Random/UUID keys defeat it
  (every retry looks new) — the key insight learners get wrong.
+ **Results vs states across machines (ties back to [[0004-results-cos]]):** a return
  value only comes back if the deployment *persists results to shared storage* (MinIO/S3)
  — encodes ran on other machines. Run *state* always lives in the server DB, so checking
  *whether* each finished works regardless. Guidance: persist the output path, not bytes.
+ New reusable **`.batch`** widget in `lesson.css`: 6 segments, #3/#5 doomed; toggle
  "block & raise" (first failure sinks the batch, rest lost) vs "gather & isolate", plus
  a **Retry coordinator** button showing `idempotency_key` reusing completed runs.

**Verified API (installed `3.7.5.dev4`, via `inspect`):**
`run_deployment(..., timeout, idempotency_key, ...) -> FlowRun`;
`wait_for_flow_run(flow_run_id, timeout=10800, poll_interval=None, ...) -> FlowRun` in
`prefect.flow_runs`; `State.result(raise_on_failure=True, retry_result_failure=True)`;
`State.is_completed / is_failed / is_crashed / is_final`; `FlowRun` carries `state`,
`parameters`, `idempotency_key`. `run_deployment` docstring confirms `timeout=0` →
immediate, `None` → poll indefinitely; `idempotency_key` "prevent[s] creating multiple
flow runs."

**Scope boundary:** stayed sync + Prefect-native. **Async gather** (`arun_deployment` +
`asyncio.gather`, so waits truly overlap rather than poll in sequence) named in the
`.ask` but NOT taught — async is still the standing thread. A bounded re-dispatch *loop*
(retry failed segments N times) was gestured at, not built.

**Status:** lesson delivered with the `.batch` widget, 3 quizzes, 1 bridge recall;
topology cheatsheet extended with a "Gather the fan-out" section + knobs table. **Not
verified end-to-end** — no runnable coordinator built on the Docker stack (still the
biggest outstanding gap in the scale arc; offered three lessons running).

**Open threads / candidate next lessons (ask the learner):**

+ **The runnable end-to-end demo** — extract + encode pools, two workers, this gather
  coordinator on the Docker stack. Now offered across L12 *and* L13; strongest candidate
  — the arc has built a lot of theory with nothing run.
+ **Async fan-out/gather** — `arun_deployment` + `asyncio.gather`; the overdue async
  thread, now directly motivated (serial polling is the visible cost).
+ **Route by machine class** — a GPU pool / `work_queue_name` (untouched since L12).
+ Still pending: a **cold redo of L10** (spacing: 3 lessons of new material since —
  L11–L13 — trigger is elapsed calendar time, not lesson count).
+ Standing: `max_workers`; (much later) the K8s graduation; Dask/Ray.
