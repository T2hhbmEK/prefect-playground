# Concurrency limits & per-machine CPU saturation (lesson 0011)

Eleventh lesson, third of the expanded scale arc ([[0008-mission-expanded-scale]]).
**Triggered by the learner's real external project** — an architecture question about
running a *media-processing / AV1-encode* workload on a self-hosted Prefect server
across heterogeneous VMs. First lesson grounded in the learner's actual workload
rather than the generic playground scripts. The learner explicitly asked to be
*challenged from first principles* (reason from the problem, push back on wrong
framing) — see [[NOTES]] preference.

**The bridge from L9 (the teaching hook):** L9 left the rule "CPU-bound → reach for
`ProcessPoolTaskRunner`." This lesson *extends and corrects* it for the subprocess
case: an AV1 encode shells out to ffmpeg/SVT-AV1, so the heavy CPU work and its 3–4
threads live in a **child process Prefect can't see**. There's no Python compute to
parallelise → the task runner is the wrong lever. The replacement is a **concurrency
limit**. The one-liner: *count runs, not cores.*

**What was covered (lesson 0011 + `reference/concurrency-cheatsheet.html`):**

+ **Prefect counts runs**, never cores or subprocess threads. You do the
  `cores ÷ threads-per-encode` math and hand Prefect the number.
+ **Pin the encoder first** (`-threads`/`--lp`/`--threads`) — encoders auto-detect
  all cores; without pinning, no limit helps. The real prerequisite.
+ **Three scopes:** pool/queue limit = *fleet-global* (the trap — `set-concurrency-limit`
  is shared by all workers, met earlier on the work-pools cheatsheet); worker
  `--limit N` = per-worker but *whole flow runs* (blunt, serialises); **GCL named per
  worker + `occupy`** = per-machine, weighted (the right tool).
+ **The formula:** `limit = machine's core budget`, `occupy = each encode's thread
  cost`. Heterogeneous fleet handled by *the same flow code* with a different limit
  number per box. New reusable `.sim` saturation widget added to `lesson.css`
  (cores-as-budget cells, launch → blocks at `⌊cores/threads⌋`).

**Verified API (docs, for the `3.7.5.dev4` build):** `from prefect.concurrency.sync
import concurrency`; `with concurrency(f"encode:{worker_id}", occupy=N):`;
`WORKER_ID=… prefect worker start --pool … --limit M`; **`prefect gcl create <name>
--limit N` — the limit must exist first, `concurrency()` does not auto-create it.**
Source: [Per-worker task concurrency](https://docs.prefect.io/v3/examples/per-worker-task-concurrency).

**Scope boundary (held the line):** Kubernetes (per-pod CPU `requests`/bin-packing)
and OS `cgroups`/`taskset` were named only as the *horizon* — the day static
per-machine limits strand cores. Kept OUT of the lesson body; K8s/remote is still
[[0008-mission-expanded-scale]] out-of-scope. Lesson stayed local + Prefect-native.

**Mission note:** the AV1 encode fleet is the concrete driver of the abstract
"scale-out / decouple what-from-where" arc. **Confirmed by the learner (2026-06-21)
and folded into `MISSION.md`** as the named *Driving scenario*, plus two new
expanded-arc success criteria (cap per-machine CPU saturation ✓ L11; route by job
type + fan out via `run_deployment`, next). K8s reframed in *Out of scope* as the
named-but-deferred graduation. Also recorded in [[NOTES]] as context.

**Status:** lesson delivered with interactive sim + 3 quizzes + 1 bridge recall card.
Not yet verified end-to-end on the learner's stack (no runnable script written yet).

**Open threads / candidate next lessons (ask the learner):**

+ **Topology** — separate `extract` (I/O, high limit) vs `encode` (CPU, tight) pools
  /queues; two workers per box; routing deployments by job type; `run_deployment` to
  fan encodes across the fleet. Most natural next rung.
+ A runnable `10_concurrency_limits.py` on the Docker stack (offered, not built).
+ Multiple workers / pool concurrency interplay; the blocked-run-holds-a-slot nuance.
+ Still pending: the **cold spaced redo of L10** (overdue — fluency vs storage).
+ Much later, if scope opens: the **Kubernetes graduation** for resource-aware packing.
