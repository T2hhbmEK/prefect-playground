# Review the fleet: cold spaced retrieval of L11–L15 (lesson 0016)

Sixteenth lesson, and the second pure-review lesson after [[0011-review-retrieve]]
(which covered L1–L9). No new material — interleaved spaced retrieval over the whole
expanded-scale fan-out arc: [[0012-concurrency-limits-saturation]] (L11),
[[0013-fleet-topology-fan-out]] (L12), [[0014-gather-fan-out]] (L13),
[[0016-async-coordinator]] (L14), [[0015-run-the-fleet]] (L15).

**Why now:** spacing was overdue — FIVE lessons of dense distributed-execution material
since the last review at L10, flagged as the top L16 candidate in every record since L13.
Trigger is elapsed time + volume, not lesson count.

**Design (mirrors [[0011-review-retrieve]] / L10):**

+ **Part 1 — 12 recall flip-cards** (`.recall`), order **shuffled across L11–L15** so the
  learner can't coast on lesson context (interleaving = desirable difficulty). Each
  card carries an `L#` tag pointing back to its source lesson for targeted re-read.
+ **Part 2 — 5 scenario quizzes** (`.quiz`, the shared widget): which-lever decisions
  mixing all five lessons. Options length-balanced to leak no formatting cue.
+ Reused existing components only — **no new asset added** (`.recall`, `.quiz`, `.note`,
  `.source-box`, `.ask`, `.pill`, `.tag`). Source-box links the five lessons + the
  concurrency/topology cheatsheets + glossary.

**The high-value gotchas the cards deliberately re-drill** (the cross-machine subtleties
most likely to have decayed):

+ Count runs, not cores — task runner is the wrong lever for subprocess CPU (L11).
+ Fleet-global pool limit (trap) vs per-worker `--limit` (blunt) vs GCL + `occupy`
  (per-machine, right) (L11/L12); GCL must exist first.
+ Two meanings of "subflow": inline call vs `run_deployment(timeout=0)` escaping the host;
  `as_subflow=True` is lineage-only (L12).
+ Fire all (`timeout=0`) then gather, else serial; `state.result(raise_on_failure=False)`
  for partial failure; stable `idempotency_key` not UUID (L13).
+ Async buys API + concurrent dispatch + less code, NOT encode wall-clock; the
  `wait_for_flow_run` event-driven race → re-read authoritative state (L14).
+ Worker `--limit` picks up, GCL caps; ⌊limit/occupy⌋ = waves (L15).
+ Results cross machines only via persisted storage; state always in the server DB (L13/L4).

**Workspace bookkeeping:** added `0016-review-the-fleet` to `NO_CITATION_OK` in
`tests/test_docs_integrity.py` (review lessons are exempt from the primary-source rule,
as L10 already was). Full suite green (285 passed, 2 review lessons skipped on citation).

**Status:** lesson delivered, no new material, no new asset, no script. Spacing debt on
the scale arc cleared.

**Open threads / candidate next lessons (ask the learner):**

+ **Bounded re-dispatch loop** — retry only the failed segments N times with a *fresh*
  `idempotency_key` per attempt (L13/L14/L15 record failures but never redo them). The
  natural next build now that fan-out + gather + async are all solid, and review is done.
+ **Route by machine class** — a GPU pool / `work_queue_name` (untouched since L12).
+ Standing: `max_workers`; (much later) the K8s graduation; Dask/Ray.
+ Re-run THIS review cold in a few days (spacing the spacing).
