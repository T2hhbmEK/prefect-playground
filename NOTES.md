# Teaching Notes

Working notes on how to teach this user. Update as preferences surface.

## Preferences

- **Challenge me — reason from first principles.** The learner explicitly wants
  design questions stress-tested: push back on wrong framing, don't just agree, and
  lead with tradeoffs and the materially-better alternative rather than the happy
  path. (Surfaced in the AV1 architecture conversation that seeded lesson 0011.)

## Context

- Learner works inside an existing repo, `prefect-playground`, with numbered
  standalone example scripts (`01_getting_started.py`, …). Each is self-contained
  with its own `@flow` + `if __name__ == "__main__"`. Lessons can build on / add to
  these scripts.
- Environment: uv-managed, `prefect==3.7.5.dev4` (a dev build — verify behavior
  against the GitHub source if docs disagree). Run things with `uv run`.
- Telemetry disabled via `PREFECT_SERVER_ANALYTICS_ENABLED=false` in `docker-compose.yml`
  (server-side env, set when the stack was Dockerized — not in `pyproject.toml`).
- **Whole local stack is Dockerized** (`docker-compose.yml`, `docker compose up -d --build`):
  Prefect server (API/UI :4200) + its Postgres DB + MinIO. The server is built from
  `Dockerfile.server`, pinned to `prefect==3.7.5.dev4` (no published dev image), so
  the server API version (`0.8.4`) matches the host client exactly. Flows run on the
  _host_ via `uv` and talk to the Dockerized server on :4200 (profile already sets
  `PREFECT_API_URL`). No more `prefect server start` by hand.
- **MinIO** (S3-compatible object store): API :9000, console :9001, creds
  `minioadmin`/`minioadmin`, bucket `prefect-artifacts`. Saved Prefect block:
  `minio-artifacts` (auto-recreated by `04_results_storage.py`'s `get_bucket()` if
  missing). Stands in for Tencent COS so storage lessons stay local. `prefect-aws`
  comes via `prefect[aws]`.
- Switching the server to Dockerized Postgres = fresh metadata DB; earlier run
  history/blocks/deployments from the old ad-hoc server container don't carry over
  (recreatable by re-running the lesson scripts). Cached _results_ survive because
  they live in MinIO, not the server DB.
- JS tooling via `bun`; Python lint/format via `uv run ruff`. Conventional Commits
  enforced by commitlint; pre-commit runs lint.
- **Real-world driver (the actual goal behind the scale arc):** a _media-processing_
  workload on a self-hosted Prefect server — two job types: (a) light I/O
  unpack/extract and (b) CPU-heavy **AV1 encodes** (ffmpeg / SVT-AV1 / aomenc) that
  run as external **subprocesses**, each multi-threaded (3–4 threads). Fleet of
  **heterogeneous VMs** (some many-core, some few), growing 1 → many, possibly a GPU
  box later. Goal: saturate CPU without oversubscribing. Grounds lessons from 0011
  on. (Candidate to fold into `MISSION.md` as the named scenario — confirm first.)

## Records ↔ lessons (why the numbers diverge)

`learning-records/` capture **lessons _and_ decisions** (ADR-style), so they are
**not** 1:1 with `lessons/`. One record is a decision, not a lesson, which offsets
every later record by +1. Each lesson-derived record's title names its lesson
(`(lesson 000X)`); the only records without a lesson are `0001-starting-point` (the
baseline) and `0008-mission-expanded-scale` (the 2026-06-21 scope change).

- `0001`–`0007` ↔ **L1–L7** (aligned)
- `0008-mission-expanded-scale` ↔ **—** (decision record, no lesson — the +1 starts here)
- `0009` / `0010` / `0011` / `0012` / `0013` ↔ **L8** / **L9** / **L10** / **L11** / **L12**
- `0014` ↔ **L13** (gather)
- `0015` ↔ **L15** and `0016` ↔ **L14** — **crossed** by the async-native reorg
  ([[0017-async-native-reorg]]): the lessons swapped (record 0015 = run-the-fleet = now
  L15; record 0016 = async coordinator = now L14), but the records did not.
- `0017-async-native-reorg` ↔ **—** (decision record, no lesson — the L14↔L15 swap)

Don't renumber to "align" them: records cross-link by slug via wikilinks
(`[[0008-mission-expanded-scale]]` alone is referenced by 0009, 0010, and 0012), so
renumbering would break the graph. The 0015/0016 ↔ L15/L14 crossing is intentional.

## Course arc (tentative — toward the mission)

1. **See your flow run** — local server + UI + run states. ← done (lesson 0001)
2. **When things fail** — retries, backoff, retry_condition_fn, timeouts. ← done (lesson 0002)
3. **Don't redo work** — caching & results (`cache_policy`). ← done (lesson 0003)
4. **Results & object storage** — cache big artifacts to local MinIO
   (`result_storage`, `S3Bucket`); Tencent COS = credentials swap. ← done
   (lesson 0004) — pulled forward by learner's mp4/COS question.
5. **On a schedule** — `flow.serve()`, cron / interval, deployments. ← done
   (lesson 0005). Covers old items 5 + 6 (serve does both).
6. **Diagnose a failed run** — read run history (UI + `prefect flow-run
   ls/inspect/logs/retry`); Failed vs Crashed. ← done (lesson 0006). **All four
   mission success criteria now covered.**
7. **Get alerted when it breaks** — `on_failure` hooks + notification blocks
   (local webhook catcher); server-side Automations for crash coverage. ← done
   (lesson 0007). First lesson beyond the original 4 goals.
**Mission expanded 2026-06-21** (learner's request): work pools / workers /
distributed execution now IN scope. See [[0008-mission-expanded-scale]].

8. **Work pools & workers** — process pool + `from_source().deploy()` + a worker;
   `deploy()` ≠ run; serve() vs worker. ← done (lesson 0008).
9. **Distribute the work** — task runners: `ThreadPoolTaskRunner` (I/O) vs
   `ProcessPoolTaskRunner` (CPU), the GIL, "processes aren't free". ← done
   (lesson 0009). Dask/Ray explicitly OUT of scope (learner's call).
10. **Review & retrieve** — interleaved spaced-retrieval across L1–L9; new
    `.recall` flip-card component. ← done (lesson 0010). No new material.
11. **Count runs, not cores** — concurrency limits for per-machine CPU saturation:
    pool/queue (fleet-global) vs worker `--limit` vs **per-worker GCL + `occupy`**.
    Grounded in the learner's real **AV1 encode fleet**; extends L9 (subprocess CPU
    work → the task runner is the wrong lever). New reusable `.sim` widget; K8s /
    cgroups named as horizon only, kept out of scope. ← done (lesson 0011).
12. **Fan out across the fleet** — topology: split `extract` (I/O) vs `encode` (CPU)
    into separate pools (head-of-line blocking), two workers per box, and
    `run_deployment(..., timeout=0)` to dispatch each encode as its own distributed
    flow run (vs a direct subflow / `.map`, which stay on one machine). New `.fleet`
    widget; verified `run_deployment` / `as_subflow` behaviour. ← done (lesson 0012).
13. **Gather the fan-out** — the second half of L12: fire all encodes (`timeout=0`),
    then `wait_for_flow_run` each; partial failure via `state.result(raise_on_failure=
    False)`; safe coordinator retry via `idempotency_key` from _stable_ inputs;
    results-vs-states across machines (ties back to L4 — return values need persisted
    shared storage). New reusable `.batch` widget; verified `wait_for_flow_run` /
    `State` predicates against the build. ← done (lesson 0013).
**Async-native reorg 2026-06-21** (learner: "swap L14 and L15 so end2end is async
ready"): the two lessons below were swapped so the end-to-end demo is async-native, and
`10_fleet.py` was rewritten async (the sync coordinator + `11_fleet_async.py` dropped).
See [[0017-async-native-reorg]]. Items renumbered to the post-swap order:

14. **The async coordinator** — one `async def` coordinator: `arun_deployment` +
    `await wait_for_flow_run` + `asyncio.gather` (two phases, both concurrent), no
    hand-rolled poll loop. Comes _before_ running the fleet; builds on L13's gather
    (`wait_for_flow_run` is async-only → the coordinator must be async). First-principles
    honesty beat: async does NOT speed the encodes (that was `timeout=0` + GCL all along)
    — it buys the documented API, concurrent _dispatch_ (matters at high fan-out), and
    less code. New reusable `.race` widget. **The race:** `wait_for_flow_run` is
    event-driven (`poll_interval` ignored) and its re-read can lag the commit → a finished
    run comes back `RUNNING` (~1 in 5) and looks failed; fix = re-read authoritative state
    before classifying (8/8 clean). Also pinned `fr.state.result.aio` drops `self` → use
    `aresult`. ← done (lesson 0014). Record: [[0016-async-coordinator]].
15. **Run the fleet** — the runnable end-to-end demo, async-native, executed on the Docker
    stack. `10_fleet.py`: `encode-segment/encode` (GCL + `occupy` around a `sleep`
    subprocess, `persist_result`) + the **async** `process-archive/extract` coordinator
    (fan out `arun_deployment(timeout=0, idempotency_key=…)` via `asyncio.gather`, gather,
    re-read settle). Verified LIVE (async): happy path `6 ok` in 3 waves of 2 (GCL holds),
    partial failure `5 ok, 1 failed:[3]`, idempotent rerun = 0 new encodes. `.runboard`
    widget. ← done (lesson 0015). Record: [[0015-run-the-fleet]].
16. (next) Candidates, in rough priority: a **cold review of L11–L15** — spacing is now
    overdue (FIVE lessons of new material since the last review at L10; trigger is elapsed
    time + volume, not lesson count); a **bounded re-dispatch loop** (retry only the failed
    segments N times with a fresh `idempotency_key` per attempt — the natural next step now
    that fan-out + gather + async are solid); or a **GPU pool** routed by machine class
    (`work_queue_name`, untouched since L12). Standing: `max_workers`; (much later) the K8s
    graduation; Dask/Ray.
