# Teaching Notes

Working notes on how to teach this user. Update as preferences surface.

## Preferences

- _(none recorded yet)_

## Context

- Learner works inside an existing repo, `prefect-playground`, with numbered
  standalone example scripts (`01_getting_started.py`, …). Each is self-contained
  with its own `@flow` + `if __name__ == "__main__"`. Lessons can build on / add to
  these scripts.
- Environment: uv-managed, `prefect==3.7.5.dev4` (a dev build — verify behavior
  against the GitHub source if docs disagree). Run things with `uv run`.
- Telemetry disabled via `[tool.prefect.server] analytics_enabled = false`.
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
11. (next) Let **spacing** work: have the learner redo L10 _cold in a few days_ and
    report which L# tags tripped them up → target those. New content only if
    reopened: combining task runner + work pool; `max_workers`; async /
    `ConcurrentTaskRunner`; deployment `parameters=`; UI Automation walkthrough;
    multiple workers / pool concurrency; (much later) Dask/Ray.
