# Prefect Resources

## Knowledge

- [Prefect 3 Documentation (official)](https://docs.prefect.io)
  The primary source. Current, versioned, matches the 3.x line we're on.
  Use for: anything authoritative — flows, tasks, states, deployments, schedules.
- [Prefect 3 — "Run flows locally" / Quickstart](https://docs.prefect.io/v3/get-started/quickstart)
  Shortest path from a script to an orchestrated run. Use for: first deployment.
- [Prefect concepts: States](https://docs.prefect.io/v3/concepts/states)
  Definitive list and meaning of run states (Completed, Failed, Crashed, …).
  Use for: understanding the UI and diagnosing failures.
- [Prefect how-to: Configure retries](https://docs.prefect.io/v3/how-to-guides/workflows/retries)
  Example-driven: fixed/list/backoff delays and `retry_condition_fn`.
  Use for: making tasks resilient to transient failure (lesson 0002).
- [Prefect how-to: Write & run workflows](https://docs.prefect.io/v3/how-to-guides/workflows/write-and-run)
  Covers `timeout_seconds` and the sync-task interruption caveat.
  Use for: capping hung runs.
- [Prefect how-to: Cache workflow steps](https://docs.prefect.io/v3/how-to-guides/workflows/cache-workflow-steps)
  Example-first: `cache_policy=INPUTS`, `cache_expiration`, `persist_result`.
  Use for: skipping redundant work (lesson 0003).
- [Prefect concept: Caching](https://docs.prefect.io/v3/concepts/caching)
  The full picture — cache policies (`DEFAULT = INPUTS + TASK_SOURCE + RUN_ID`),
  keys, and composing with `+`. Use for: the cross-run caching gotcha.
- [Prefect concept: Results & persistence](https://docs.prefect.io/v3/advanced/results)
  The `result_storage` / `persist_result` / `result_storage_key` model — where
  return values get stored. Use for: caching to remote/object storage (lesson 0004).
- [prefect-aws — S3Bucket block](https://docs.prefect.io/integrations/prefect-aws)
  The S3-compatible storage block. Works with Tencent COS via a custom
  `endpoint_url`. Use for: storing artifacts/results in COS.
  (Verify behavior against the *installed* package — `AwsClientParameters.endpoint_url`,
  `S3Bucket.upload_from_path` — since we're on dev builds.)
- [MinIO docs](https://min.io/docs/minio/container/index.html)
  Local S3-compatible object store (runs via `docker-compose.yml` in this repo).
  Use for: a local stand-in for COS/S3 while learning. Console at `:9001`.
- [Tencent Cloud COS docs](https://www.tencentcloud.com/document/product/436)
  COS-specifics: bucket name `<name>-<APPID>`, endpoint
  `https://cos.<region>.myqcloud.com`, SecretId/SecretKey. Use for: the COS side
  of the S3Bucket block config.
- [Prefect 3 — Per-worker task concurrency (example)](https://docs.prefect.io/v3/examples/per-worker-task-concurrency)
  The Global Concurrency Limit pattern: a limit named by `WORKER_ID`, acquired with
  `concurrency(name, occupy=N)`, to cap per-machine CPU saturation without
  oversubscribing. Create the limit first (`prefect gcl create` — not auto-created).
  Use for: lesson 0011 + the concurrency cheatsheet. (URL + behavior verified against
  the docs for `3.7.5`.)
- [Prefect 3 — Run deployments (how-to)](https://docs.prefect.io/v3/how-to-guides/deployments/run-deployments)
  `run_deployment` parameters, `timeout` (0 = fire-and-forget), and `idempotency_key`
  for safe coordinator retries. Use for: fan-out (lesson 0012) + gather (lesson 0013).
- [Prefect 3 — flow-runs Python API](https://docs.prefect.io/v3/api-ref/python/prefect-deployments-flow_runs)
  Full `run_deployment` signature + `as_subflow`. Pair with `prefect.flow_runs.
  wait_for_flow_run(id)` (poll a run to a final state) and `State.result(
  raise_on_failure=False)` to gather a distributed batch and survive partial failure.
  Use for: collecting encode results across the fleet (lesson 0013). (Signatures
  verified against the installed `3.7.5` via `inspect`.)
- [Prefect API & CLI reference](https://docs.prefect.io/v3/api-ref/cli)
  Use for: `prefect server start`, `prefect flow-runs ls`, deployment commands.
- [Prefect GitHub repo](https://github.com/PrefectHQ/prefect)
  Source of truth for actual behavior (we're on `3.7.5`; earlier lessons were verified
  against the `3.7.5.dev4` pre-release). Use for: checking what a feature actually does
  when docs lag the release.

## Wisdom (Communities)

- [Prefect Community Slack](https://prefect.io/slack)
  High-signal, maintainers active. Use for: "is this the idiomatic way?" questions
  and behavior that the docs don't cover.
- [Prefect GitHub Discussions / Issues](https://github.com/PrefectHQ/prefect/discussions)
  Use for: confirming bugs vs. misuse.

## Gaps

- No resource yet vetted specifically for *local-only* production patterns
  (running a long-lived `serve()` process on a laptop). Revisit at deployment stage.
