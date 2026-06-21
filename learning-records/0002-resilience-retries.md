# Resilience: retries, backoff, conditions, timeouts (lesson 0002)

Second session. Built on the observability foundation from [[0001-starting-point]]:
the learner can now read run states in the UI, so retries were taught *through*
that lens — "watch the attempts in the UI" rather than abstract config.

**What was covered (lesson 0002 + `reference/resilience-cheatsheet.html`):**

- `retries=N` → `N + 1` total attempts. This was the one quiz the learner is most
  likely to slip on (off-by-one), so it's the first check-yourself question.
- `retry_delay_seconds=` as a scalar, a list, or `exponential_backoff(backoff_factor=)`
  from `prefect.tasks`.
- `retry_condition_fn` — the transient-vs-permanent distinction. This is the
  *conceptual* core: retries are only for failures that might heal (network blip,
  503), not for 401/404/bad-input. Ties back to the Failed-vs-Crashed framing from
  lesson 1.
- `timeout_seconds` for hangs, with the documented caveat that sync blocking tasks
  under `ThreadPoolTaskRunner` may not be interrupted (deferred to a task-runners lesson).

**Verified:** `02_resilience.py` runs and visibly recovers (failed twice → Completed)
against the local server. The demo is grounded in their own repo, not a toy.

**Implication for next session:** ZPD is now lesson 3 — **caching & results**
(`cache_key_fn`, `cache_expiration`, result persistence). Natural follow-on: retries
re-run work; caching *avoids* re-running work that already succeeded. After that,
deployments + schedules (the back half of the mission in [[MISSION.md]]).

**Open thread to watch:** flow-level retries and `retry_condition_fn`'s `(task,
task_run, state)` signature were flagged as ask-me topics in the lesson but not
drilled — revisit if the learner asks, otherwise leave dormant.
