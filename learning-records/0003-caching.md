# Caching: cache_policy, cross-run gotcha, persistence (lesson 0003)

Third session. Follows [[0002-resilience-retries]] with the framing the prior record
set up: retries *re-run* failed work; caching *avoids re-running* succeeded work.
They compose.

**What was covered (lesson 0003 + `reference/caching-cheatsheet.html`):**

- `cache_policy=` is the Prefect 3 idiom — taught this, *not* `cache_key_fn`, which
  is now the escape hatch. (The course-arc note in [[0002-resilience-retries]] said
  "`cache_key_fn`" — superseded; 3.x centers on policies.)
- Built-in policies from `prefect.cache_policies`: `INPUTS`, `TASK_SOURCE`,
  `FLOW_PARAMETERS`, `NO_CACHE`, and `DEFAULT = INPUTS + TASK_SOURCE + RUN_ID`.
  Combine with `+`.
- `cache_expiration=timedelta(...)`.
- **The conceptual core / desirable-difficulty point:** the default policy includes
  `RUN_ID`, so the default cache only hits *within one flow run*. Cross-run reuse
  (the actual mission win — skip re-fetching between scheduled runs) requires a
  policy *without* `RUN_ID`, e.g. `INPUTS`. This is the off-by-one-equivalent gotcha
  for this lesson; it's quiz Q2 and a `.why` callout. Watch for the learner
  defaulting to `DEFAULT` and being puzzled that "caching doesn't work" tomorrow.
- New reusable asset: `.pill.cached` added to `assets/lesson.css`; glossary gained a
  `Cached` run-state row and a "Cache key / policy" term.

**Verified:** `03_caching.py` runs against the local server. Run 1 → middle call
`Cached(type=COMPLETED)`, only 2 of 3 fetches actually sleep. Run 2 (separate
process) → all 3 `Cached`, whole run sub-second — proving `INPUTS` persists across
runs. Grounded in their repo's customer-fetch theme, not a toy.

**Implication for next session:** ZPD moves to the back half of the mission —
**lesson 4: script → deployment** (`flow.serve()`), then **lesson 5: schedules**
(cron / interval). The glossary already stubs Deployment/Schedule. This is the
pivot from "make one run good" to "make runs happen on their own."

**Open thread:** `cache_key_fn` custom keys, on-disk result storage location
(`~/.prefect/storage`), and *why caching needs result persistence* were flagged as
ask-me topics, not drilled. Revisit only if asked.
