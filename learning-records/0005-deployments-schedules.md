# Deployments & schedules: serve() runs the flow itself (lesson 0005)

Fifth session. The pivot the whole mission ([[MISSION.md]]) was building toward:
from "make one run good" to "make runs happen on their own." Builds on the
resilience, caching, and storage stack ([[0002-resilience-retries]],
[[0003-caching]], [[0004-results-cos]]) — those made a flow worth scheduling; this
schedules it.

**What was covered (lesson 0005 + `reference/deployments-cheatsheet.html`):**

+ A **deployment** = flow + how/when to run it. `flow.serve(name=...)` is the
  local-first path: one call that (1) registers the deployment and (2) starts a
  long-lived process that creates and executes scheduled runs. No work pools /
  workers / containers — those stay out of scope per the mission.
+ **cron vs interval:** `cron="0 8 * * *"` (clock-based) vs `interval=30` (every N
  seconds / a `timedelta`). The committed demo uses `interval=30` for instant
  gratification; the realistic `cron` is shown as a comment + in the lesson.
+ `@flow(log_prints=True)` so `print()` lands in run logs (used to make the
  scheduled runs legible in the UI).
+ Triggering off-schedule: `prefect deployment run '<flow>/<name>'` (the "try it").
+ **The gotcha (core wisdom):** `serve()` *is* the scheduler — kill the process and
  the schedule goes dark. Real use needs a process manager (launchd/tmux/systemd);
  the logout-surviving alternative is `deploy()` + work pools (deferred). Quiz Q3.

**Verified against the live local server (:4200):** ran `05_deployment.py` via
`serve()` in the background; `prefect deployment ls` showed
`scheduled-pipeline/scheduled-pipeline`, and a scheduled run (`diligent-duck`)
fired on its own ~30s later — `fetch_rows` Completed, logged `[nightly] processed
42 rows`, flow Completed. Then stopped the process. (Docs fetched fresh via ctx7;
`.serve()` confirmed as the no-infra path, `.deploy()` as the work-pool path.)

**Mission status:** all four success criteria are now covered — see a run (L1),
resilience (L2), don't-redo-work (L3/L4 incl. local MinIO object storage), and
**deployment on a schedule (L5)**. The arc's back half is essentially done.

**Implication for next session:** ZPD moves to polish / operability —
**lesson 6: parameters + failure notifications** (the "visible when it breaks at
3am" half of the mission), and reading run history to diagnose past failures.
Glossary now promotes Deployment / Schedule / `serve()` out of the "coming later"
section.

**Open threads (ask-me, not drilled):** serving multiple deployments from one
process; schedule timezones; `parameters=` defaults; keeping `serve()` alive with
launchd on macOS; when to graduate to `deploy()` + work pools.
