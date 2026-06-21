# Diagnosing failed runs from history (lesson 0006)

Sixth session. Completes the **fourth and final mission success criterion**
([[MISSION.md]]): "read run history after the fact to diagnose why a past run
failed." Closes the loop opened in [[0001-starting-point]] (states) and
[[0002-resilience-retries]] (Failed vs Crashed) — now applied to post-mortem
debugging of a scheduled run ([[0005-deployments-schedules]]).

**What was covered (lesson 0006 + `reference/diagnosis-cheatsheet.html`):**

+ **State first:** Failed (code raised → read the traceback) vs Crashed (infra died
  → look at the machine, traceback may be absent). Decides *where you even look*.
+ **UI path:** Flow Runs → filter Failed → the run's task-run timeline shows which
  task broke and how far the pipeline got; the failed task's Logs tab has the
  traceback; Parameters/inputs let you reproduce.
+ **CLI path** (`prefect flow-run`): `ls --state Failed` (find) → `inspect <id>`
  (state + message) → `logs <id>` (traceback) → `retry <id>` (re-run after a fix).
  Also `watch`, `cancel`, `delete`.
+ Framed as "a run failed at 3am, diagnose it at 9am from history alone" — the
  exact pain the mission names (cron + print() failing silently).

**Verified against the live server (:4200):** `06_diagnose.py` (a `validate` task
raises `ValueError('row 2 has no amount')` on bad input — a *permanent* failure, by
design, contrasting L2's transient ones) runs to a Failed flow run. Confirmed the
full diagnosis chain on the real run: `flow-run ls --state Failed` surfaces only
`nightly-report`; `inspect` shows `StateType.FAILED` + the exception message;
`logs` shows the traceback and that `fetch_batch` Completed while `validate` Failed.
CLI surface confirmed on the dev build (ls/inspect/logs/retry/watch/cancel/delete).

**Mission status: all four success criteria now covered.** See-a-run (L1),
resilience (L2), don't-redo-work + remote storage (L3/L4, local MinIO),
deployment-on-a-schedule (L5), diagnose-from-history (L6). The core arc is
complete.

**Implication for next session:** ZPD is now *polish / proactive ops*, beyond the
original four goals — **lesson 7: get alerted when it breaks** (Automations +
notification blocks; the proactive complement to this lesson's reactive
diagnosis), and **default `parameters=`** on deployments. Confirm with the learner
whether to continue into this "operability" extension or consider the mission
done — possible point to revisit/expand the mission (per the teach guidance on
mission changes).

**Open threads (ask-me, not drilled):** inspecting a specific failed *task* run
(vs the flow run); filtering history by tag / time window; why a run lands Crashed
vs Failed in practice.

**Housekeeping note:** learner consolidated infra into a single `docker-compose.yml`
(Postgres + Prefect server + MinIO) — the local stack is now `docker compose up -d`,
no separate `prefect server start`. Reflected in [[NOTES.md]].
