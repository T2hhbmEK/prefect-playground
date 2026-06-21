# Failure alerts: on_failure hooks vs server-side Automations (lesson 0007)

Seventh session. First lesson _beyond_ the original four mission goals
([[MISSION.md]]) — the learner chose to extend into proactive ops. The proactive
complement to [[0006-diagnose-failures]]: L6 was "go look at a failure," L7 is "the
failure comes to you." Builds on the Failed-vs-Crashed framing from
[[0002-resilience-retries]] / L6.

**What was covered (lesson 0007 + `reference/alerts-cheatsheet.html`):**

+ **State-change hooks:** `@flow(on_failure=[fn])`; the fn receives
  `(flow, flow_run, state)`. Per-flow, runs in-process. Other hooks: on_completion,
  on_crashed, on_cancellation, on_running.
+ **Notification blocks:** `.notify(body, subject)`. Production = `SlackWebhook`
  (prefect-slack). Built-ins in `prefect.blocks.notifications`: SlackWebhook,
  MicrosoftTeamsWebhook, DiscordWebhook, MattermostWebhook, OpsgenieWebhook,
  CustomWebhookNotificationBlock + Apprise-backed.
+ **Local-only verifiable demo:** a `CustomWebhookNotificationBlock` (needs
  `name=...` at construction AND `allow_private_urls=True` for localhost) POSTing to
  a tiny `alert_listener.py` http.server catcher. Two-terminal pattern like serve().
  Swap to real Slack = one line (different block, same `.notify()`).
+ **The core insight / desirable difficulty:** `on_failure` runs _inside the flow's
  process_, so it catches Failed but NOT a hard Crash (killed process can't run its
  own hook). For crash coverage + one-rule-for-all-deployments you need a
  **server-side Automation** (event trigger flow-run.Failed/Crashed → action). Ties
  directly to L6's Failed-vs-Crashed and L5's "no process, no runs." Quiz Q2/Q3.

**Verified end-to-end against the live stack:** `07_alerts.py` (reuses L6's
validate-on-bad-data failure) → flow Failed → logs show
`Running hook 'alert_on_failure'` → `alert_listener.py` printed the formatted alert
(flow name + `state.message` with the ValueError). One bug found & fixed during
build: `CustomWebhookNotificationBlock` requires `name` at construction (pydantic),
not just at `.save()`.

**Mission status:** all four original goals done (L1–L6); L7 is the first
operability extension. Automations were _taught_ but the hands-on demo is the
hook path (server-side Automation is UI-clicky / needs a channel — left as a
guided ask-me / community step).

**Implication for next session:** remaining optional ops/polish — default
`parameters=` on deployments (run same flow per-source); building the Automation in
the UI step-by-step; richer Slack formatting; alert-on-Nth-consecutive-failure.
Otherwise the course is at a natural complete state — candidate for a
spaced-retrieval **review** lesson across L1–L7 to convert fluency → storage
strength. Worth asking the learner.

**Open threads (ask-me, not drilled):** UI Automation walkthrough; on_completion /
on_crashed hooks; throttling alerts (Nth failure); real prefect-slack setup
(flagged to community).
