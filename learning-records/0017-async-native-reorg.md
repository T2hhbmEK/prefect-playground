# Decision: make the end-to-end fleet demo async-native (swap L14 ↔ L15)

A reorganization decision (no new lesson), like [[0008-mission-expanded-scale]]. After
[[0016-async-coordinator]] added the async coordinator as a lesson that came *after* the
sync end-to-end run ([[0015-run-the-fleet]]), the course taught the awkward sync
workaround as the canonical end-to-end demo and only fixed it afterward. The learner's
call: the runnable end-to-end demo should be **async-native from the start** ("swap L14
and L15 so end2end is async ready").

**What changed:**

- **Lessons swapped.** L14 is now **"The async coordinator"** (the building-block
  concepts: `arun_deployment` + `await wait_for_flow_run` + `asyncio.gather`, the honest
  "what async buys", the event-driven `wait_for_flow_run` race + re-read fix). L15 is now
  **"Run the fleet"** — the culminating end-to-end run, async-native. Lesson files keep
  their slugs; only the numeric prefixes swapped
  (`0014-the-async-coordinator.html`, `0015-run-the-fleet.html`).
- **One async script.** `10_fleet.py` was rewritten to be async-native: it defines
  `encode_segment` + the **async** `process-archive` coordinator (with the re-read race
  fix) and deploys both. `11_fleet_async.py` was **deleted** — its coordinator merged in.
  The sync coordinator + hand-rolled sync poll loop are dropped from the runnable code
  (kept only as history here and as the "naive sync" contrast in the cheatsheet).
- **Deployment naming.** The canonical deployment is `process-archive/extract` (now
  async); the transitional `process-archive-async/extract` deployment was removed.
- **Cross-refs updated:** L13's forward links, the topology cheatsheet (async section now
  cited L14, nav reordered, "naive sync" relabel), and the NOTES course arc + record↔lesson
  mapping.

**Verified end-to-end (async `10_fleet.py`, `3.7.5.dev4`):** happy path `6 ok, 0 failed`
in three waves of two (GCL holds); partial failure `FAIL_SEGMENT=3` → `5 ok, 1 failed:
[3]` (coordinator doesn't crash); idempotent rerun → `6 ok` with **0 new encodes**.

**Implications for future sessions:** the scale arc now ends on an async, production-shaped
fleet. The standing record↔lesson crossing this creates (record 0015 ↔ L15, record 0016 ↔
L14) is intentional — do **not** renumber records to "align"; they cross-link by slug.
Next candidates unchanged: a bounded re-dispatch loop, a GPU pool by machine class, or a
cold review of L11–L15. Pairs with [[challenge-me-first-principles]] (the learner pushed
for the better structure rather than accepting the sync-then-fix arc).
