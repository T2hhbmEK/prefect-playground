# Interleaved spaced-retrieval review across L1–L9 (lesson 0010)

Tenth lesson — the first **review** lesson, requested by the learner at the natural
consolidation point after nine content lessons. No new material; pure
storage-strength work.

**Pedagogy applied (deliberately):**

+ **Active recall, not recognition:** new reusable `.recall` flip-card component
  (added to `assets/lesson.css`) — prompt → think/answer aloud → click *Reveal*.
  Recall-then-check builds durable memory far better than picking an MCQ option.
+ **Interleaving:** cards are shuffled across topics on purpose (L2→L1·L6→L3→L4→
  L5→L8→L9→L6→L7→L9→L4→L8·L9), and Part 2 "which tool?" scenarios force
  cross-lesson discrimination (e.g. timeout vs retry vs runner). Interleaving is the
  one technique reserved for practice, and this is the place for it.
+ **Desirable difficulty** named explicitly in the intro — blanking is a signal, not
  failure; each card carries an <span class="tag">L#</span> tag pointing back to its
  source lesson for targeted revisit.
+ **Spacing:** the lesson tells the learner to redo it *cold in a few days* — the
  spacing only pays off if they actually return.

**Coverage:** 12 recall cards + 5 interleaved scenario MCQs spanning all of L1–L9
(states/Failed-vs-Crashed, retries N+1 / retry_condition_fn / timeout, INPUTS +
RUN_ID cross-run gotcha, cache-pointer/store-payload + MinIO⇄COS swap, serve()
dual role, deploy()≠run/worker, on_failure-can't-catch-Crashed/Automations,
flow-run diagnosis chain, task-runner-only-on-submit + GIL + thread/process,
work-pool-vs-task-runner two axes).

**New reusable asset:** `.recall` card styles in the shared stylesheet — available
to every future review lesson; reveal JS is a 3-line inline handler.

**Not "verified" like code lessons** — this is interactive HTML (recall + quiz
widgets), nothing to run. Visually consistent with all prior lessons via
`lesson.css`.

**Implication for next session:** the course is at a complete, consolidated state
(L1–L7 reliability arc + L8–L9 scale arc + L10 review). Best next move is to let the
**spacing** do its work — have the learner redo L10 cold in a few days and report
which <span class="tag">L#</span> tags tripped them up, then target those. New
content (combining task runner + work pool, `max_workers`, async/Concurrent runner,
deployment `parameters=`, revisiting Dask/Ray) only if the learner reopens it.

**Open thread:** consider a `ScheduleWakeup`/reminder cadence for the cold re-test —
spaced repetition works best on a real schedule, but the learner hasn't asked for
automation; leave dormant unless raised.
