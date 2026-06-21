# Starting point: brand-new, productionizing local data pipelines

Established at the first session. The learner is **brand new** to Prefect but not to
Python: they can write and run a `@flow` with `@task`, use `.map()` to fan out over a
list, and call `.result()` — evidenced by the existing `01_getting_started.py`. They
have **not** deployed, scheduled, retried, or used the UI.

The mission ([[MISSION.md]]) is to turn ordinary Python scripts into reliable,
**observable** data pipelines, running **locally** for now (Prefect Cloud, work
pools, and Airflow/Dagster comparisons are explicitly out of scope).

**Implication for teaching:** start from observability, not abstract concepts. The
zone of proximal development is "you can run a flow → now *see* it run." Lesson 0001
builds on their own existing script rather than a toy. Resilience (retries), caching,
deployments, and schedules follow, in that order (see course arc in [[NOTES.md]]).
Defer all distributed/remote-infra material until the local loop is solid.
