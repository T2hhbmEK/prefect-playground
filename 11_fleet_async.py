"""Lesson 15 — the async coordinator.

L14 ran the fleet for real and surfaced the catch: `wait_for_flow_run` is a
*coroutine* in our `3.7.5.dev4` build, so the **sync** coordinator in
`10_fleet.py` couldn't call it — it dropped to a hand-rolled poll loop over the
sync client. This is that same coordinator, made `async`. Two changes matter:

    run_deployment(..., timeout=0)         ->  await arun_deployment(..., timeout=0)
    hand-rolled sync poll loop             ->  await wait_for_flow_run(id)

and `asyncio.gather` fans both phases out concurrently. The encode side
(`encode-segment/encode`, the GCL, the pools, the workers) is UNCHANGED from
L14 — only the coordinator changed — so this script reuses that deployment by
name and deploys *just* the async coordinator, alongside the sync one.

Be honest about what async buys (lesson 0015 §2): for six segments the wall
clock barely moves — the encodes already overlapped via `timeout=0`. Async (a)
lets you call `wait_for_flow_run`, the documented gather API, at all; (b) fans
the *dispatch* (one create per segment) out concurrently, which matters when a
fan-out is hundreds of segments, not six; (c) deletes the manual poll loop.

    uv run python 11_fleet_async.py     # registers the async coordinator
    # then (lesson 0015): trigger process-archive-async/extract
"""

import asyncio
import sys
from pathlib import Path

from prefect.client.orchestration import get_client
from prefect.deployments import arun_deployment
from prefect.flow_runs import wait_for_flow_run

from prefect import flow, task

HERE = Path(__file__).parent


@task
async def unpack(archive: str, n_segments: int) -> list[int]:
    """Light I/O: split the archive into segments (same job as 10_fleet.py).

    Async here so the coordinator can `await` it without bridging an event loop.
    """
    print(f"unpacking {archive} → {n_segments} segments")
    return list(range(n_segments))


@flow(name="process-archive-async", log_prints=True)
async def process_archive_async(archive: str = "demo.tar", n_segments: int = 6) -> dict:
    """Async sibling of 10_fleet.py's coordinator: fan out, then gather.

    Fire EVERY encode with `timeout=0` (so the fleet runs them in parallel,
    gated by each box's GCL), then await them all. `asyncio.gather` overlaps the
    create-calls in phase 1 and the waits in phase 2; the hand-rolled sync poll
    loop from L14 is gone, replaced by `await wait_for_flow_run`.
    """
    segments = await unpack(archive, n_segments)

    # Phase 1 — fire all, concurrently. Each `arun_deployment(timeout=0)` only
    # CREATES a run and returns; gather sends every create at once (the dispatch
    # win at scale). `idempotency_key` from stable inputs → safe rerun (L13).
    runs = await asyncio.gather(
        *(
            arun_deployment(
                "encode-segment/encode",
                parameters={"archive": archive, "segment": s},
                timeout=0,  # return the instant it's created — don't block here
                idempotency_key=f"{archive}:{s}",
            )
            for s in segments
        )
    )
    print(f"dispatched {len(runs)} encode runs; gathering…")

    # Phase 2 — gather, concurrently. `wait_for_flow_run` is async-only in this
    # build (L14's finding); under `await` + gather the waits overlap instead of
    # the serial sync poll loop `10_fleet.py` had to hand-roll.
    finished = await asyncio.gather(*(wait_for_flow_run(r.id) for r in runs))

    # The catch running it surfaced (lesson 0015 §4): `wait_for_flow_run` here is
    # EVENT-driven (its `poll_interval` is deprecated/ignored) — it returns on the
    # first final-state *event*, then re-reads the run. That re-read can race the
    # state commit and hand back a still-RUNNING state for a run that actually
    # finished, so classifying on it can mark a completed encode "failed" (seen
    # live, ~1 run in 5). Re-read each run's authoritative state first — one read
    # settles it. Gathered, so it stays concurrent.
    async with get_client() as client:
        finished = await asyncio.gather(
            *(client.read_flow_run(fr.id) for fr in finished)
        )

    done, failed = [], []
    for fr in finished:
        if fr.state.is_completed():
            # Async cross-process result fetch: `aresult`, NOT `result.aio` — the
            # bound-method `.aio` form drops `self` in 3.7.5.dev4. Works because
            # the encode persists its result (L4/L13).
            done.append(await fr.state.aresult(raise_on_failure=False))
        else:
            failed.append(fr.parameters["segment"])  # record, don't re-raise

    print(f"{len(done)} ok, {len(failed)} failed: {failed}")
    return {"archive": archive, "ok": len(done), "failed": failed}


if __name__ == "__main__":
    # Reuses L14's encode deployment (`encode-segment/encode`) — deploys ONLY the
    # async coordinator, to the same `extract` pool. The distinct flow name means
    # it lives alongside the sync `process-archive/extract` for side-by-side runs.
    process_archive_async.from_source(
        source=str(HERE), entrypoint="11_fleet_async.py:process_archive_async"
    ).deploy(name="extract", work_pool_name="extract")

    print(
        "\nDeployed:\n"
        "  process-archive-async/extract  → extract pool"
        " (reuses encode-segment/encode)\n\n"
        "Next (see lesson 0015): with the extract + encode workers running,\n"
        "  uv run prefect deployment run process-archive-async/extract",
        file=sys.stderr,
    )
