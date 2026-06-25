"""Lesson 15 — the runnable end-to-end fleet demo (async-native).

Four lessons of theory — L11 per-worker GCL + `occupy`, L12 split pools + fan-out,
L13 gather, L14 the async coordinator — executed on the Docker stack. Two
deployments, two pools, two workers — all on one host, standing in for the
heterogeneous VM fleet.

    encode-segment/encode    → the `encode` pool (CPU work; GCL-capped per worker)
    process-archive/extract  → the `extract` pool (light I/O; fans out + gathers)

The coordinator is an `async def` flow (L14): `wait_for_flow_run` is async-only in
our `3.7.5` build, so a coordinator that gathers a fan-out the documented way
*must* be async. `asyncio.gather` fans both phases — the creates and the waits —
out concurrently.

Run `uv run python 10_fleet.py` to register BOTH deployments, then follow the
lesson (0015) for the pool/worker/GCL setup and how to trigger a run.
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

from prefect.client.orchestration import get_client
from prefect.concurrency.sync import concurrency
from prefect.deployments import arun_deployment
from prefect.flow_runs import wait_for_flow_run

from prefect import flow, task

HERE = Path(__file__).parent

# How many threads one "encode" claims. In the real fleet this is the ffmpeg/
# SVT-AV1 thread count you PIN (L11) — Prefect can't see it, so you tell the GCL.
THREADS_PER_ENCODE = 2


# --- The CPU job: one segment, one distributed flow run ----------------------
# `persist_result=True` is what lets the coordinator read this run's return value
# from ANOTHER process (L4/L13). Here, on one host, the default local storage
# round-trips fine; on real separate VMs you'd point `result_storage` at shared
# storage (MinIO/S3, `result_storage="s3-bucket/minio-artifacts"`).
@flow(name="encode-segment", log_prints=True, persist_result=True)
def encode_segment(archive: str, segment: int) -> dict:
    """Encode one segment. Deployed to the `encode` pool; runs on any encode worker.

    The GCL named for THIS worker (`encode:<WORKER_ID>`) is what protects the
    machine's CPU: `occupy=THREADS_PER_ENCODE` so a box whose GCL limit is its
    core count admits exactly ⌊cores / threads⌋ encodes at once (L11). Every
    machine runs this same code; only its GCL limit number differs.
    """
    wid = os.environ.get("WORKER_ID", "local-1")

    # Optional: flip one segment to fail, to watch the coordinator isolate it (L13).
    fail = os.environ.get("FAIL_SEGMENT")

    with concurrency(f"encode:{wid}", occupy=THREADS_PER_ENCODE):
        print(
            f"[{wid}] encoding {archive} segment {segment} "
            f"(holding {THREADS_PER_ENCODE} slots)"
        )
        if fail is not None and segment == int(fail):
            raise RuntimeError(f"segment {segment}: corrupt frame (simulated)")
        # The heavy work is an EXTERNAL subprocess Prefect can't see (L11) — here
        # a `sleep` stands in for `ffmpeg -threads 2 ...`. The GCL, not Prefect's
        # run-counting, is what keeps the box from oversubscribing.
        subprocess.run(["sleep", "4"], check=True)

    out = f"encoded/{archive}/seg-{segment}.ivf"
    print(f"[{wid}] done → {out}")
    # Return the output PATH, never the bytes (L4/L13): the coordinator gathers
    # these from another process, so keep return values small and persistable.
    return {"segment": segment, "output": out, "worker": wid}


# --- The coordinator: extract, fan out, gather (async, L14) -------------------
@task
async def unpack(archive: str, n_segments: int) -> list[int]:
    """Light I/O: split the archive into segments. Runs here, on the extract worker.

    Async so the coordinator can `await` it without bridging an event loop.
    """
    print(f"unpacking {archive} → {n_segments} segments")
    return list(range(n_segments))


@flow(name="process-archive", log_prints=True)
async def process_archive(archive: str = "demo.tar", n_segments: int = 6) -> dict:
    """Unpack one archive, then dispatch each segment as its own encode run.

    L12 fan-out + L13 gather + L14 async, for real. Fire EVERY encode with
    `timeout=0` (so the fleet runs them in parallel, gated by each box's GCL),
    then `await` them all. `asyncio.gather` overlaps the creates in phase 1 and
    the waits in phase 2; `idempotency_key` from stable inputs makes a coordinator
    rerun reuse finished encodes instead of doubling the CPU.
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
                idempotency_key=f"{archive}:{s}",  # stable → safe to rerun
            )
            for s in segments
        )
    )
    print(f"dispatched {len(runs)} encode runs; gathering…")

    # Phase 2 — gather, concurrently. `wait_for_flow_run` is async-only in this
    # build (L14); under `await` + gather the waits overlap instead of a serial
    # poll loop.
    finished = await asyncio.gather(*(wait_for_flow_run(r.id) for r in runs))

    # The catch L14 surfaced: `wait_for_flow_run` is EVENT-driven (its
    # `poll_interval` is deprecated/ignored) — it returns on the first final-state
    # *event*, then re-reads the run, and that re-read can race the state commit,
    # handing back a still-RUNNING state for a run that actually finished (seen
    # live, ~1 run in 5). Re-read each run's authoritative state before
    # classifying — one read settles it. Gathered, so it stays concurrent.
    async with get_client() as client:
        finished = await asyncio.gather(
            *(client.read_flow_run(fr.id) for fr in finished)
        )

    done, failed = [], []
    for fr in finished:
        if fr.state.is_completed():
            # Async cross-process result fetch: `aresult`, NOT `result.aio` — the
            # bound-method `.aio` form drops `self` on our 3.7.5 build. Works because
            # the encode persists its result (L4/L13).
            done.append(await fr.state.aresult(raise_on_failure=False))
        else:
            failed.append(fr.parameters["segment"])  # record, don't re-raise

    print(f"{len(done)} ok, {len(failed)} failed: {failed}")
    return {"archive": archive, "ok": len(done), "failed": failed}


if __name__ == "__main__":
    # Registers BOTH deployments and exits — it does NOT run anything (that's the
    # workers' job, L8). Pre-reqs are in lesson 0015: create the `extract` and
    # `encode` pools, the per-worker GCL, then start a worker on each pool.
    src = str(HERE)

    process_archive.from_source(
        source=src, entrypoint="10_fleet.py:process_archive"
    ).deploy(name="extract", work_pool_name="extract")

    encode_segment.from_source(
        source=src, entrypoint="10_fleet.py:encode_segment"
    ).deploy(name="encode", work_pool_name="encode")

    print(
        "\nDeployed:\n"
        "  process-archive/extract  → extract pool (async coordinator)\n"
        "  encode-segment/encode    → encode pool\n\n"
        "Next (see lesson 0015): start a worker on each pool, then trigger\n"
        "  uv run prefect deployment run process-archive/extract",
        file=sys.stderr,
    )
