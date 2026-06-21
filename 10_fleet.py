"""Lesson 14 — the runnable end-to-end fleet demo.

Three lessons of theory (L11 per-worker GCL + `occupy`, L12 split pools + fan-out,
L13 gather + partial failure + `idempotency_key`) finally executed on the Docker
stack. Two deployments, two pools, two workers — all on one host, standing in for
the heterogeneous VM fleet.

    encode-segment/encode   → the `encode` pool (CPU work; GCL-capped per worker)
    process-archive/extract → the `extract` pool (light I/O; fans out + gathers)

Run `uv run python 10_fleet.py` to register BOTH deployments, then follow the
lesson (0014) for the pool/worker/GCL setup and how to trigger a run.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

from prefect.client.orchestration import get_client
from prefect.concurrency.sync import concurrency
from prefect.deployments import run_deployment

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


# --- The coordinator: extract, fan out, gather -------------------------------
@task
def unpack(archive: str, n_segments: int) -> list[int]:
    """Light I/O: split the archive into segments. Runs here, on the extract worker."""
    print(f"unpacking {archive} → {n_segments} segments")
    return list(range(n_segments))


def gather(run_ids: list, poll: int = 2) -> list:
    """Block until every fired run reaches a final state; return them in order.

    L13 reached for `wait_for_flow_run`, but in our 3.7.5.dev4 build that's a
    *coroutine* — it can't be called from this sync flow. So the sync coordinator
    polls run state with the sync client instead. The encodes already run in
    parallel on the fleet (we fired them with `timeout=0`); this just waits on the
    batch. Overlapping the waits with `asyncio.gather` is the next lesson.
    """
    pending, finished = list(run_ids), {}
    with get_client(sync_client=True) as client:
        while pending:
            time.sleep(poll)
            still = []
            for rid in pending:
                fr = client.read_flow_run(rid)
                if fr.state.is_final():
                    finished[rid] = fr
                else:
                    still.append(rid)
            pending = still
    return [finished[rid] for rid in run_ids]


@flow(name="process-archive", log_prints=True)
def process_archive(archive: str = "demo.tar", n_segments: int = 6) -> dict:
    """Unpack one archive, then dispatch each segment as its own encode run.

    L12 fan-out + L13 gather, for real: fire EVERY encode with `timeout=0` (so the
    fleet runs them in parallel, gated by each box's GCL), then `wait_for_flow_run`
    on each. `idempotency_key` from stable inputs makes a coordinator rerun reuse
    finished encodes instead of doubling the CPU.
    """
    segments = unpack(archive, n_segments)

    runs = [  # fire all — don't block here
        run_deployment(
            "encode-segment/encode",
            parameters={"archive": archive, "segment": s},
            timeout=0,  # return the instant it's created
            idempotency_key=f"{archive}:{s}",  # stable → safe to rerun
        )
        for s in segments
    ]
    print(f"dispatched {len(runs)} encode runs; gathering…")

    finished = gather([r.id for r in runs])  # fire all first, then wait on the batch

    done, failed = [], []
    for fr in finished:
        if fr.state.is_completed():
            # Cross-process result: works because the encode persists it (L4/L13).
            done.append(fr.state.result(raise_on_failure=False))
        else:
            failed.append(fr.parameters["segment"])  # record, don't re-raise

    print(f"{len(done)} ok, {len(failed)} failed: {failed}")
    return {"archive": archive, "ok": len(done), "failed": failed}


if __name__ == "__main__":
    # Registers BOTH deployments and exits — it does NOT run anything (that's the
    # workers' job, L8). Pre-reqs are in lesson 0014: create the `extract` and
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
        "  process-archive/extract  → extract pool\n"
        "  encode-segment/encode    → encode pool\n\n"
        "Next (see lesson 0014): start a worker on each pool, then trigger\n"
        "  uv run prefect deployment run process-archive/extract",
        file=sys.stderr,
    )
