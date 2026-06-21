from pathlib import Path

from prefect import flow, task


@task
def fetch_rows() -> int:
    return 42


@flow(log_prints=True)
def worker_pipeline(label: str = "from-a-worker") -> int:
    n = fetch_rows()
    print(f"[{label}] processed {n} rows — executed by a worker, not by me")
    return n


if __name__ == "__main__":
    # Unlike `serve()` (lesson 5), this does NOT run the flow or stay attached.
    # It registers a deployment that points a *work pool* at this code, then
    # exits. A separate `prefect worker start --pool local-process` process is
    # what actually pulls and runs the scheduled work.
    worker_pipeline.from_source(
        source=str(Path(__file__).parent),  # local code; the worker loads it from here
        entrypoint="08_workers.py:worker_pipeline",
    ).deploy(
        name="worker-pipeline",
        work_pool_name="local-process",
        # cron="0 8 * * *",   # schedules like serve(); omitted for the demo
    )
