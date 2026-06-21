from prefect import flow, task


@task
def fetch_rows() -> int:
    # Stand-in for the real fetch step of your pipeline.
    return 42


@flow(log_prints=True)
def scheduled_pipeline(label: str = "nightly") -> int:
    """A normal flow — nothing here knows about schedules.

    `log_prints=True` routes plain `print()` into the run's logs, so you can
    read this line in the UI for every scheduled run.
    """
    n = fetch_rows()
    print(f"[{label}] processed {n} rows")
    return n


if __name__ == "__main__":
    # `.serve()` does two things: it registers this flow as a *deployment*, and
    # it starts a long-running process that creates scheduled runs and executes
    # them. Leave it running in a terminal; Ctrl-C to stop. No work pools, no
    # workers, no containers — just this process.
    scheduled_pipeline.serve(
        name="scheduled-pipeline",
        interval=30,  # fire a run every 30s so you can watch it tick live
        # cron="0 8 * * *",   # ...the real-world version: every day at 08:00
        tags=["lesson-5"],
    )
