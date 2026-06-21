import random

from prefect.tasks import exponential_backoff

from prefect import flow, task


@task(retries=3, retry_delay_seconds=2)
def fetch_flaky_data(source: str) -> dict:
    """Simulate a flaky network fetch that fails ~60% of the time.

    A transient blip (timeout, dropped connection, 503) is exactly what
    `retries` is for: the task re-runs itself instead of sinking the flow.
    """
    if random.random() < 0.6:
        raise ConnectionError(f"transient blip talking to {source}")
    return {"source": source, "rows": random.randint(10, 100)}


@task(retries=4, retry_delay_seconds=exponential_backoff(backoff_factor=2))
def fetch_with_backoff(source: str) -> dict:
    """Same idea, but waits 2s, 4s, 8s, 16s between attempts.

    Backoff is the polite way to retry: it gives a struggling upstream
    service room to recover instead of hammering it.
    """
    if random.random() < 0.6:
        raise ConnectionError(f"transient blip talking to {source}")
    return {"source": source, "rows": random.randint(10, 100)}


@flow
def resilient_pipeline(source: str = "api://customers") -> dict:
    # Watch this in the UI: the task may show several attempts before it
    # finally lands on Completed. The flow only fails if ALL attempts fail.
    return fetch_flaky_data(source)


if __name__ == "__main__":
    print(resilient_pipeline())
