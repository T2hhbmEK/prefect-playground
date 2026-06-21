import time
from datetime import timedelta

from prefect.cache_policies import INPUTS

from prefect import flow, task


@task(cache_policy=INPUTS, cache_expiration=timedelta(minutes=5))
def expensive_fetch(source: str) -> dict:
    """Pretend this hits a slow, rate-limited API.

    Retries (lesson 2) *re-run* work that failed. Caching does the opposite:
    it *avoids re-running* work that already succeeded. With `cache_policy=INPUTS`
    the result is keyed on the arguments — same `source` returns the stored
    result instantly instead of sleeping again.

    Because the policy is `INPUTS` (and NOT the default, which also keys on the
    flow run id), the cache survives *across separate runs*: run this script
    twice and the second run is instant.
    """
    print(f"  actually fetching {source} (slow)...")
    time.sleep(3)
    return {"source": source, "rows": 42}


@flow
def caching_pipeline(source: str = "api://customers") -> dict:
    # First call does the real (slow) work and persists the result.
    first = expensive_fetch(source)
    # Same input -> Cached state, returned instantly, no 3s sleep.
    second = expensive_fetch(source)
    # A different input misses the cache and does the work again.
    other = expensive_fetch("api://orders")
    return {"first": first, "second": second, "other": other}


if __name__ == "__main__":
    print(caching_pipeline())
