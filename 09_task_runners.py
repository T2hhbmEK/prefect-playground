import time

from prefect.task_runners import ProcessPoolTaskRunner, ThreadPoolTaskRunner

from prefect import flow, task


@task
def fetch(source: int) -> int:
    """I/O-bound: waiting on a (simulated) network call.

    Sleeping releases the GIL, so threads can overlap the waiting — four
    half-second fetches finish in about half a second, not two.
    """
    time.sleep(0.5)
    return source


@task
def crunch(seed: int) -> int:
    """CPU-bound: a tight Python loop that holds the GIL.

    Threads CANNOT run this in parallel (the GIL serializes them); only
    separate processes can use multiple cores at once.
    """
    total = 0
    for i in range(40_000_000):
        total += i
    return total


# Task runners only affect tasks you .submit() or .map() — plain calls stay
# sequential no matter which runner you pick.
@flow(task_runner=ThreadPoolTaskRunner(max_workers=4))
def fetch_threaded() -> list[int]:
    return fetch.map(range(4)).result()


@flow(task_runner=ThreadPoolTaskRunner(max_workers=4))
def crunch_threaded() -> list[int]:
    return crunch.map(range(4)).result()


@flow(task_runner=ProcessPoolTaskRunner(max_workers=4))
def crunch_parallel() -> list[int]:
    return crunch.map(range(4)).result()


def timed(fn) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


if __name__ == "__main__":
    io_threads = timed(fetch_threaded)
    cpu_threads = timed(crunch_threaded)
    cpu_procs = timed(crunch_parallel)
    print(f"4x fetch  (I/O) ThreadPool : {io_threads:.1f}s  # ~2s sequential")
    print(f"4x crunch (CPU) ThreadPool : {cpu_threads:.1f}s  # GIL: no speedup")
    print(f"4x crunch (CPU) ProcessPool: {cpu_procs:.1f}s  # real parallelism")
