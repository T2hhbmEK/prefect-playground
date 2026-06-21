from prefect import flow, task


@task
def fetch_batch() -> list[dict]:
    # The second row is missing its amount — a data-quality problem upstream.
    return [{"id": 1, "amount": 10}, {"id": 2, "amount": None}]


@task
def validate(rows: list[dict]) -> int:
    total = 0
    for row in rows:
        if row["amount"] is None:
            # A *permanent* failure: bad input won't fix itself on a retry
            # (contrast with the transient blips from lesson 2). This is the
            # kind of failure you diagnose from run history the next morning.
            raise ValueError(f"row {row['id']} has no amount")
        total += row["amount"]
    return total


@flow(log_prints=True)
def nightly_report() -> int:
    rows = fetch_batch()
    total = validate(rows)
    print(f"total = {total}")
    return total


if __name__ == "__main__":
    nightly_report()
