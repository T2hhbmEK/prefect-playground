from prefect.blocks.notifications import CustomWebhookNotificationBlock

from prefect import flow, task

ALERT_BLOCK = "local-alert"


def get_alerter() -> CustomWebhookNotificationBlock:
    """Load the saved notification block, or build + save it on first run.

    This points at the local `alert_listener.py` catcher. In production you'd
    swap it for a `SlackWebhook` block (see lesson 7) — the `.notify()` call
    in the hook below doesn't change.
    """
    try:
        return CustomWebhookNotificationBlock.load(ALERT_BLOCK)
    except Exception:
        block = CustomWebhookNotificationBlock(
            name=ALERT_BLOCK,
            url="http://127.0.0.1:8077/alert",
            method="POST",
            json_data={"text": "{{subject}}\n{{body}}"},
            headers={"Content-Type": "application/json"},
            allow_private_urls=True,  # localhost is a private URL; opt in
        )
        block.save(ALERT_BLOCK, overwrite=True)
        return block


def alert_on_failure(flow, flow_run, state) -> None:
    """An on_failure hook: Prefect calls this when the flow enters Failed.

    Runs *in the flow's process*, so it catches Failed — but NOT Crashed (a
    killed process can't run its own hook). For Crashed coverage you'd add a
    server-side Automation; see lesson 7.
    """
    message = f"Flow '{flow_run.name}' entered {state.name}.\nReason: {state.message}"
    get_alerter().notify(body=message, subject="🔴 Prefect: a run failed")


@task
def fetch_batch() -> list[dict]:
    return [{"id": 1, "amount": 10}, {"id": 2, "amount": None}]


@task
def validate(rows: list[dict]) -> int:
    total = 0
    for row in rows:
        if row["amount"] is None:
            raise ValueError(f"row {row['id']} has no amount")
        total += row["amount"]
    return total


@flow(log_prints=True, on_failure=[alert_on_failure])
def nightly_report() -> int:
    rows = fetch_batch()
    return validate(rows)


if __name__ == "__main__":
    nightly_report()
