"""Behavioural tests for flow/task logic.

Two flavours:

* Pure-function tests via `task.fn` — the underlying callable, run with no
  Prefect runtime at all. Fast and deterministic.
* One real end-to-end flow run via the in-process test harness (ephemeral
  SQLite, no Docker), to prove the orchestration path actually works.
"""

from __future__ import annotations

import asyncio

import pytest
from conftest import REPO_ROOT, load_script

getting_started = load_script(REPO_ROOT / "01_getting_started.py")
fleet = load_script(REPO_ROOT / "10_fleet.py")


# --- pure-function logic (no Prefect runtime) --------------------------------


def test_unpack_splits_archive_into_indexed_segments():
    # `unpack` is an async task in the async-native coordinator, so `.fn` is a
    # coroutine function — run it to completion off any event loop.
    assert asyncio.run(fleet.unpack.fn("demo.tar", 6)) == [0, 1, 2, 3, 4, 5]


def test_process_customer_formats_the_id():
    assert getting_started.process_customer.fn("customer7") == "Processed customer7"


def test_get_customer_ids_returns_ten_unique_ids():
    ids = getting_started.get_customer_ids.fn()
    assert len(ids) == 10
    assert len(set(ids)) == 10
    assert all(cid.startswith("customer") for cid in ids)


# --- end-to-end orchestration (in-process harness) ---------------------------


@pytest.mark.slow
@pytest.mark.usefixtures("prefect_harness")
def test_getting_started_flow_runs_end_to_end():
    results = getting_started.main()
    assert len(results) == 10
    assert all(r.startswith("Processed customer") for r in results)
