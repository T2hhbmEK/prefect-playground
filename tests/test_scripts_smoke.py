"""Smoke tests for the example scripts.

Cheap and infra-free: every script must import without side effects (real work
is guarded behind `__main__`), and every numbered lesson script must actually
define a Prefect flow. This catches the common breakage — a bad import, a
renamed symbol, work accidentally run at module scope — without a live server.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import example_scripts, load_script

from prefect import Flow

SCRIPTS = example_scripts()


def _flows(module) -> list[Flow]:
    return [v for v in vars(module).values() if isinstance(v, Flow)]


@pytest.mark.parametrize("script", SCRIPTS, ids=lambda p: p.name)
def test_script_imports_without_side_effects(script: Path):
    # If a script ran a flow / deploy / network call at import, this would hang
    # or raise — importing must be a no-op beyond defining flows and tasks.
    load_script(script)


@pytest.mark.parametrize(
    "script",
    [p for p in SCRIPTS if p.name[0].isdigit()],
    ids=lambda p: p.name,
)
def test_numbered_script_defines_a_flow(script: Path):
    assert _flows(load_script(script)), f"{script.name} defines no @flow"


def test_alert_listener_is_a_plain_webhook_catcher():
    # It's deliberately not a flow — just the local notification sink for L7.
    assert not _flows(load_script(Path("alert_listener.py").resolve())), (
        "alert_listener.py should not define a flow"
    )


def test_fleet_flow_names_match_the_fan_out_contract():
    # 10_fleet fans out via run_deployment("encode-segment/encode"), so the flow
    # names are a contract the coordinator depends on — pin them.
    fleet = load_script(next(p for p in SCRIPTS if p.name == "10_fleet.py"))
    names = {f.name for f in _flows(fleet)}
    assert {"encode-segment", "process-archive"} <= names, (
        f"10_fleet flow names drifted: {names}"
    )
