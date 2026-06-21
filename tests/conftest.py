"""Shared helpers and fixtures for the test suite.

The suite has two halves:

* **Artifact integrity** (`test_docs_integrity`, `test_repo_config`) — pure file
  reads, no Prefect, no Docker. These guard the teaching workspace against the
  kind of rot that keeps creeping in (broken cross-links, config drift).
* **Code** (`test_scripts_smoke`, `test_flows`) — importing the example scripts
  is safe because every script guards its real work behind `if __name__ ==
  "__main__"`, so import has no side effects. The one end-to-end flow run uses
  Prefect's in-process test harness (ephemeral SQLite) — still no Docker.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
LESSONS_DIR = REPO_ROOT / "lessons"
REFERENCE_DIR = REPO_ROOT / "reference"
RECORDS_DIR = REPO_ROOT / "learning-records"


def example_scripts() -> list[Path]:
    """Every runnable example script: the numbered flows plus `alert_listener`.

    Excludes `main.py` (the unused project-scaffold stub).
    """
    numbered = sorted(p for p in REPO_ROOT.glob("*.py") if p.name[0].isdigit())
    return [*numbered, REPO_ROOT / "alert_listener.py"]


def load_script(path: Path) -> ModuleType:
    """Import a top-level script by file path.

    The numbered scripts can't be imported with a normal `import` statement
    (their names start with a digit), so go through importlib. Results are cached
    in `sys.modules` under a sanitized name so repeated loads are cheap.
    """
    mod_name = "example_" + path.stem.replace("-", "_")
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(mod_name, path)
    assert spec and spec.loader, f"could not build import spec for {path}"
    module = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def prefect_harness():
    """Ephemeral in-process Prefect server for flow runs (no Docker needed)."""
    from prefect.testing.utilities import prefect_test_harness

    with prefect_test_harness():
        yield
