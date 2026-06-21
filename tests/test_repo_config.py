"""Guards on where configuration lives, so the docs and reality can't drift.

Each assertion here mirrors a claim made in AGENTS.md. They exist because these
exact facts have rotted before: ruff config moving to its own file, Prefect
server settings leaving pyproject, a stray pnpm lockfile contradicting bun-only.
"""

from __future__ import annotations

import re
import tomllib

from conftest import REPO_ROOT

PREFECT_PIN = "3.7.5.dev4"


def _load_toml(name: str) -> dict:
    return tomllib.loads((REPO_ROOT / name).read_text())


# --- ruff config lives in ruff.toml, not pyproject ---------------------------


def test_ruff_config_is_a_dedicated_file():
    assert (REPO_ROOT / "ruff.toml").exists(), "ruff.toml is the canonical ruff config"


def test_pyproject_has_no_inline_ruff_config():
    tool = _load_toml("pyproject.toml").get("tool", {})
    assert "ruff" not in tool, "ruff config belongs in ruff.toml, not [tool.ruff]"


# --- Prefect settings: client in prefect.toml, server in docker-compose ------


def test_pyproject_has_no_prefect_server_config():
    # The old telemetry home; it now lives in docker-compose.yml's environment.
    tool = _load_toml("pyproject.toml").get("tool", {})
    assert "prefect" not in tool, "[tool.prefect.*] must not return to pyproject"


def test_prefect_client_config_points_at_local_server():
    cfg = _load_toml("prefect.toml")
    assert cfg.get("home") == ".prefect", "client state should stay inside the repo"
    assert "4200" in cfg.get("api", {}).get("url", ""), "client must target :4200"


def test_server_analytics_disabled_in_compose():
    compose = (REPO_ROOT / "docker-compose.yml").read_text()
    assert "PREFECT_SERVER_ANALYTICS_ENABLED" in compose
    assert re.search(r'PREFECT_SERVER_ANALYTICS_ENABLED:\s*"false"', compose), (
        "server telemetry should be disabled in docker-compose.yml"
    )


# --- JS tooling is bun-only --------------------------------------------------


def test_bun_is_the_only_js_lockfile():
    assert (REPO_ROOT / "bun.lock").exists(), "bun.lock is the JS lockfile"
    for forbidden in ("pnpm-lock.yaml", "package-lock.json", "yarn.lock"):
        assert not (REPO_ROOT / forbidden).exists(), (
            f"{forbidden} contradicts the bun-only policy"
        )


# --- the dev-build pin matches between client and server ---------------------


def test_client_and_server_pin_the_same_prefect_build():
    deps = _load_toml("pyproject.toml")["project"]["dependencies"]
    assert any(f"=={PREFECT_PIN}" in d for d in deps), (
        f"client must pin prefect=={PREFECT_PIN}"
    )
    dockerfile = (REPO_ROOT / "Dockerfile.server").read_text()
    assert f"prefect=={PREFECT_PIN}" in dockerfile, (
        f"server image must pin the same prefect=={PREFECT_PIN} as the client"
    )
