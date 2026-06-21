# prefect-playground

A playground for experimenting with [Prefect](https://docs.prefect.io) 3.x flows.

Numbered scripts (e.g. `01_getting_started.py`) are standalone, self-contained
examples — each defines its own `@flow` and runs via `if __name__ == "__main__"`.

## Setup

Managed with [uv](https://docs.astral.sh/uv/):

```bash
uv sync
```

## Usage

```bash
# Run an example flow
uv run python 01_getting_started.py

# Prefect CLI (e.g. start a local API/UI)
uv run prefect server start
```

## Notes

Prefect telemetry is disabled via `[tool.prefect.server] analytics_enabled = false`
in `pyproject.toml`.
