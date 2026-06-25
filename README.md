# prefect-playground

A **Prefect 3 teaching workspace** — runnable example flows plus a structured set of
lessons, reference cheatsheets, and learning records built with the `teach` skill.

Numbered scripts (`01_getting_started.py` … `10_fleet.py`, plus `alert_listener.py`) are
standalone, self-contained examples — each defines its own `@flow` and runs via
`if __name__ == "__main__"`. See `MISSION.md` for the goal, `NOTES.md` for the course
arc, and `AGENTS.md` for the full layout.

## Setup

Managed with [uv](https://docs.astral.sh/uv/) (Python) and [bun](https://bun.sh) (JS tooling):

```bash
uv sync        # Python deps
bun install    # lint/format tooling + git hooks
```

## Running

The local stack — Prefect server + UI, its Postgres DB, and a MinIO object store — runs
in Docker:

```bash
docker compose up -d                  # server/UI at http://localhost:4200
uv run python 01_getting_started.py   # run a flow against it
docker compose down                   # stop (data persists in volumes)
```

The host client talks to the Dockerized server via `prefect.toml`, so there's no need to
run `prefect server start` by hand.

## Browsing the lessons

The lessons, reference cheatsheets, and learning records render as a single web
front-end (`index.html`) — an arc-grouped sidebar over the course, loading each lesson
in an iframe. Open it directly (double-click / `file://`) or serve the repo root:

```bash
bun run serve   # → uv run python -m http.server 8000, then http://localhost:8000/
```

## Tests

```bash
uv run pytest        # docs-integrity + config + script-smoke + flow tests (no Docker)
```

See `AGENTS.md` for what each module covers.

## Notes

- Prefect is pinned to `3.7.5` (the stable release; previously the `3.7.5.dev4`
  pre-release). The server runs the official `prefecthq/prefect:3.7.5-python3.13` image,
  pinned to match the client.
- Server telemetry is disabled via `PREFECT_SERVER_ANALYTICS_ENABLED=false` in
  `docker-compose.yml`; client settings live in `prefect.toml`.
