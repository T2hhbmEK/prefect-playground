# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Overview

A playground for experimenting with [Prefect](https://docs.prefect.io) 3.x flows. Numbered scripts (e.g. `01_getting_started.py`) are standalone, self-contained examples; each defines its own `@flow` and runs via an `if __name__ == "__main__"` block. `main.py` is the unused project scaffold stub.

## Environment & commands

Managed with [uv](https://docs.astral.sh/uv/). The `prefect` dependency is pinned to a dev build (`3.7.5.dev4`), so use uv to resolve it from `uv.lock`.

- Install deps: `uv sync`
- Start the local stack: `docker compose up -d --build` — runs the Prefect server (API/UI at `http://localhost:4200`), its Postgres database, and a MinIO object store (`docker-compose.yml`). The server image is pinned to the same `3.7.5.dev4` build as the client via `Dockerfile.server` so API versions match. Stop with `docker compose down` (data persists in volumes; `-v` wipes it).
- Run an example flow: `uv run python 01_getting_started.py` (the host client talks to the Dockerized server on `:4200`).
- Prefect CLI: `uv run prefect <cmd>` (e.g. `uv run prefect flow-run ls`).

There is no test suite or build step configured.

### Linting & checks

JS-based linters/checkers are managed with [bun](https://bun.sh) (`package.json`). Use `bun`/`bunx` for JS tooling in this repo — not `npm`, `pnpm`, or `yarn`.

- Install JS tooling: `bun install`
- Lint everything: `bun run lint`
- Markdown only: `bun run lint:md` (autofix: `bun run lint:md:fix`)
- Python only: `bun run lint:py` (autofix + format: `bun run lint:py:fix`)

Markdown is checked with [markdownlint-cli2](https://github.com/DavidAnson/markdownlint-cli2); rules and ignores live in `.markdownlint-cli2.yaml`.

Python is checked and formatted with [ruff](https://docs.astral.sh/ruff/) (a uv dev dependency); config is `[tool.ruff]` in `pyproject.toml`. Run directly with `uv run ruff check .` and `uv run ruff format .`.

### Git hooks

Managed by [husky](https://typicode.github.io/husky/) (installed via the `prepare` script on `bun install`; hooks live in `.husky/`).

- **pre-commit** runs [lint-staged](https://github.com/lint-staged/lint-staged) (`bunx lint-staged`), linting only the *staged* files: markdownlint-cli2 for `*.md`, `uv run ruff check` + `ruff format --check` for `*.py`. A commit is blocked if any staged file fails. (Use `bun run lint` to check the whole repo manually.)
- **commit-msg** runs [commitlint](https://commitlint.js.org) against [Conventional Commits](https://www.conventionalcommits.org) (config: `commitlint.config.mjs` extends `@commitlint/config-conventional`). Commit messages must look like `type(scope): summary`, e.g. `feat: add deployment example` or `fix(lint): correct ruff config`.
