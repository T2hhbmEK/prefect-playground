# AGENTS.md

This file provides guidance to AI coding agents when working with code in this repository.

## Overview

A **Prefect 3 teaching workspace** — not just a sandbox. Two intertwined layers:

1. **Runnable Prefect examples.** Numbered standalone scripts (`01_getting_started.py`
   … `10_fleet.py`, plus `alert_listener.py`); each is self-contained, defines its own
   `@flow`, and runs via an `if __name__ == "__main__"` block against the local
   Dockerized server. `main.py` is the unused project-scaffold stub.
2. **A structured learning system** driven by the `teach` skill, persisted as files in
   the repo (mission, lessons, reference docs, learning records). See
   [The teaching workspace](#the-teaching-workspace).

The real-world driver behind the lessons (`MISSION.md`): turning fragile scripts into
reliable Prefect pipelines, scaling toward a **heterogeneous AV1 media-encode fleet** —
light I/O *extract* jobs plus CPU-heavy ffmpeg/SVT-AV1 *encodes* that run as
subprocesses — on a self-hosted server. Lessons from L11 on are grounded in that scenario.

## The teaching workspace

The learning workflow is defined by the **`teach` skill** (`.claude/skills/teach/SKILL.md`;
invoke with `/teach`). Learning state lives in the files below — keep them in sync when
you add or change a lesson (the `*-FORMAT.md` files beside the skill define each doc's format):

- **`MISSION.md`** — *why* the user is learning; grounds every lesson.
- **`NOTES.md`** — teaching preferences + the running course arc (L1…L14). **Read this
  first** to see where the learner is and what comes next.
- **`RESOURCES.md`** — curated, trust-rated external sources; lessons cite these.
- **`lessons/NNNN-<slug>.html`** — the primary unit: one self-contained, beautiful HTML
  lesson per topic. Each links `../assets/lesson.css`, cross-links sibling lessons /
  reference docs by relative path, and cites a primary source.
- **`reference/*.html`** — compressed cheatsheets + a `glossary.html`; the durable
  quick-reference distilled from lessons.
- **`assets/lesson.css`** — the shared stylesheet + component library every lesson links.
  Reuse-first: add new reusable widgets here (e.g. `.sim`, `.fleet`, `.batch`,
  `.runboard`, `.recall` flip-cards) instead of inlining per-lesson.
- **`learning-records/NNNN-<slug>.md`** — ADR-style records of what was learned/decided.
  **Do not map records 1:1 to lessons, and do not renumber them:** records cross-link by
  slug via `[[wikilinks]]`, and they include decision-only records with no lesson
  (`0001-starting-point`, `0008-mission-expanded-scale`) that offset later records by +1
  (record `000X` ↔ lesson `L(X-1)` from `0009`/L8 onward). `NOTES.md` documents the mapping.

`handoff` (`.claude/skills/handoff/`) is a second installed skill. Skills are vendored
from `mattpocock/skills` and pinned in `skills-lock.json`; the `teach` skill is mirrored
under both `.agents/skills/` (agent-agnostic) and `.claude/skills/` (Claude).

## Environment & commands

Python is managed with [uv](https://docs.astral.sh/uv/). `prefect` is pinned to a **dev
build** (`prefect[aws,redis]==3.7.5.dev4`) — resolve it from `uv.lock` via uv, and don't
assume released-version behavior (verify against the Prefect GitHub source if docs disagree).

- Install deps: `uv sync`
- **Start the local stack:** `docker compose up -d --build` — runs the Prefect server
  (API/UI at `http://localhost:4200`), its Postgres DB, and a MinIO object store (console
  `:9001`, creds `minioadmin`/`minioadmin`, bucket `prefect-artifacts`). The server is
  built from `Dockerfile.server`, pinned to the same `3.7.5.dev4` build as the client so
  API versions match. Stop with `docker compose down` (volumes persist; `-v` wipes them).
  There is **no `prefect server start` by hand** — the host client talks to the Dockerized
  server on `:4200`.
- Run an example flow: `uv run python 01_getting_started.py`.
- Prefect CLI: `uv run prefect <cmd>` (e.g. `uv run prefect flow-run ls`).

No build step is configured.

## Tests

`uv run pytest` (pytest is a uv dev dependency; config in `pyproject.toml`
`[tool.pytest.ini_options]`). The suite needs **no Docker** — it never touches the
`:4200` server. `tests/`:

- **`test_docs_integrity.py`** — the teaching artifacts are the repo's product, so
  these guard them: every relative `href` across `lessons/` + `reference/` resolves,
  every lesson links `../assets/lesson.css` and cites an external primary source
  (review lessons exempt via `NO_CITATION_OK`), `[[wikilinks]]` in `learning-records/`
  resolve, lessons/records are contiguously numbered, and every page parses with a `<title>`.
- **`test_repo_config.py`** — pins the config facts AGENTS.md claims (ruff in `ruff.toml`
  not pyproject, no `[tool.prefect.*]` in pyproject, analytics off in compose, bun-only
  lockfile, client/server share the `3.7.5.dev4` pin). These have rotted before.
- **`test_scripts_smoke.py`** — every example script imports side-effect-free (real work
  stays behind `__main__`); each numbered script defines a flow.
- **`test_flows.py`** — `task.fn` unit tests plus one end-to-end run via
  `prefect_test_harness` (in-process ephemeral server, marked `slow`).

Markers (`pyproject.toml`): `-m "not slow"` skips the harness run; `integration` is
reserved for tests that need the live Dockerized stack (none yet).

## Configuration — where settings actually live

- **Prefect *client*** → `prefect.toml` (committed): pins `PREFECT_HOME=.prefect` (keeps
  all client state inside the repo), the API URL, and the local result-storage path.
  Auto-applied for any flow run from the repo root, regardless of the active `~/.prefect` profile.
- **Prefect *server*** → `docker-compose.yml` `environment:` block: DB connection URL and
  `PREFECT_SERVER_ANALYTICS_ENABLED: "false"` (telemetry off). Server settings are **not**
  in `pyproject.toml` or `prefect.toml`.
- **ruff** → `ruff.toml` (a dedicated file — **not** `[tool.ruff]` in `pyproject.toml`).
- **markdownlint** → `.markdownlint-cli2.yaml`; **commitlint** → `commitlint.config.mjs`.

## Linting & checks

JS-based tooling is managed with [bun](https://bun.sh). **Use `bun`/`bunx` — not
`npm`/`pnpm`/`yarn`** (a `PreToolUse` hook in `.claude/settings.json` denies the others).

- Install JS tooling: `bun install`
- Lint everything: `bun run lint`
- Markdown only: `bun run lint:md` (autofix: `bun run lint:md:fix`)
- Python only: `bun run lint:py` (autofix + format: `bun run lint:py:fix`)

Python is checked and formatted with [ruff](https://docs.astral.sh/ruff/) (a uv dev
dependency; config in `ruff.toml`). Run directly with `uv run ruff check .` and
`uv run ruff format .`.

## Git hooks

Managed by [husky](https://typicode.github.io/husky/) (installed via `package.json`'s
`prepare` script on `bun install`; hooks live in `.husky/`).

- **pre-commit** runs [lint-staged](https://github.com/lint-staged/lint-staged): linting
  only the *staged* files — markdownlint-cli2 for `*.md`, `uv run ruff check` +
  `ruff format --check` for `*.py`. A commit is blocked if any staged file fails. (Use
  `bun run lint` to check the whole repo manually.)
- **commit-msg** enforces [Conventional Commits](https://www.conventionalcommits.org) via
  [commitlint](https://commitlint.js.org) (config: `commitlint.config.mjs` extends
  `@commitlint/config-conventional`). Messages must look like `type(scope): summary`,
  e.g. `feat: add deployment example`. Teaching changes use a `teach` scope, e.g.
  `docs(teach): add lesson 14`.
