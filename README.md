# scenario_pipeliner

`scenario_pipeliner` is a v0 plugin migration planner and optional worker runtime.

**Consumer guide (installed library):**

- English: [docs/CONSUMER.md](docs/CONSUMER.md)

Example plugin skeleton and thin launcher: [`examples/`](examples/).

Current v0 scope:

- load and validate `plugin.manifest.json`,
- apply registry policy checks (compatibility + checksum),
- build dry-run migration plan as JSON,
- apply core DB migrations (`db migrate-core`) via Alembic,
- apply plugin SQL (`db migrate-plugins`, PostgreSQL),
- run worker via `scenario_pipeliner run` or `examples/main.py` (plugins stay external),
- parent/subtask lifecycle (`WAITING` → `FINISHED` / `FINISHED_WITH_ERROR`).

Out of scope in v0:

- bundling scenario plugins inside the published wheel.

## Install (development)

From the repository root:

```bash
uv sync --dev
```

From PyPI:

```bash
pip install scenario-pipeliner
```

Installed-library flow: [docs/CONSUMER.md](docs/CONSUMER.md).

## Lint commands (CI source of truth)

Use exactly the lint commands from `./.github/workflows/ci.yml`:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy .
```

## CLI usage (v0)

```bash
# validate plugins / dry-run plan
uv run scenario_pipeliner migrate --dry-run --format json

# core schema
uv run scenario_pipeliner db migrate-core

# plugin SQL (reads SCENARIO_PIPELINER_PLUGINS_ROOT)
uv run scenario_pipeliner db migrate-plugins

# worker (optional migrations + execute existing tasks)
uv run scenario_pipeliner run
uv run scenario_pipeliner run --skip-migrations

# compute / write plugin unpacked checksum
uv run scenario_pipeliner plugin checksum examples/plugins/hello_scenario
uv run scenario_pipeliner plugin checksum examples/plugins/hello_scenario --write
```

`--skip-migrations` requires a previous `db migrate-core` (library tables live in
`SCENARIO_PIPELINER_DB_SCHEMA`, default `sp`, not `public`).

Thin equivalent of `run`:

```bash
uv run python examples/main.py
```

Environment variables: see [docs/CONSUMER.md](docs/CONSUMER.md) and [`examples/.env.example`](examples/.env.example).
Python model bases (`Settings` / `FrozenSettings`): [docs/CONSUMER.md](docs/CONSUMER.md#settings-and-models).

CLI loads `.env` from the current working directory (and parents) automatically;
already-exported process environment variables still win.

Common discovery vars:

- `SCENARIO_PIPELINER_MODE`: `dev` (default) or `prod`
- `SCENARIO_PIPELINER_PLUGINS_ROOT`: plugin root path (default: `plugins`)
- `SCENARIO_PIPELINER_CORE_VERSION`: core version string for `core_compat` checks

Exit codes for `migrate --dry-run`:

- `0`: report status `ok` or `warning`
- `1`: report status `error`

## API usage (stable surface)

```python
from pathlib import Path

from scenario_pipeliner import (
    Mode,
    ScenarioPipelinerConfig,
    apply_migrations,
)

config = ScenarioPipelinerConfig(
    mode=Mode.DEV,
    plugins_root=Path("plugins"),
)
# db_backend defaults to postgresql; core_version defaults to __version__

report = apply_migrations(config, dry_run=True)
payload = report.model_dump(mode="json")
```

`dry_run=False` is not implemented in v0 and raises `NotImplementedError`.

Core schema bootstrap API:

```python
from scenario_pipeliner import CoreMigrationConfig, DbBackend, apply_core_migrations

report = apply_core_migrations(
    CoreMigrationConfig(
        db_backend=DbBackend.POSTGRESQL,
        db_schema="sp",
        postgres_host="localhost",
        postgres_db="scenario_pipeliner",
        postgres_user="postgres",
        postgres_password="postgres",
    )
)
```

From an already-running event loop (FastAPI, nested worker) use
`await apply_core_migrations_async(config)` instead. The sync wrapper is CLI-only.

## Plugin result semantics

Each plugin in `report.plugins` has one of:

- `loaded`: plugin passed checks and has migration plan for selected backend.
- `skipped`: plugin passed checks, but either has no `migrations` section or has no migration path for selected backend.
- `error`: plugin failed validation/policy checks.

For `skipped`, plugin is registered in dry-run context and keeps checksum/policy warnings in `reasons`, plus the skip reason (`no migration path for backend=...`).

## Task payload

`tasks.payload`, broker `TaskDraft.payload`, and pipeline `TaskState.payload` are
`TaskPayload` (`type_doc`, `data`, optional `params`). The library does not read
`params`. Full contract: [docs/CONSUMER.md](docs/CONSUMER.md#task-payload-taskpayload).

## Checksum notes

- `prod`: checksum mismatch is a hard error.
- `dev`: checksum mismatch is warning-only (best effort).
- Directory checksum excludes symlinks.
- Hardlinked files are included in checksum content.
- `plugin.manifest.json` is hashed as canonical JSON with `checksum.value` omitted.
