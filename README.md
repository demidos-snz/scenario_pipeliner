# scenario_pipeliner

`scenario_pipeliner` is a v0 plugin migration planner and optional worker runtime.

**Consumer guide (installed library):**

- English: [docs/CONSUMER.md](docs/CONSUMER.md)
- Русский: [docs/CONSUMER.ru.md](docs/CONSUMER.ru.md)

Example plugin skeleton and thin launcher: [`examples/`](examples/).

Current v0 scope:

- load and validate `plugin.manifest.json`,
- apply registry policy checks (compatibility + checksum),
- build dry-run migration plan as JSON,
- apply core DB migrations (`db migrate-core`) via Alembic,
- apply plugin SQL (`db migrate-plugins`, PostgreSQL),
- run worker via `scenario_pipeliner run` or `examples/main.py` (plugins stay external).

Out of scope in v0:

- bundling scenario plugins inside the published wheel.

## Install (development)

From `src/scenario_pipeliner`:

```bash
uv sync --dev
```

Installed-from-TestPyPI flow is documented in [docs/CONSUMER.md](docs/CONSUMER.md).

## CLI usage (v0)

```bash
# validate plugins / dry-run plan
uv run scenario_pipeliner migrate --dry-run --format json --db-backend postgresql

# core schema
uv run scenario_pipeliner db migrate-core --db-backend postgresql

# plugin SQL (reads SCENARIO_PIPELINER_PLUGINS_ROOT)
uv run scenario_pipeliner db migrate-plugins

# worker (optional migrations + execute existing tasks)
uv run scenario_pipeliner run
uv run scenario_pipeliner run --skip-migrations

# compute / write plugin unpacked checksum
uv run scenario_pipeliner plugin checksum examples/plugins/hello_scenario
uv run scenario_pipeliner plugin checksum examples/plugins/hello_scenario --write
```

Thin equivalent of `run`:

```bash
uv run python examples/main.py
```

Environment variables: see [docs/CONSUMER.md](docs/CONSUMER.md) and [`examples/.env.example`](examples/.env.example).

Common discovery vars:

- `SCENARIO_PIPELINER_MODE`: `dev` (default) or `prod`
- `SCENARIO_PIPELINER_PLUGINS_ROOT`: plugin root path (default: `plugins`)
- `SCENARIO_PIPELINER_CORE_VERSION`: core version string (default: `0.1.0`)

Exit codes for `migrate --dry-run`:

- `0`: report status `ok` or `warning`
- `1`: report status `error`

## API usage (stable surface)

```python
from pathlib import Path

from scenario_pipeliner import (
    DbBackend,
    Mode,
    ScenarioPipelinerConfig,
    apply_migrations,
)

config = ScenarioPipelinerConfig(
    mode=Mode.DEV,
    db_backend=DbBackend.SQLITE,
    plugins_root=Path("plugins"),
    core_version="0.1.0",
)

report = apply_migrations(config, dry_run=True)
payload = report.model_dump(mode="json")
```

`dry_run=False` is not implemented in v0 and raises `NotImplementedError`.

Core schema bootstrap API:

```python
from scenario_pipeliner import CoreMigrationConfig, DbBackend, apply_core_migrations

report = apply_core_migrations(
    CoreMigrationConfig(
        db_backend=DbBackend.SQLITE,
        sqlite_path="runtime/core.sqlite3",
    )
)
```

## Plugin result semantics

Each plugin in `report.plugins` has one of:

- `loaded`: plugin passed checks and has migration plan for selected backend.
- `skipped`: plugin passed checks, but has no migration path for selected backend.
- `error`: plugin failed validation/policy checks.

For `skipped`, plugin is registered in dry-run context and keeps checksum/policy warnings in `reasons`, plus the skip reason (`no migration path for backend=...`).

## Checksum notes

- `prod`: checksum mismatch is a hard error.
- `dev`: checksum mismatch is warning-only (best effort).
- Directory checksum excludes symlinks.
- Hardlinked files are included in checksum content.
- Manifest file itself is excluded from the directory digest.
