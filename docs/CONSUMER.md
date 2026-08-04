# Consumer guide: installed `scenario_pipeliner` (TestPyPI)

Русская версия: [CONSUMER.ru.md](./CONSUMER.ru.md)

This guide explains how to install the library from [TestPyPI](https://test.pypi.org/), place plugins, run migrations, and start a worker — the same flow as `examples/main.py` / `scenario_pipeliner run`.

## 1. Install (TestPyPI only)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install \
  --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  scenario-pipeliner
```

`--extra-index-url` is required so dependencies resolve from real PyPI.

Verify:

```bash
python -c "import scenario_pipeliner; print(scenario_pipeliner.__file__)"
scenario_pipeliner --help
```

## 2. Project layout

Plugins are **not** shipped inside the wheel. Keep them in your app repo:

```text
my_app/
  .env
  plugins/                    # or any path set via SCENARIO_PIPELINER_PLUGINS_ROOT
    hello_scenario/           # importable package name == directory name
      __init__.py
      plugin.manifest.json
      plugin.py               # entrypoint target
      pipeline.py
      settings.py
      steps.py
      states.py
      migration.sql
  run_worker.py               # optional thin launcher (see examples/main.py)
```

Reference skeleton: [`examples/plugins/hello_scenario`](../examples/plugins/hello_scenario).

### Discovery rules

- Library scans `SCENARIO_PIPELINER_PLUGINS_ROOT` (default: `./plugins`) for `plugin.manifest.json`.
- Manifest `entrypoint` looks like `hello_scenario.plugin:register`.
- At load time the library prepends **plugins root** to `sys.path`, so `hello_scenario` must be a package directory **directly under** that root.

## 3. Minimal plugin contract

Required pieces:

| File | Role |
|------|------|
| `plugin.manifest.json` | name, version, `core_compat`, checksum, entrypoint, scenarios, migrations |
| `plugin.py` | `register(registry, context=None)` registering each scenario |
| pipeline / steps / states | build an `AsyncPipeline` + task state class |
| `migration.sql` | optional plugin-owned SQL (path from manifest) |

Manifest sketch:

```json
{
  "plugin_name": "hello_scenario",
  "plugin_version": "1.0.0",
  "plugin_api_version": "v1",
  "core_compat": ">=0.1,<1.0",
  "checksum": {
    "algorithm": "sha256",
    "scope": "unpacked",
    "value": "<sha256 of plugin tree>"
  },
  "entrypoint": "hello_scenario.plugin:register",
  "scenarios": ["hello_scenario.ping"],
  "migrations": {
    "migration_order": "20260803120000",
    "postgresql": "migration.sql",
    "sqlite": "migration.sql"
  }
}
```

In `dev` mode checksum mismatches are warnings; in `prod` they fail loading.

## 4. Capabilities (CLI)

| Command | Purpose |
|---------|---------|
| `scenario_pipeliner migrate --dry-run --db-backend …` | Validate manifests / policy; JSON plan only (no SQL apply) |
| `scenario_pipeliner db migrate-core --db-backend …` | Apply **core** Alembic migrations (`tasks`, `settings`, `results`) |
| `scenario_pipeliner db migrate-plugins` | Apply **plugin** SQL files in `migration_order` (PostgreSQL) |
| `scenario_pipeliner run` | Opinionated worker: optional migrations → poll/execute existing tasks |
| `scenario_pipeliner plugin checksum <dir>` | Compute unpacked sha256 for a plugin tree |
| `scenario_pipeliner plugin checksum <dir> --write` | Write that checksum into existing `plugin.manifest.json` |

Public Python API (stable): `ScenarioPipelinerConfig`, `apply_migrations` (dry-run), `CoreMigrationConfig`, `apply_core_migrations`.

## 5. Recommended command order

From your app directory (with Postgres up). CLI loads `.env` from the current
working directory (and parents) automatically; exported process env still wins.

```bash
# 1) plan / validate plugins
scenario_pipeliner migrate --dry-run --format json --db-backend postgresql

# 2) core schema
scenario_pipeliner db migrate-core --db-backend postgresql

# 3) plugin SQL
scenario_pipeliner db migrate-plugins

# 4) run worker without re-applying migrations
scenario_pipeliner run --skip-migrations
```

One-shot alternative (migrations inside the process):

```bash
scenario_pipeliner run
# equivalent thin script:
python examples/main.py
```

`run` / `examples/main.py` currently support **PostgreSQL only**.

## 6. Environment variables

### Discovery / CLI

| Variable | Default | Meaning |
|----------|---------|---------|
| `SCENARIO_PIPELINER_PLUGINS_ROOT` | `plugins` | Root directory with plugin packages |
| `SCENARIO_PIPELINER_MODE` | `dev` | `dev` or `prod` policy |
| `SCENARIO_PIPELINER_CORE_VERSION` | `0.1.0` | Core version for `core_compat` (CLI and `run`) |
| `SCENARIO_PIPELINER_SQLITE_PATH` | — | SQLite path for `db migrate-core` |

### Postgres (migrate-plugins / run)

| Variable | Meaning |
|----------|---------|
| `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` | connection |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | credentials |
| `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE` | asyncpg pool |

### Logging / observability

Configured by `scenario_pipeliner.observability.setup_logging` (called from `RunnerApp.from_env`).

| Variable | Default | Meaning |
|----------|---------|---------|
| `LEVEL_LOGGING` / `SCENARIO_PIPELINER_LOG_LEVEL` | `INFO` | root log level |
| `SENTRY_DSN` | unset | enable Sentry (requires `pip install scenario-pipeliner[sentry]`) |
| `ENV` | `production` | Sentry environment |
| `SENTRY_TRACES_SAMPLE_RATE` | `0.1` | Sentry traces sample rate |
| `SENTRY_PROFILES_SAMPLE_RATE` | `0.1` | Sentry profiles sample rate |

### Runner entrypoint

| Variable | Default | Meaning |
|----------|---------|---------|
| `RUNNER_DB_BACKEND` | `postgresql` | must be postgresql for `run` |
| `RUNNER_POLL_INTERVAL_SECONDS` | `2` | runner poll |
| `RUNNER_TASKS_LIMIT` | `10` | batch size |
| `RUNNER_RUN_SECONDS` | unset | optional wall-clock stop for the runner process |
| `RUNNER_APPLY_MIGRATIONS` | `true` | set `false` / use `--skip-migrations` after CLI migrate |

Task seeding (`scenario`, payload, interval, max_executions) is **not** runner config.
Create tasks separately (planned: `scenario_pipeliner task create` — see `ROADMAP.md`).

See also [`examples/.env.example`](../examples/.env.example).

## 7. What `run` / `examples/main.py` do

1. Read env (`RuntimeEnvSettings` + Postgres settings) via `scenario_pipeliner.worker.runtime`.
2. Create asyncpg pool.
3. Optionally apply core + plugin migrations (`RUNNER_APPLY_MIGRATIONS`).
4. Discover plugins under `SCENARIO_PIPELINER_PLUGINS_ROOT` and enable registered scenarios.
5. Build Postgres `RunnerDB` and poll/execute existing `tasks` until timeout / signal.

Plugins that need host DI use `PluginContext.services`:

- shared resources from stock `run`: `services["__shared__"]` (e.g. `postgres_pool`);
- plugin-specific options: `services["<plugin_name>"]` (mapping or options object).

`track_documents` can auto-configure from `__shared__.postgres_pool` + Diadoc env vars
(`API_BASE_URL`, `DIADOC_API_CLIENT_ID`, `DIADOC_BOX_IDS`, `API_TOKEN` or
`API_USERNAME`/`API_PASSWORD`).

## 8. Exit codes

- `migrate --dry-run`: `0` if report status is `ok`/`warning`, `1` if `error`
- `db migrate-core` / `db migrate-plugins`: `0` on success, `2` on config validation errors
- `run`: `0` success, `130` interrupt, `1` runtime error
