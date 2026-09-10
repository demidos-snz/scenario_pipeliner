# Consumer guide: installed `scenario_pipeliner`

This guide explains how to install the library from [PyPI](https://pypi.org/project/scenario-pipeliner/), place plugins, run migrations, and start a worker — the same flow as `examples/main.py` / `scenario_pipeliner run`.

## 1. Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install scenario-pipeliner
# optional observability:
# pip install 'scenario-pipeliner[sentry]'
```

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
- Each plugin directory must sit **directly under** that root. The worker loads it from `plugin_dir` as a unique internal package (`_sp_plugin_{name}_{hash}`) and aliases the public name (`hello_scenario`) when that name is free. Two plugins no longer share a `sys.path` prepend of the plugins root.

## 3. Minimal plugin contract

Required pieces:

| File | Role |
|------|------|
| `plugin.manifest.json` | name, version, `core_compat`, checksum, entrypoint, scenarios, optional migrations |
| `plugin.py` | `register(registry, context=None)` registering each scenario |
| pipeline / steps / states | build an `AsyncPipeline` + task state class |
| `migration.sql` | optional plugin-owned SQL (path from manifest) |

Manifest sketch:

```json
{
  "plugin_name": "hello_scenario",
  "plugin_version": "1.0.0",
  "plugin_api_version": "v1",
  "core_compat": ">=0.0.1,<1.0",
  "checksum": {
    "algorithm": "sha256",
    "scope": "unpacked",
    "value": "<sha256 of plugin tree>"
  },
  "entrypoint": "hello_scenario.plugin:register",
  "scenarios": ["hello_scenario.ping"],
  "migrations": {
    "migration_order": "20260803120000",
    "postgresql": "migration.sql"
  }
}
```

`migrations` is optional. If the section is missing, dry-run marks the plugin as
`skipped` for migration planning. When the section is present, `postgresql` is
**required**.

Plugin SQL must contain both `{{core_schema}}` and `{{plugin_schema}}` placeholders
(see [PostgreSQL schemas](#postgresql-schemas)).

In `dev` mode checksum mismatches are warnings; in `prod` they fail loading.
The unpacked digest hashes plugin files plus canonical `plugin.manifest.json`
with `checksum.value` stripped, so changing `entrypoint` / `core_compat` /
`migrations` invalidates the digest. Ignore rules (`__pycache__`, `.DS_Store`)
use paths relative to the plugin directory.

### Settings and models

Library models are Pydantic. Shared bases live in `scenario_pipeliner.base_settings`
(re-exported from `scenario_pipeliner.worker.core.settings`):

| Base | Use |
|------|-----|
| `Settings` | configuration (env, runner, plugin step/client settings, `ScenarioPipelinerConfig`, `CoreMigrationConfig`). Mutable. Unknown keys ignored. |
| `FrozenSettings` | immutable DTOs: `api.settings` (manifests, dry-run reports), `db.settings` (`CoreDb`, `CoreSchema`, `PostgresUrls`), `worker.broker.settings` (drafts/views). |

Plugin code subclasses `StepSettings` / `ClientSettings` (both are `Settings`).
Do not add dataclasses for new library models.

### Task payload (`TaskPayload`)

`tasks.payload` (JSONB), `TaskDraft.payload` (broker ingress), and
`TaskState.payload` (pipeline steps) are the same model:
`scenario_pipeliner.worker.core.states.TaskPayload`.

```python
from scenario_pipeliner.worker.core.enums import EnumDoc
from scenario_pipeliner.worker.core.states import TaskPayload, TaskPayloadParams

TaskPayload(
    type_doc=EnumDoc.JSON,  # default: XML
    data=["…"],  # document bodies or opaque strings
    params=TaskPayloadParams(  # optional JSON object; None if unused
        request_ids=["req-1"],
        cursor=10,
    ),
)
```

| Field | Library behavior |
|-------|------------------|
| `type_doc` | Validated as `XML` \| `JSON`. Stored, not interpreted. |
| `data` | List of strings (`str` is coerced to a one-element list). `payload.primary` is `data[0]` or `""`. Cyclical retry writes the whole model back to JSONB. |
| `params` | Optional JSON object (`extra=allow`). **The library never reads it.** Put plugin keys here (or subclass `TaskPayloadParams`). Omit if unused. |
| any other JSON key on `TaskPayload` | Ignored (`extra=ignore`). Do not put `order_id` next to `data` — it will not reach the step. |

A dict is accepted and coerced to `TaskPayload` (broker hooks may still pass a dict).
Inserts persist `model_dump()`, so JSONB matches what steps see.

### Broker (`PluginBrokerSettings`)

`run --mode broker` / `all` consumes plugin queues and publishes replies via
`broker_outbox`. Each broker plugin supplies `PluginBrokerSettings`
(`input_queues`, `output_queues`, `queue_bindings`, `prefetch_count`,
`stale_processing_minutes`, `max_ingress_retries`). Process-level outbox cadence
(`POLL_INTERVAL_SECONDS`, `RECONCILE_LIMIT`, `OUTBOX_LIMIT`) lives on
`RunnerBrokerSettings` (env: `RUNNER_POLL_INTERVAL_SECONDS`,
`RUNNER_RECONCILE_LIMIT`, `RUNNER_OUTBOX_LIMIT`).

**Outbox claim is process-wide, not per plugin.** `claim_pending_outbox` selects
from `broker_outbox` without filtering on `broker_ingress.plugin_name`. Two
broker processes cannot take the same row (`PROCESSING` + `SKIP LOCKED`). The
`PROCESSING` lease (`stale_processing_minutes`, default 15) is one number for
the whole process: with several plugins the **shortest** configured value is
used. A plugin cannot have a longer lease than a sibling in the same process.
Per-plugin leases are out of scope in v0 (would need a claim filtered by
plugin).

A broker plugin must own at least one registered scenario (`{plugin_name}.*`
or the plugin name itself). Startup fails if there are none — the library does
not bind foreign scenarios.

Publish is **at-least-once per output queue**. One outbox row fans out to
`ReplyDraft.routing_queues` (else ingress `reply_to`, else `output_queues`).
`SENT` is written only after every queue in that list succeeds in one pass.
A crash or error after queue A and before queue B (or before `SENT`) returns
the row to `PENDING` and retries the **whole** list — A may see a duplicate.
Consumers must dedupe (library sets `task_id` on publish headers). Per-queue
delivery state is out of scope in v0.

`IngressRetry` (and unexpected hook errors) requeue until
`max_ingress_retries` (default 3), counted from `x-delivery-count` /
`x-death`, else `redelivered` (then the cap is effectively 2). After the cap:
nack without requeue.

## 4. Capabilities (CLI)

| Command | Purpose |
|---------|---------|
| `scenario_pipeliner migrate --dry-run` | Validate manifests / policy; JSON plan only (no SQL apply) |
| `scenario_pipeliner db migrate-core` | Apply **core** Alembic migrations in the configured schema (`tasks`, `settings`, `results`, `broker_*`, `plugin_migrations`) |
| `scenario_pipeliner db migrate-plugins` | Apply **plugin** SQL files in `migration_order` (PostgreSQL) for plugins that define migrations |
| `scenario_pipeliner run` | Opinionated worker: optional migrations → poll/execute existing tasks |
| `scenario_pipeliner plugin checksum <dir>` | Compute unpacked sha256 for a plugin tree |
| `scenario_pipeliner plugin checksum <dir> --write` | Write that checksum into existing `plugin.manifest.json` |
| `scenario_pipeliner plugin init <dir>` | Scaffold package stubs + `plugin.manifest.json` + checksum (`--if-exists fail` / `overwrite` / `checksum-only`) |
| `scenario_pipeliner task list` | List scenario keys from manifests under `SCENARIO_PIPELINER_PLUGINS_ROOT` |
| `scenario_pipeliner task create --scenario …` | Insert a `NEW` row into `{schema}.tasks` (`LINEAR` default; Postgres from env) |

Public Python API (stable): `ScenarioPipelinerConfig`, `apply_migrations` (dry-run),
`CoreMigrationConfig`, `apply_core_migrations` (sync, CLI-only — cannot run inside
an event loop), `apply_core_migrations_async` (embedders / FastAPI).

## 5. Recommended command order

From your app directory (with Postgres up). CLI loads `.env` from the current
working directory (and parents) automatically; exported process env still wins.

```bash
# 0) optional: scaffold a plugin (name = directory = import path)
scenario_pipeliner plugin init plugins/hello_scenario

# 1) plan / validate plugins
scenario_pipeliner migrate --dry-run --format json

# 2) core schema
scenario_pipeliner db migrate-core

# 3) plugin SQL
scenario_pipeliner db migrate-plugins

# 4) insert a task (`run` does not seed tasks)
scenario_pipeliner task list
scenario_pipeliner task create --scenario hello_scenario.ping

# 5) run worker without re-applying migrations
scenario_pipeliner run --skip-migrations
```

`--skip-migrations` only skips *applying* SQL. Core tables must already exist in
`SCENARIO_PIPELINER_DB_SCHEMA` (default `sp`, e.g. `sp.settings`). Older installs
with tables in `public` are a different namespace — run `db migrate-core` against
the new schema (that does not copy data).

`db migrate-core` creates schema `SCENARIO_PIPELINER_DB_SCHEMA` (default `sp`).
The app role needs `CREATE` on the database. PostgreSQL 15+ does not grant
`CREATE` to `PUBLIC`, so `permission denied for database` is common.

A DBA can grant it:

```sql
GRANT CREATE ON DATABASE <database> TO <role>;
```

or create the schema without `CREATE` on the whole database:

```sql
CREATE SCHEMA sp AUTHORIZATION <role>;
GRANT USAGE, CREATE ON SCHEMA sp TO <role>;
```

Plugin schemas (`sanitize(plugin_name)`, e.g. `hello_scenario`) need the same treatment.
Re-run `db migrate-core` after the grant or pre-created schema.

One-shot alternative (migrations inside the process):

```bash
scenario_pipeliner run
# equivalent thin script:
python examples/main.py
```

`run` / `examples/main.py` support **PostgreSQL only**.

## 6. Environment variables

### Discovery / CLI

| Variable | Default | Meaning |
|----------|---------|---------|
| `SCENARIO_PIPELINER_PLUGINS_ROOT` | `plugins` | Root directory with plugin packages |
| `SCENARIO_PIPELINER_MODE` | `dev` | `dev` or `prod` policy |
| `SCENARIO_PIPELINER_CORE_VERSION` | package version (`0.0.1`) | Core version for `core_compat` (CLI and `run`) |
| `SCENARIO_PIPELINER_DB_SCHEMA` | `sp` | PostgreSQL schema for **library** tables (`tasks`, `settings`, …) |

### Postgres (migrate-plugins / run)

| Variable | Meaning |
|----------|---------|
| `POSTGRES_URL` | optional full URL (`postgresql://`, `postgresql+asyncpg://`, `postgresql+psycopg://`). Used when it is a valid Postgres URL (host + database). Invalid or unset → `POSTGRES_*` |
| `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` | connection (fallback when `POSTGRES_URL` is missing or not a valid Postgres URL) |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | credentials for the `POSTGRES_*` fallback |
| `DB_POOL_MIN_SIZE` / `DB_POOL_MAX_SIZE` | pool bounds (default 1 / 5). **Each process opens two pools** (SQLAlchemy worker + asyncpg for plugins), so the Postgres budget is up to `2 * DB_POOL_MAX_SIZE` connections per process. |

`POSTGRES_URL` is parsed only (sync or async driver). A URL that does not parse as Postgres, or that lacks host/database, is ignored; the process does **not** probe the server to decide fallback. Live connection errors still fail at connect time. Invalid `POSTGRES_PORT` (non-integer) is an error, not a silent `5432`.

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
| `RUNNER_POLL_INTERVAL_SECONDS` | `2` | DB poll and broker outbox reconcile/publish interval |
| `RUNNER_TASKS_LIMIT` | `10` | DB batch size |
| `RUNNER_ZOMBIE_TASKS_TIMEOUT_MINUTES` | `30` | reclaim `QUEUED`/`RUNNING` without a fresh heartbeat |
| `RUNNER_MAX_CONCURRENT_TASKS` | `5` | in-flight pipelines per batch |
| `RUNNER_HEARTBEAT_INTERVAL_SECONDS` | `30` | lease refresh while a task runs (must stay below zombie timeout) |
| `RUNNER_RECONCILE_LIMIT` | `50` | broker: terminal tasks considered per outbox reconcile |
| `RUNNER_OUTBOX_LIMIT` | `20` | broker: outbox rows claimed per publish pass |
| `RUNNER_SHUTDOWN_TIMEOUT_SECONDS` | `30` | drain after `stop()` / signal, **not** a per-batch SLA |
| `RUNNER_RUN_SECONDS` | unset | optional wall-clock stop for the runner process |
| `RUNNER_APPLY_MIGRATIONS` | `true` | set `false` / use `--skip-migrations` after CLI migrate |

Task seeding (`scenario`, payload, interval, max_executions) is **not** runner
config. Use `scenario_pipeliner task create` (or host SQL / `create_task`).

See also [`examples/.env.example`](../examples/.env.example).

## 7. What `run` / `examples/main.py` do

1. Load the nearest `.env` from cwd/parents (`RunnerApp.from_env()` and CLI
   `main`; existing shell env wins). Then read env (`RuntimeEnvSettings` +
   Postgres settings).
2. Create a SQLAlchemy async engine and an asyncpg pool for plugins
   (`services["__shared__"]["postgres_pool"]`). Both set `search_path` to the
   core schema. Combined connection budget: `2 * DB_POOL_MAX_SIZE`.
3. Optionally apply core + plugin migrations (`RUNNER_APPLY_MIGRATIONS`).
4. Discover plugins under `SCENARIO_PIPELINER_PLUGINS_ROOT` and enable registered scenarios.
5. For each scenario, insert `pipeline_active_{scenario}=1` and `worker_enabled=1`
   if those keys are missing (`ensure_worker_settings`). Existing values, including
   ops kill-switches (`0`), are left unchanged.
6. Build Postgres `RunnerDB` and poll/execute existing `tasks` until timeout / signal.

Plugins that need host DI use `PluginContext.services`:

- shared resources from stock `run`: `services["__shared__"]` (e.g. `postgres_pool`);
- plugin-specific options: `services["<plugin_name>"]` (mapping or options object).

The library does not auto-configure plugin-specific clients. A plugin reads
`PluginContext.services` itself.

### Parent / subtask statuses

| Situation | Parent status |
|-----------|---------------|
| Root finished pipeline but **any** children still open (blocking or not) | `WAITING` |
| All children terminal, no failed non-blocking | `FINISHED` (only if parent is still `WAITING`) |
| All children terminal, some `is_block=false` failed | `FINISHED_WITH_ERROR` (only if parent is still `WAITING`) |
| Blocking child `FAILED` | parent `FAILED` **only if it is still `WAITING`**; `NEW` / `QUEUED` / `RUNNING` siblings `CANCELLED` |

The worker lock primarily picks `NEW` (and zombie `QUEUED`). After unblock the
parent is **not** set back to `QUEUED`, so it does not sit forever unpicked.

### Plugin layout notes

- Plugins are packages **directly under** `SCENARIO_PIPELINER_PLUGINS_ROOT`
  (not `scenario_pipeliner.*`). Intra-plugin imports such as
  `from hello_scenario.steps import …` use the public alias when it is free.
- `core_compat` in `plugin.manifest.json` must include `SCENARIO_PIPELINER_CORE_VERSION`
  (e.g. library `0.0.x` needs `>=0.0.1,<1.0`, not `>=0.1,<1.0`).
- Already-exported shell env vars override `.env` (`load_dotenv(override=False)`).

## 8. PostgreSQL schemas

Library objects live in a dedicated schema (default `sp`), not in `public` and not
as table-name prefixes (`sp.tasks`, not `sp_tasks`).

| Object | Schema |
|--------|--------|
| Core tables (`tasks`, `settings`, `results`, `broker_ingress`, `broker_outbox`, `plugin_migrations`, `alembic_version`) | `SCENARIO_PIPELINER_DB_SCHEMA` (default `sp`) |
| Plugin-owned tables | sanitized `plugin_name` (e.g. `hello_scenario.events`) |

Schema names must match `^[a-z][a-z0-9_]*$`. Reserved: `public`, `information_schema`,
`pg_catalog`, and anything starting with `pg_`.

The first successful migrate writes `settings.db_schema` and
`settings.plugin_schema_<plugin_name>`. A later run with a different mapping raises.
Changing the env schema without a data migration can still create a **second empty
install** in the new schema (known limitation).

`tasks.type_task` defaults to `LINEAR`. Omitting it on ad-hoc INSERT does not
create a 1s cyclical task. Core schema is a single Alembic revision
`0001_core_tables` (all library tables and enums). Later changes need a new
revision with `ALTER`. `table.create(checkfirst=True)` is only for bootstrapping
missing tables. If `{schema}.alembic_version` still has `0002`–`0007` from an
older wheel, stamp `0001_core_tables` (do not re-run upgrade from empty).

`tasks.deleted_at` is a soft-delete marker. When set, the DB worker does not
lock the row (`lock_runnable_tasks`) and outbox reconcile skips it. Nothing in
the library writes `deleted_at` yet; host SQL can.

`created_at` / `updated_at` / `deleted_at` / `results.created_at` are
`timestamptz`. Cyclical `next_run_at` is written with `datetime.now(UTC)`.

While a task is `QUEUED`/`RUNNING`, the stock runner refreshes
`last_heartbeat_at` every `RUNNER_HEARTBEAT_INTERVAL_SECONDS`. Another worker may reclaim the row only after
`RUNNER_ZOMBIE_TASKS_TIMEOUT_MINUTES` without a heartbeat. Pause / kill-switch (`worker_enabled=0` or
`pipeline_active_{scenario}=0`) skips lock and cancels in-flight work as
`CANCELLED`. Process SIGTERM is different: wait
`RUNNER_SHUTDOWN_TIMEOUT_SECONDS`, then requeue leftover in-flight rows as
`NEW` with heartbeat cleared (no extra `results` row). A database error while reading those flags or touching heartbeats
cancels the in-flight batch (fail-closed).

Plugin SQL is rendered at apply time. Both `{{core_schema}}` and `{{plugin_schema}}`
must appear; leftover `{{` after render is an error. Manifest `postgresql` paths
must stay inside the plugin directory (same guards as dry-run: no absolute path,
no `../` escape).

Apply records `(plugin_name, migration_order, sha256 of the SQL file)` in
`{schema}.plugin_migrations`. An unchanged file is skipped on the next
`migrate-plugins` / `run`. Changing the SQL without bumping
`migrations.migration_order` is an error.

Statements are split on `;` outside quotes (`'…'` / `"…"`), `--` / `/* */` comments, and
dollar-quoted bodies (`$tag$…$tag$`). Do not put top-level `BEGIN` / `COMMIT`
in the file (apply already runs in a transaction). Example:

```sql
CREATE TABLE IF NOT EXISTS {{plugin_schema}}.events (
    id BIGSERIAL PRIMARY KEY,
    task_id BIGINT NULL REFERENCES {{core_schema}}.tasks(id)
);
```

On connect the library sets `search_path` to the **core** schema on both the
SQLAlchemy engine and the plugin asyncpg pool. Unqualified names resolve to core
tables (`tasks`, `settings`, …), not `public`. Expanding `search_path` to include
plugin schemas is **future work**; until then plugin SQL must qualify names with
`{{plugin_schema}}`.

Plugin schema is always `sanitize(plugin_name)` in this version. A later override
order (manifest > env > default) is **future work**.

`plugin_name` must be unique under `plugins_root`. Sanitized schema names must
also be unique: `foo-bar` and `foo.bar` both become `foo_bar`. Dry-run,
`db migrate-plugins`, and worker load all fail with
`PLUGIN_SCHEMA_CONFLICT` (duplicate names: `PLUGIN_NAME_CONFLICT`).

CLI: `--db-schema` on `migrate`, `db migrate-core`, and `db migrate-plugins`
(also `SCENARIO_PIPELINER_DB_SCHEMA`).

## 9. Exit codes

- `migrate --dry-run`: `0` if report status is `ok`/`warning`, `1` if `error`
- `db migrate-core` / `db migrate-plugins`: `0` on success, `2` on config validation errors
- `run`: `0` success, `130` interrupt, `1` runtime error
