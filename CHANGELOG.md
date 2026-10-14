# Changelog

All notable changes to `scenario_pipeliner` are documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.0.2] - 2026-10-14

### Fixed
- A blocking child that failed before the parent reached `WAITING` fails the
  parent on the root's successful persist and cancels open siblings in that
  transaction.
- `mark_task_running` updates only a `QUEUED` row, so a cancel that already
  landed is not overwritten by `RUNNING`.
- Ingress persist errors and hook retries share `max_ingress_retries`. A
  classic queue without `x-delivery-count` or `x-death` stops on the redelivery.
- A dead broker task stops the process (`scenario_pipeliner run` exits 1).
- Soft-deleted children are omitted from parent rollup and sibling cancel.
- Pausing one scenario cancels only that scenario's in-flight tasks.
- Any active-flag poller error cancels the batch. Heartbeats are not skipped
  while the batch keeps running.
- Outbox completion requires the `claim_token` from the claim. A stale worker
  cannot mark the row sent or return it to `PENDING`.
- Cyclical success requeues until `max_executions`. Failure and cancel stay
  terminal.
- CI and publish jobs install Python 3.13. `greenlet` is a direct dependency,
  so the async SQLAlchemy engine imports even when the platform marker skips it.
- PostgreSQL enums are created with an explicit schema. `inherit_schema=True`
  is not combined with that schema, which SQLAlchemy 2.1 rejects.

### Added
- `AsyncClient.ping`. The broker loop pings before reconcile and reconnects
  with QoS and consumers after a failed ping (`PING_FAILURE_LIMIT`, default 3).
- Bounded ingress handler timeout, Postgres connect/command timeouts, and
  `statement_timeout` on both pools.
- Alembic `0002_outbox_claim_token` (`broker_outbox.claim_token`).
- Status map: `docs/TASK_STATUS.md`, `docs/TASK_STATUS.ru.md`.

### Changed
- Package version `0.0.2`. `plugin init` `core_compat` default is
  `>=0.0.2,<1.0`.
- Runner numeric defaults are shared constants. `POLL_INTERVAL_SECONDS`
  default is 2. Configured timeouts are `int`.
- `tasks.interval_seconds` server default, `task create --interval-seconds`,
  and task seeds share `DEFAULT_INTERVAL_SECONDS` (`1`).

## [0.0.1] - 2026-09-10

First public PyPI release. Plugins are **not** bundled in the wheel.

### Added
- Plugin discovery from `plugin.manifest.json`: `core_compat`, unpacked checksum,
  optional `migrations.postgresql`.
- Dry-run migration plan (`scenario_pipeliner migrate --dry-run`,
  `apply_migrations`).
- Core PostgreSQL schema (default `sp`, `SCENARIO_PIPELINER_DB_SCHEMA`) via
  Alembic revision `0001_core_tables`: `tasks`, `settings`, `results`,
  `broker_ingress`, `broker_outbox`, `plugin_migrations`.
- Plugin SQL apply (`db migrate-plugins`) with `{{core_schema}}` /
  `{{plugin_schema}}`, path guards, and a sha256 ledger.
- DB worker (`scenario_pipeliner run`): poll, heartbeats, kill-switches,
  parent/subtask `WAITING` → `FINISHED` / `FINISHED_WITH_ERROR`, cyclical
  requeue, shutdown drain (in-flight → `NEW`; pause → `CANCELLED`).
- Broker runner (`run --mode broker`): RabbitMQ `queue.consume` ingress and
  exclusive outbox publish.
- Authoring CLI: `plugin init`, `plugin checksum`, `task list`, `task create`.
- Public API: `ScenarioPipelinerConfig`, `apply_migrations`,
  `apply_core_migrations` / `apply_core_migrations_async`, `__version__`.
- Optional extra: `pip install 'scenario-pipeliner[sentry]'`.

### Notes
- PostgreSQL only. SQLite is not a library backend (client/settings remain as
  unused skeletons).
- `core_version` and `plugin init` `core_compat` default from `__version__`
  (`>=0.0.1,<1.0`).
- `SCENARIO_PIPELINER_MODE` default is `dev` (checksum mismatch is a warning;
  `prod` fails closed).
- `RabbitMQSettings` is connection-only (`RABBITMQ_URL`, `RABBITMQ_VHOST`);
  queue names come from the plugin broker contract.
