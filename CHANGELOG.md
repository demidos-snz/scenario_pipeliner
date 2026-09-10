# Changelog

All notable changes to `scenario_pipeliner` are documented in this file.

The format is inspired by [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
