from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

from pydantic import ValidationError

from scenario_pipeliner.api.core_migrate import apply_core_migrations
from scenario_pipeliner.api.enums import DbBackend, Mode
from scenario_pipeliner.api.migrate import apply_migrations
from scenario_pipeliner.api.settings import CoreMigrationConfig, ScenarioPipelinerConfig
from scenario_pipeliner.core.manifest_loader import list_plugin_scenarios
from scenario_pipeliner.core.plugin_checksum import (
    compute_plugin_checksum,
    write_checksum_to_manifest,
)
from scenario_pipeliner.core.plugin_init import (
    DEFAULT_CORE_COMPAT,
    PluginInitError,
    init_plugin,
)
from scenario_pipeliner.core.plugin_migrate import apply_plugin_migrations_async
from scenario_pipeliner.db.engine import create_async_engine_for_schema
from scenario_pipeliner.db.schemes_names import DEFAULT_DB_SCHEMA, validate_schema_name
from scenario_pipeliner.env_loader import load_environment_file
from scenario_pipeliner.version import __version__
from scenario_pipeliner.worker.core.custom_settings import PostgreSQLClientSettings
from scenario_pipeliner.worker.runtime import RunnerApp


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scenario_pipeliner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    migrate_parser = subparsers.add_parser(
        "migrate",
        help="Plugin migration plan (dry-run)",
    )
    migrate_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only build migration plan report",
    )
    migrate_parser.add_argument(
        "--format",
        default="json",
        choices=["json"],
        help="Report output format",
    )
    migrate_parser.add_argument(
        "--mode",
        choices=["dev", "prod"],
        default=os.getenv("SCENARIO_PIPELINER_MODE", "dev"),
    )
    migrate_parser.add_argument(
        "--db-backend",
        choices=["postgresql"],
        default="postgresql",
    )
    migrate_parser.add_argument(
        "--db-schema",
        default=os.getenv("SCENARIO_PIPELINER_DB_SCHEMA", DEFAULT_DB_SCHEMA),
        help="PostgreSQL schema for core tables (default: sp)",
    )
    migrate_parser.add_argument(
        "--core-version",
        default=os.getenv("SCENARIO_PIPELINER_CORE_VERSION", __version__),
    )

    db_parser = subparsers.add_parser("db", help="Database operations")
    db_subparsers = db_parser.add_subparsers(dest="db_command", required=True)
    migrate_core_parser = db_subparsers.add_parser(
        "migrate-core",
        help="Create core DB tables and seed default settings",
    )
    _add_db_backend_args(migrate_core_parser)

    migrate_plugins_parser = db_subparsers.add_parser(
        "migrate-plugins",
        help="Apply plugin SQL migrations from SCENARIO_PIPELINER_PLUGINS_ROOT",
    )
    migrate_plugins_parser.add_argument(
        "--db-backend",
        choices=["postgresql"],
        default="postgresql",
        help="v0 plugin apply supports postgresql only",
    )
    migrate_plugins_parser.add_argument(
        "--mode",
        choices=["dev", "prod"],
        default=os.getenv("SCENARIO_PIPELINER_MODE", "dev"),
    )
    migrate_plugins_parser.add_argument(
        "--core-version",
        default=os.getenv("SCENARIO_PIPELINER_CORE_VERSION", __version__),
    )
    migrate_plugins_parser.add_argument(
        "--db-schema",
        default=os.getenv("SCENARIO_PIPELINER_DB_SCHEMA", DEFAULT_DB_SCHEMA),
        help="PostgreSQL schema for core tables (default: sp)",
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Bootstrap Postgres worker: migrations (optional), execute existing tasks",
    )
    run_parser.add_argument(
        "--mode",
        choices=["db", "broker", "all"],
        default=None,
        help="Runner mode (default: env SCENARIO_PIPELINER_RUNNER_MODE or db)",
    )
    run_parser.add_argument(
        "--skip-migrations",
        action="store_true",
        help="Do not apply core/plugin migrations (use after db migrate-*)",
    )

    plugin_parser = subparsers.add_parser("plugin", help="Plugin authoring helpers")
    plugin_subparsers = plugin_parser.add_subparsers(
        dest="plugin_command",
        required=True,
    )
    checksum_parser = plugin_subparsers.add_parser(
        "checksum",
        help="Compute unpacked sha256 for a plugin directory",
    )
    checksum_parser.add_argument(
        "plugin_dir",
        type=Path,
        help="Path to plugin package directory (contains plugin.manifest.json)",
    )
    checksum_parser.add_argument(
        "--write",
        action="store_true",
        help="Write checksum into existing plugin.manifest.json",
    )
    init_parser = plugin_subparsers.add_parser(
        "init",
        help="Scaffold plugin.manifest.json, package stubs, and checksum",
    )
    init_parser.add_argument(
        "plugin_dir",
        type=Path,
        help="Plugin package directory (name becomes plugin_name / import path)",
    )
    init_parser.add_argument(
        "--if-exists",
        choices=["fail", "overwrite", "checksum-only"],
        default="fail",
        help="fail (default), overwrite stubs+manifest, or refresh checksum only",
    )
    init_parser.add_argument(
        "--core-compat",
        default=os.getenv("SCENARIO_PIPELINER_CORE_COMPAT", DEFAULT_CORE_COMPAT),
        help=f"Manifest core_compat (default: {DEFAULT_CORE_COMPAT})",
    )
    init_parser.add_argument(
        "--no-migrations",
        action="store_true",
        help="Do not add migrations section or migration.sql",
    )

    task_parser = subparsers.add_parser(
        "task",
        help="Create tasks and list scenarios from plugins_root",
    )
    task_subparsers = task_parser.add_subparsers(dest="task_command", required=True)
    task_subparsers.add_parser(
        "list",
        help="List scenarios from plugin manifests (no DB)",
    )
    create_parser = task_subparsers.add_parser(
        "create",
        help="Insert a NEW task for a discovered scenario",
    )
    create_parser.add_argument(
        "--scenario",
        required=True,
        help="Scenario key from `task list` / plugin.manifest.json",
    )
    create_parser.add_argument(
        "--type",
        dest="type_task",
        choices=["LINEAR", "CYCLICAL"],
        default="LINEAR",
    )
    create_parser.add_argument(
        "--interval-seconds",
        type=int,
        default=1,
        help="Stored on the row; used when type is CYCLICAL",
    )
    create_parser.add_argument(
        "--max-executions",
        type=int,
        default=None,
        help="Optional cap (typical for CYCLICAL)",
    )
    create_parser.add_argument("--alias", default=None)
    create_parser.add_argument(
        "--payload",
        default=None,
        help="JSON object for TaskPayload (default: empty validated payload)",
    )
    create_parser.add_argument(
        "--steps-name",
        action="append",
        default=None,
        dest="steps_names",
        help="Pipeline step name snapshot (repeatable)",
    )
    create_parser.add_argument(
        "--allow-unknown-scenario",
        action="store_true",
        help="Insert even if the scenario is not in plugins_root manifests",
    )
    create_parser.add_argument(
        "--db-schema",
        default=os.getenv("SCENARIO_PIPELINER_DB_SCHEMA", DEFAULT_DB_SCHEMA),
        help="PostgreSQL schema for core tables (default: sp)",
    )
    return parser


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    stripped = raw.strip()
    if not stripped:
        return default
    try:
        return int(stripped)
    except ValueError as exc:
        raise ValueError(f"{name}={raw!r} is not an integer") from exc


def _add_db_backend_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--db-backend",
        choices=["postgresql"],
        default="postgresql",
    )
    parser.add_argument(
        "--postgres-url",
        default=os.getenv("POSTGRES_URL"),
        help=(
            "PostgreSQL URL (postgresql://, postgresql+asyncpg://, "
            "postgresql+psycopg://). If missing or invalid, "
            "POSTGRES_* / --postgres-* are used."
        ),
    )
    parser.add_argument(
        "--postgres-host",
        default=os.getenv("POSTGRES_HOST"),
    )
    parser.add_argument(
        "--postgres-port",
        type=int,
        default=_env_int("POSTGRES_PORT", 5432),
    )
    parser.add_argument(
        "--postgres-db",
        default=os.getenv("POSTGRES_DB"),
    )
    parser.add_argument(
        "--postgres-user",
        default=os.getenv("POSTGRES_USER"),
    )
    parser.add_argument(
        "--postgres-password",
        default=os.getenv("POSTGRES_PASSWORD"),
    )
    parser.add_argument(
        "--db-schema",
        default=os.getenv("SCENARIO_PIPELINER_DB_SCHEMA", DEFAULT_DB_SCHEMA),
        help="PostgreSQL schema for core tables (default: sp)",
    )


def _plugins_root() -> Path:
    return Path(os.getenv("SCENARIO_PIPELINER_PLUGINS_ROOT", "plugins"))


async def _migrate_plugins(args: argparse.Namespace) -> int:
    config = ScenarioPipelinerConfig(
        mode=Mode(args.mode),
        db_backend=DbBackend(args.db_backend),
        plugins_root=_plugins_root(),
        core_version=args.core_version,
        db_schema=validate_schema_name(args.db_schema),
    )
    try:
        postgres = PostgreSQLClientSettings.model_validate(dict(os.environ))
        async_url = postgres.async_url
    except (ValidationError, ValueError) as e:
        sys.stderr.write(f"{e}\n")
        return 2

    engine = create_async_engine_for_schema(
        async_url,
        config.db_schema,
        **postgres.pool_kwargs,
    )
    try:
        applied = await apply_plugin_migrations_async(config, engine)
    finally:
        await engine.dispose()

    payload = {
        "status": "ok",
        "db_backend": args.db_backend,
        "plugins_root": config.plugins_root.as_posix(),
        "applied_plugins": applied,
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
    sys.stdout.write("\n")
    return 0


async def _run_worker(*, skip_migrations: bool, mode: str | None) -> int:
    """Start the Postgres worker via RunnerApp.

    RunnerApp is imported lazily so lighter CLI commands (migrate, db, plugin)
    do not pay the worker-runtime import cost on every invocation.
    """
    if skip_migrations:
        os.environ["RUNNER_APPLY_MIGRATIONS"] = "false"
    if mode:
        os.environ["SCENARIO_PIPELINER_RUNNER_MODE"] = mode

    app = await RunnerApp.from_env()
    await app.run()
    return 0


def main(argv: list[str] | None = None) -> int:
    # Load before argparse defaults (os.getenv) and pydantic-settings are resolved.
    load_environment_file()
    try:
        parser = build_parser()
        args = parser.parse_args(argv)
        return _dispatch(parser, args)
    except NotImplementedError as exc:
        sys.stderr.write(f"ERROR: {exc}\n")
        return 1
    except ValueError as exc:
        sys.stderr.write(f"ERROR: {exc}\n")
        return 2


def _dispatch(parser: argparse.ArgumentParser, args: argparse.Namespace) -> int:

    if args.command == "migrate":
        if not args.dry_run:
            parser.error("v0 supports only --dry-run mode")

        config = ScenarioPipelinerConfig(
            mode=Mode(args.mode),
            db_backend=DbBackend(args.db_backend),
            plugins_root=_plugins_root(),
            core_version=args.core_version,
            db_schema=validate_schema_name(args.db_schema),
        )
        dry_run_report = apply_migrations(config, dry_run=True)
        payload = dry_run_report.model_dump(mode="json")
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
        sys.stdout.write("\n")
        return 0 if dry_run_report.status != "error" else 1

    if args.command == "db" and args.db_command == "migrate-core":
        try:
            core_migration_config = CoreMigrationConfig(
                db_backend=DbBackend(args.db_backend),
                db_schema=validate_schema_name(args.db_schema),
                postgres_url=args.postgres_url,
                postgres_host=args.postgres_host,
                postgres_port=args.postgres_port,
                postgres_db=args.postgres_db,
                postgres_user=args.postgres_user,
                postgres_password=args.postgres_password,
            )
        except ValidationError as e:
            sys.stderr.write(f"{e}\n")
            return 2

        core_migration_report = apply_core_migrations(core_migration_config)
        payload = core_migration_report.model_dump(mode="json")
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
        sys.stdout.write("\n")
        return 0

    if args.command == "db" and args.db_command == "migrate-plugins":
        return asyncio.run(_migrate_plugins(args))

    if args.command == "run":
        try:
            return asyncio.run(
                _run_worker(skip_migrations=args.skip_migrations, mode=args.mode)
            )
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            sys.stderr.write(f"ERROR: {type(exc).__name__}: {exc}\n")
            return 1

    if args.command == "plugin" and args.plugin_command == "checksum":
        return _plugin_checksum(args.plugin_dir, write=args.write)

    if args.command == "plugin" and args.plugin_command == "init":
        return _plugin_init(args)

    if args.command == "task" and args.task_command == "list":
        return _task_list()

    if args.command == "task" and args.task_command == "create":
        return asyncio.run(_task_create(args))

    parser.error(f"unsupported command: {args.command}")
    return 2


def _plugin_checksum(plugin_dir: Path, *, write: bool) -> int:
    try:
        if write:
            manifest_path, value = write_checksum_to_manifest(plugin_dir)
            payload = {
                "status": "ok",
                "plugin_dir": plugin_dir.expanduser().resolve().as_posix(),
                "manifest_path": manifest_path.as_posix(),
                "algorithm": "sha256",
                "scope": "unpacked",
                "value": value,
                "written": True,
            }
        else:
            value = compute_plugin_checksum(plugin_dir)
            payload = {
                "status": "ok",
                "plugin_dir": plugin_dir.expanduser().resolve().as_posix(),
                "algorithm": "sha256",
                "scope": "unpacked",
                "value": value,
                "written": False,
            }
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"ERROR: {exc}\n")
        return 2

    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
    sys.stdout.write("\n")
    return 0


def _plugin_init(args: argparse.Namespace) -> int:
    try:
        payload = init_plugin(
            args.plugin_dir,
            if_exists=args.if_exists,
            core_compat=args.core_compat,
            include_migrations=not args.no_migrations,
        )
    except PluginInitError as exc:
        sys.stderr.write(f"ERROR: {exc}\n")
        return 2
    except (FileNotFoundError, ValueError, json.JSONDecodeError) as exc:
        sys.stderr.write(f"ERROR: {exc}\n")
        return 2
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
    sys.stdout.write("\n")
    return 0


def _task_list() -> int:
    root = _plugins_root().expanduser().resolve()
    scenarios, errors = list_plugin_scenarios(root)
    payload = {
        "status": "ok" if not errors else "warning",
        "plugins_root": root.as_posix(),
        "scenarios": scenarios,
        "errors": errors,
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
    sys.stdout.write("\n")
    return 0 if not errors else 1


async def _task_create(args: argparse.Namespace) -> int:
    from scenario_pipeliner.db.exceptions import CoreSchemaMissingError
    from scenario_pipeliner.db.schema_utils import require_core_tables
    from scenario_pipeliner.db.settings import CoreDb
    from scenario_pipeliner.db.tables import build_core_schema
    from scenario_pipeliner.worker.core.enums import TaskType
    from scenario_pipeliner.worker.runtime.runner_helpers import TaskSeed, create_task

    if args.interval_seconds < 1:
        sys.stderr.write("ERROR: --interval-seconds must be >= 1\n")
        return 2

    root = _plugins_root().expanduser().resolve()
    scenarios, _manifest_errors = list_plugin_scenarios(root)
    known = {item["scenario"] for item in scenarios}
    if args.scenario not in known and not args.allow_unknown_scenario:
        available = ", ".join(sorted(known)) or "(none — run plugin init or task list)"
        sys.stderr.write(
            f"ERROR: unknown scenario {args.scenario!r}. "
            f"Known: {available}. Use --allow-unknown-scenario to insert anyway.\n"
        )
        return 2

    payload_obj: dict[str, object] | None = None
    if args.payload is not None:
        try:
            loaded = json.loads(args.payload)
        except json.JSONDecodeError as exc:
            sys.stderr.write(f"ERROR: --payload is not valid JSON: {exc}\n")
            return 2
        if not isinstance(loaded, dict):
            sys.stderr.write("ERROR: --payload must be a JSON object\n")
            return 2
        payload_obj = loaded

    try:
        postgres = PostgreSQLClientSettings.model_validate(dict(os.environ))
        schema = validate_schema_name(args.db_schema)
        async_url = postgres.async_url
    except (ValidationError, ValueError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 2

    engine = create_async_engine_for_schema(
        async_url,
        schema,
        **postgres.pool_kwargs,
    )
    db = CoreDb(engine=engine, core=build_core_schema(schema))
    try:
        await require_core_tables(db)
        seed = TaskSeed(
            scenario=args.scenario,
            type_task=TaskType(args.type_task),
            interval_seconds=args.interval_seconds,
            max_executions=args.max_executions,
            alias=args.alias,
            steps_names=tuple(args.steps_names or ()),
            payload=payload_obj,
        )
        task_id = await create_task(db=db, seed=seed)
    except CoreSchemaMissingError as exc:
        sys.stderr.write(f"ERROR: {exc}\n")
        return 2
    except ValidationError as exc:
        sys.stderr.write(f"ERROR: {exc}\n")
        return 2
    finally:
        await engine.dispose()

    result = {
        "status": "ok",
        "task_id": task_id,
        "scenario": args.scenario,
        "type_task": args.type_task,
        "db_schema": schema,
    }
    sys.stdout.write(json.dumps(result, ensure_ascii=False, indent=2))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
