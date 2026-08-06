from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import asyncpg
from pydantic import ValidationError

from scenario_pipeliner.api.config import CoreMigrationConfig, ScenarioPipelinerConfig
from scenario_pipeliner.api.core_migrate import apply_core_migrations
from scenario_pipeliner.api.enums import DbBackend, Mode
from scenario_pipeliner.api.migrate import apply_migrations
from scenario_pipeliner.core.plugin_checksum import (
    compute_plugin_checksum,
    write_checksum_to_manifest,
)
from scenario_pipeliner.core.plugin_migrate import apply_plugin_migrations_async
from scenario_pipeliner.env_loader import load_environment_file
from scenario_pipeliner.worker.core.custom_settings import PostgreSQLClientSettings


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
        choices=["sqlite", "postgresql"],
        required=True,
    )
    migrate_parser.add_argument(
        "--core-version",
        default=os.getenv("SCENARIO_PIPELINER_CORE_VERSION", "0.1.0"),
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
        default=os.getenv("SCENARIO_PIPELINER_CORE_VERSION", "0.1.0"),
    )

    run_parser = subparsers.add_parser(
        "run",
        help="Bootstrap Postgres worker: migrations (optional), execute existing tasks",
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
    return parser


def _add_db_backend_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--db-backend",
        choices=["sqlite", "postgresql"],
        required=True,
    )
    parser.add_argument(
        "--sqlite-path",
        default=os.getenv("SCENARIO_PIPELINER_SQLITE_PATH"),
        help="SQLite DB path (required for sqlite backend)",
    )
    parser.add_argument(
        "--postgres-host",
        default=os.getenv("POSTGRES_HOST"),
    )
    parser.add_argument(
        "--postgres-port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", "5432")),
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


def _plugins_root() -> Path:
    return Path(os.getenv("SCENARIO_PIPELINER_PLUGINS_ROOT", "plugins"))


async def _migrate_plugins(args: argparse.Namespace) -> int:
    config = ScenarioPipelinerConfig(
        mode=Mode(args.mode),
        db_backend=DbBackend(args.db_backend),
        plugins_root=_plugins_root(),
        core_version=args.core_version,
    )
    try:
        postgres = PostgreSQLClientSettings.model_validate(dict(os.environ))
    except ValidationError as e:
        sys.stderr.write(f"{e}\n")
        return 2

    pool = await asyncpg.create_pool(
        dsn=postgres.sync_url,
        min_size=postgres.DB_POOL_MIN_SIZE,
        max_size=postgres.DB_POOL_MAX_SIZE,
    )
    try:
        applied = await apply_plugin_migrations_async(config, pool)
    finally:
        await pool.close()

    payload = {
        "status": "ok",
        "db_backend": args.db_backend,
        "plugins_root": config.plugins_root.as_posix(),
        "applied_plugins": applied,
    }
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, indent=2))
    sys.stdout.write("\n")
    return 0


async def _run_worker(*, skip_migrations: bool) -> int:
    """Start the Postgres worker via RunnerApp.

    RunnerApp is imported lazily so lighter CLI commands (migrate, db, plugin)
    do not pay the worker-runtime import cost on every invocation.
    """
    if skip_migrations:
        os.environ["RUNNER_APPLY_MIGRATIONS"] = "false"
    from scenario_pipeliner.worker.runtime import RunnerApp

    app = await RunnerApp.from_env()
    await app.run()
    return 0


def main(argv: list[str] | None = None) -> int:
    # Load before argparse defaults (os.getenv) and pydantic-settings are resolved.
    load_environment_file()
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "migrate":
        if not args.dry_run:
            parser.error("v0 supports only --dry-run mode")

        config = ScenarioPipelinerConfig(
            mode=Mode(args.mode),
            db_backend=DbBackend(args.db_backend),
            plugins_root=_plugins_root(),
            core_version=args.core_version,
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
                postgres_host=args.postgres_host,
                postgres_port=args.postgres_port,
                postgres_db=args.postgres_db,
                postgres_user=args.postgres_user,
                postgres_password=args.postgres_password,
                sqlite_path=Path(args.sqlite_path) if args.sqlite_path else None,
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
            return asyncio.run(_run_worker(skip_migrations=args.skip_migrations))
        except KeyboardInterrupt:
            return 130
        except Exception as exc:
            sys.stderr.write(f"ERROR: {type(exc).__name__}: {exc}\n")
            return 1

    if args.command == "plugin" and args.plugin_command == "checksum":
        return _plugin_checksum(args.plugin_dir, write=args.write)

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


if __name__ == "__main__":
    raise SystemExit(main())
