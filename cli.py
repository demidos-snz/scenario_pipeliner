from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from pydantic import ValidationError

from scenario_pipeliner.api.config import CoreMigrationConfig, ScenarioPipelinerConfig
from scenario_pipeliner.api.core_migrate import apply_core_migrations
from scenario_pipeliner.api.enums import DbBackend, Mode
from scenario_pipeliner.api.migrate import apply_migrations


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="scenario_pipeliner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    migrate_parser = subparsers.add_parser("migrate", help="Migration operations")
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
    migrate_core_parser.add_argument(
        "--db-backend",
        choices=["sqlite", "postgresql"],
        required=True,
    )
    migrate_core_parser.add_argument(
        "--sqlite-path",
        default=os.getenv("SCENARIO_PIPELINER_SQLITE_PATH"),
        help="SQLite DB path (required for sqlite backend)",
    )
    migrate_core_parser.add_argument(
        "--postgres-host",
        default=os.getenv("POSTGRES_HOST"),
    )
    migrate_core_parser.add_argument(
        "--postgres-port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", "5432")),
    )
    migrate_core_parser.add_argument(
        "--postgres-db",
        default=os.getenv("POSTGRES_DB"),
    )
    migrate_core_parser.add_argument(
        "--postgres-user",
        default=os.getenv("POSTGRES_USER"),
    )
    migrate_core_parser.add_argument(
        "--postgres-password",
        default=os.getenv("POSTGRES_PASSWORD"),
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "migrate":
        if not args.dry_run:
            parser.error("v0 supports only --dry-run mode")

        config = ScenarioPipelinerConfig(
            mode=Mode(args.mode),
            db_backend=DbBackend(args.db_backend),
            plugins_root=Path(os.getenv("SCENARIO_PIPELINER_PLUGINS_ROOT", "plugins")),
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

    parser.error(f"unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
