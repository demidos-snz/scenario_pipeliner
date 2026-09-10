from __future__ import annotations

from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from scenario_pipeliner.api.settings import ScenarioPipelinerConfig
from scenario_pipeliner.core.checksum import sha256_file
from scenario_pipeliner.core.manifest_loader import find_manifest_files, load_manifest
from scenario_pipeliner.core.migration_paths import resolve_plugin_migration_path
from scenario_pipeliner.core.plugin_identity import claim_plugin_identity
from scenario_pipeliner.db.engine import ensure_postgres_schema
from scenario_pipeliner.db.exceptions import PluginSqlTemplateError
from scenario_pipeliner.db.schema_utils import (
    check_plugin_migration_replay,
    get_plugin_migration,
    persist_plugin_schema,
    record_plugin_migration,
)
from scenario_pipeliner.db.schemes_names import plugin_schema_name, validate_schema_name
from scenario_pipeliner.db.sql_templates import render_plugin_sql, split_sql_statements
from scenario_pipeliner.db.tables import build_core_schema


def collect_plugin_migration_plans(
    config: ScenarioPipelinerConfig,
) -> list[tuple[str, str, Path]]:
    """Return sorted (order, plugin_name, sql_path) for postgresql."""
    plans: list[tuple[str, str, Path]] = []
    seen_names: set[str] = set()
    schema_owners: dict[str, str] = {}
    for manifest_path in find_manifest_files(config.plugins_root):
        manifest = load_manifest(manifest_path)
        claim_plugin_identity(
            plugin_name=manifest.plugin_name,
            seen_names=seen_names,
            schema_owners=schema_owners,
        )
        if manifest.migrations is None:
            continue
        relative = manifest.migrations.postgresql
        if not relative:
            continue
        path = resolve_plugin_migration_path(
            plugin_dir=manifest_path.parent, raw_path=relative
        )
        plans.append((manifest.migrations.migration_order, manifest.plugin_name, path))
    plans.sort(key=lambda item: (item[0], item[1]))
    return plans


async def apply_plugin_migrations_async(
    config: ScenarioPipelinerConfig,
    engine: AsyncEngine,
) -> list[str]:
    """Apply plugin SQL migrations in manifest order. Returns applied plugin names."""
    core_schema = validate_schema_name(config.db_schema)
    core = build_core_schema(core_schema)
    applied: list[str] = []

    for order, plugin_name, path in collect_plugin_migration_plans(config):
        if not path.exists():
            raise RuntimeError(
                f"plugin migration not found for {plugin_name}: {path.as_posix()}"
            )
        plugin_schema = plugin_schema_name(plugin_name)
        sql_sha256 = sha256_file(path)

        try:
            sql = render_plugin_sql(
                path.read_text(encoding="utf-8"),
                core_schema=core_schema,
                plugin_schema=plugin_schema,
            )
        except PluginSqlTemplateError as exc:
            raise PluginSqlTemplateError(
                f"{plugin_name} ({path.as_posix()}): {exc}"
            ) from exc
        statements = split_sql_statements(sql)

        async with engine.begin() as conn:
            await persist_plugin_schema(
                conn,
                core,
                plugin_name=plugin_name,
                plugin_schema=plugin_schema,
            )
            existing = await get_plugin_migration(conn, core, plugin_name)
            if check_plugin_migration_replay(
                plugin_name=plugin_name,
                migration_order=order,
                sql_sha256=sql_sha256,
                existing=existing,
            ):
                continue
            await ensure_postgres_schema(conn, plugin_schema)
            for statement in statements:
                await conn.execute(text(statement))
            await record_plugin_migration(
                conn,
                core,
                plugin_name=plugin_name,
                migration_order=order,
                sql_sha256=sql_sha256,
            )
        applied.append(plugin_name)
    return applied
