from __future__ import annotations

from pathlib import Path
from typing import Any

from scenario_pipeliner.api.config import ScenarioPipelinerConfig
from scenario_pipeliner.core.manifest_loader import find_manifest_files, load_manifest


def collect_plugin_migration_plans(
    config: ScenarioPipelinerConfig,
) -> list[tuple[str, str, Path]]:
    """Return sorted (order, plugin_name, sql_path) for the selected DB backend."""
    plans: list[tuple[str, str, Path]] = []
    backend = config.db_backend.value
    for manifest_path in find_manifest_files(config.plugins_root):
        manifest = load_manifest(manifest_path)
        if manifest.migrations is None:
            continue
        relative = getattr(manifest.migrations, backend, None)
        if not relative:
            continue
        path = (manifest_path.parent / relative).resolve()
        plans.append((manifest.migrations.migration_order, manifest.plugin_name, path))
    plans.sort(key=lambda item: (item[0], item[1]))
    return plans


async def apply_plugin_migrations_async(
    config: ScenarioPipelinerConfig,
    pool: Any,
) -> list[str]:
    """Apply plugin SQL migrations in manifest order. Returns applied plugin names."""
    applied: list[str] = []
    for _order, plugin_name, path in collect_plugin_migration_plans(config):
        if not path.exists():
            raise RuntimeError(
                f"plugin migration not found for {plugin_name}: {path.as_posix()}"
            )
        sql = path.read_text(encoding="utf-8")
        async with pool.acquire() as conn:
            await conn.execute(sql)
        applied.append(plugin_name)
    return applied
