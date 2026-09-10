"""Shared plugin SQL path guards (dry-run and apply)."""

from __future__ import annotations

from pathlib import Path

from scenario_pipeliner.api.enums import DryRunErrorCode
from scenario_pipeliner.core.exceptions import DryRunPluginError


def resolve_plugin_migration_path(*, plugin_dir: Path, raw_path: str) -> Path:
    """Resolve a manifest SQL path; reject absolute paths and directory escape."""
    if Path(raw_path).is_absolute():
        raise DryRunPluginError(
            DryRunErrorCode.MIGRATION_PATH_ABSOLUTE,
            f"absolute migration path is not allowed: {raw_path}",
        )
    resolved_plugin_dir = plugin_dir.resolve()
    migration_path = (plugin_dir / raw_path).resolve()
    if (
        migration_path != resolved_plugin_dir
        and resolved_plugin_dir not in migration_path.parents
    ):
        raise DryRunPluginError(
            DryRunErrorCode.MIGRATION_PATH_TRAVERSAL,
            f"migration path escapes plugin directory: {raw_path}",
        )
    return migration_path
