from __future__ import annotations

from pathlib import Path

from scenario_pipeliner.api.config import ScenarioPipelinerConfig
from scenario_pipeliner.api.enums import (
    ChecksumReportScope,
    ChecksumStatus,
    DbBackend,
    DryRunErrorCode,
    DryRunStatus,
    LoadStatus,
)
from scenario_pipeliner.api.models import (
    ChecksumDryRunInfo,
    DryRunError,
    DryRunReport,
    DryRunSummary,
    MigrationPlanItem,
    PluginDryRunResult,
    PluginManifestV1,
)
from scenario_pipeliner.core.exceptions import DryRunPluginError
from scenario_pipeliner.core.manifest_loader import find_manifest_files, load_manifest
from scenario_pipeliner.core.registry import PluginRegistryV1


def build_dry_run_report(config: ScenarioPipelinerConfig) -> DryRunReport:
    registry = PluginRegistryV1()
    manifest_paths = find_manifest_files(config.plugins_root)
    plugin_results: list[PluginDryRunResult] = []
    errors: list[DryRunError] = []
    migration_plan_count = 0

    for manifest_path in manifest_paths:
        plugin_dir = manifest_path.parent
        manifest = None
        try:
            manifest = load_manifest(manifest_path)
            migration_path = _pick_backend_migration_path(
                backend=config.db_backend,
                plugin_dir=plugin_dir,
                sqlite_path=manifest.migrations.sqlite,
                postgresql_path=manifest.migrations.postgresql,
            )

            migration_plan = []
            planned_migrations_for_plugin = 0
            if migration_path is not None:
                if not migration_path.exists():
                    raise DryRunPluginError(
                        DryRunErrorCode.MIGRATION_PATH_MISSING,
                        f"migration file does not exist: {migration_path.as_posix()}",
                    )
                if not migration_path.is_file():
                    raise DryRunPluginError(
                        DryRunErrorCode.MIGRATION_PATH_NOT_FILE,
                        f"migration path must point to a file: {migration_path.as_posix()}",
                    )
                migration_plan.append(
                    MigrationPlanItem(
                        backend=config.db_backend,
                        path=migration_path.as_posix(),
                        order=manifest.migrations.migration_order,
                        plugin_name=manifest.plugin_name,
                    )
                )
                planned_migrations_for_plugin = 1

            register_result = registry.register(
                manifest,
                mode=config.mode,
                core_version=config.core_version,
                plugin_path=plugin_dir,
            )
            migration_plan_count += planned_migrations_for_plugin
            is_skipped = migration_path is None
            reasons = list(register_result.warnings)
            if is_skipped:
                reasons.append(f"no migration path for backend={config.db_backend}")

            plugin_results.append(
                PluginDryRunResult(
                    plugin_name=manifest.plugin_name,
                    plugin_version=manifest.plugin_version,
                    load_status=(
                        LoadStatus.SKIPPED if is_skipped else LoadStatus.LOADED
                    ),
                    reasons=reasons,
                    scenarios=manifest.scenarios,
                    checksum=ChecksumDryRunInfo(
                        status=register_result.checksum_status,
                        scope=ChecksumReportScope(manifest.checksum.scope),
                    ),
                    migration_plan=migration_plan,
                )
            )
        except DryRunPluginError as e:
            plugin_results.append(
                _build_error_plugin_result(
                    manifest=manifest,
                    manifest_path=manifest_path,
                    reason=str(e),
                )
            )
            errors.append(DryRunError(code=e.code, message=str(e)))
        except (ValueError, OSError) as e:
            plugin_results.append(
                _build_error_plugin_result(
                    manifest=manifest,
                    manifest_path=manifest_path,
                    reason=str(e),
                )
            )
            errors.append(
                DryRunError(
                    code=DryRunErrorCode.PLUGIN_LOAD_ERROR,
                    message=str(e),
                )
            )

    # Deterministic order for execution planning.
    for item in plugin_results:
        item.migration_plan.sort(key=lambda x: (x.order, x.plugin_name))
    plugin_results.sort(key=_plugin_result_sort_key)

    summary = DryRunSummary(
        plugins_discovered=len(manifest_paths),
        plugins_loaded=sum(
            1 for p in plugin_results if p.load_status == LoadStatus.LOADED
        ),
        plugins_skipped=sum(
            1 for p in plugin_results if p.load_status == LoadStatus.SKIPPED
        ),
        migrations_planned=migration_plan_count,
    )
    status: DryRunStatus = (
        DryRunStatus.ERROR
        if errors
        else DryRunStatus.WARNING
        if _has_warnings(plugin_results)
        else DryRunStatus.OK
    )

    return DryRunReport(
        status=status,
        mode=config.mode,
        db_backend=config.db_backend,
        core_version=config.core_version,
        summary=summary,
        plugins=plugin_results,
        errors=errors,
    )


def _pick_backend_migration_path(
    *,
    backend: DbBackend,
    plugin_dir: Path,
    sqlite_path: str | None,
    postgresql_path: str | None,
) -> Path | None:
    raw_path = sqlite_path if backend == DbBackend.SQLITE else postgresql_path
    if raw_path is None:
        return None
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


def _has_warnings(plugin_results: list[PluginDryRunResult]) -> bool:
    return any(
        plugin.reasons
        for plugin in plugin_results
        if plugin.load_status in {LoadStatus.LOADED, LoadStatus.SKIPPED}
    )


def _build_error_plugin_result(
    *,
    manifest: PluginManifestV1 | None,
    manifest_path: Path,
    reason: str,
) -> PluginDryRunResult:
    plugin_name = (
        manifest.plugin_name if manifest is not None else manifest_path.parent.name
    )
    plugin_version = manifest.plugin_version if manifest is not None else "unknown"
    return PluginDryRunResult(
        plugin_name=plugin_name,
        plugin_version=plugin_version,
        load_status=LoadStatus.ERROR,
        reasons=[reason],
        scenarios=[],
        checksum=ChecksumDryRunInfo(
            status=ChecksumStatus.NOT_CHECKED,
            scope=ChecksumReportScope.UNKNOWN,
        ),
        migration_plan=[],
    )


def _plugin_result_sort_key(item: PluginDryRunResult) -> tuple[int, str, str]:
    if item.migration_plan:
        return (0, item.migration_plan[0].order, item.plugin_name)
    return (1, "99999999999999", item.plugin_name)
