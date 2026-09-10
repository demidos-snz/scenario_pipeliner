from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

from scenario_pipeliner.core.manifest_loader import MANIFEST_FILE_NAME
from scenario_pipeliner.core.plugin_checksum import write_checksum_to_manifest
from scenario_pipeliner.version import __version__

_PACKAGE_NAME = re.compile(r"^[a-z][a-z0-9_]*$")
DEFAULT_CORE_COMPAT = f">={__version__},<1.0"
_PLACEHOLDER_CHECKSUM = "0" * 64

IfExists = Literal["fail", "overwrite", "checksum-only"]

_STUB_FILES = (
    "__init__.py",
    "plugin.py",
    "pipeline.py",
    "settings.py",
    "steps.py",
    "states.py",
    "migration.sql",
    MANIFEST_FILE_NAME,
)


class PluginInitError(ValueError):
    """Raised when plugin scaffold cannot proceed."""


def plugin_package_name(plugin_dir: Path) -> str:
    name = plugin_dir.expanduser().resolve().name
    if not _PACKAGE_NAME.fullmatch(name):
        raise PluginInitError(
            f"plugin directory name {name!r} must match {_PACKAGE_NAME.pattern} "
            "(importable package; use underscores, not hyphens)"
        )
    return name


def _class_prefix(plugin_name: str) -> str:
    return "".join(part.capitalize() for part in plugin_name.split("_"))


def _migration_order() -> str:
    return datetime.now(UTC).strftime("%Y%m%d%H%M%S")


def _manifest_payload(
    *,
    plugin_name: str,
    core_compat: str,
    include_migrations: bool,
    migration_order: str,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "plugin_name": plugin_name,
        "plugin_version": "0.1.0",
        "plugin_api_version": "v1",
        "core_compat": core_compat,
        "checksum": {
            "algorithm": "sha256",
            "scope": "unpacked",
            "value": _PLACEHOLDER_CHECKSUM,
        },
        "entrypoint": f"{plugin_name}.plugin:register",
        "scenarios": [f"{plugin_name}.ping"],
    }
    if include_migrations:
        payload["migrations"] = {
            "migration_order": migration_order,
            "postgresql": "migration.sql",
        }
    return payload


def _stub_texts(*, plugin_name: str) -> dict[str, str]:
    prefix = _class_prefix(plugin_name)
    scenario = f"{plugin_name}.ping"
    return {
        "__init__.py": f'"""Plugin package {plugin_name}."""\n',
        "plugin.py": (
            "from __future__ import annotations\n"
            "\n"
            "from scenario_pipeliner.worker.plugin_registry import (\n"
            "    MainPipelinePluginRegistry,\n"
            "    PluginContext,\n"
            "    ScenarioPluginDefinition,\n"
            ")\n"
            "\n"
            f"from {plugin_name}.pipeline import build_pipeline\n"
            f"from {plugin_name}.states import {prefix}TaskState\n"
            "\n"
            f"SCENARIO = {scenario!r}\n"
            "\n"
            "\n"
            "def register(\n"
            "    registry: MainPipelinePluginRegistry,\n"
            "    context: PluginContext | None = None,\n"
            ") -> None:\n"
            "    _ = context\n"
            "    registry.register(\n"
            "        ScenarioPluginDefinition(\n"
            "            scenario=SCENARIO,\n"
            "            pipeline_factory=build_pipeline,\n"
            f"            state_cls={prefix}TaskState,\n"
            "        )\n"
            "    )\n"
        ),
        "pipeline.py": (
            "from scenario_pipeliner.worker.core.pipeline import AsyncPipeline\n"
            "\n"
            f"from {plugin_name}.settings import {prefix}StepSettings\n"
            f"from {plugin_name}.steps import {prefix}PingStep\n"
            "\n"
            "\n"
            "def build_pipeline() -> AsyncPipeline:\n"
            f"    return AsyncPipeline(steps=[{prefix}PingStep("
            f"settings={prefix}StepSettings())])\n"
        ),
        "settings.py": (
            "from scenario_pipeliner.worker.core.settings import StepSettings\n"
            "\n"
            "\n"
            f"class {prefix}StepSettings(StepSettings):\n"
            '    message: str = "ping"\n'
        ),
        "steps.py": (
            "from scenario_pipeliner.worker.core.clients import AsyncClient\n"
            "from scenario_pipeliner.worker.core.step import AsyncStep\n"
            "\n"
            f"from {plugin_name}.settings import {prefix}StepSettings\n"
            f"from {plugin_name}.states import {prefix}TaskState\n"
            "\n"
            "\n"
            f"class {prefix}PingStep(AsyncStep[{prefix}TaskState, {prefix}StepSettings]):\n"
            "    @property\n"
            "    def clients(self) -> list[AsyncClient]:\n"
            "        return []\n"
            "\n"
            f"    async def _run(self, state: {prefix}TaskState) -> None:\n"
            "        state.result.ok = True\n"
            '        state.result.message = {"ping": self.settings.message}\n'
        ),
        "states.py": (
            "from dataclasses import dataclass\n"
            "\n"
            "from scenario_pipeliner.worker.core.states import TaskState\n"
            "\n"
            "\n"
            "@dataclass\n"
            f"class {prefix}TaskState(TaskState):\n"
            f'    """Task state for {plugin_name}.ping."""\n'
        ),
        "migration.sql": (
            "-- Plugin-owned tables. Core lives in {{core_schema}}.\n"
            "\n"
            "CREATE TABLE IF NOT EXISTS {{plugin_schema}}.events (\n"
            "    id BIGSERIAL PRIMARY KEY,\n"
            "    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),\n"
            "    note TEXT NOT NULL DEFAULT 'ping',\n"
            "    task_id BIGINT NULL REFERENCES {{core_schema}}.tasks(id)\n"
            ");\n"
        ),
    }


def init_plugin(
    plugin_dir: Path,
    *,
    if_exists: IfExists = "fail",
    core_compat: str = DEFAULT_CORE_COMPAT,
    include_migrations: bool = True,
) -> dict[str, object]:
    """Scaffold a plugin directory and write unpacked checksum into the manifest."""
    resolved = plugin_dir.expanduser().resolve()
    if resolved.exists() and not resolved.is_dir():
        raise PluginInitError(f"not a directory: {resolved.as_posix()}")

    plugin_name = plugin_package_name(resolved)
    manifest_path = resolved / MANIFEST_FILE_NAME

    if if_exists == "checksum-only":
        if not manifest_path.is_file():
            raise PluginInitError(
                f"manifest not found: {manifest_path.as_posix()} "
                "(checksum-only requires an existing plugin.manifest.json)"
            )
        written_path, value = write_checksum_to_manifest(resolved)
        return {
            "status": "ok",
            "plugin_dir": resolved.as_posix(),
            "plugin_name": plugin_name,
            "manifest_path": written_path.as_posix(),
            "scenario": None,
            "created": [],
            "checksum": value,
            "if_exists": if_exists,
        }

    resolved.mkdir(parents=True, exist_ok=True)
    existing = [name for name in _STUB_FILES if (resolved / name).exists()]
    if existing and if_exists == "fail":
        raise PluginInitError(
            f"plugin already exists in {resolved.as_posix()}: {', '.join(existing)} "
            "(use --if-exists overwrite or --if-exists checksum-only)"
        )

    stubs = _stub_texts(plugin_name=plugin_name)
    created: list[str] = []
    for name, body in stubs.items():
        if name == "migration.sql" and not include_migrations:
            continue
        path = resolved / name
        path.write_text(body, encoding="utf-8")
        created.append(name)

    manifest = _manifest_payload(
        plugin_name=plugin_name,
        core_compat=core_compat,
        include_migrations=include_migrations,
        migration_order=_migration_order(),
    )
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    if MANIFEST_FILE_NAME not in created:
        created.append(MANIFEST_FILE_NAME)

    written_path, value = write_checksum_to_manifest(resolved)
    return {
        "status": "ok",
        "plugin_dir": resolved.as_posix(),
        "plugin_name": plugin_name,
        "manifest_path": written_path.as_posix(),
        "scenario": f"{plugin_name}.ping",
        "created": created,
        "checksum": value,
        "if_exists": if_exists,
    }
