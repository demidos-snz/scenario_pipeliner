import json
from pathlib import Path

import pytest

from scenario_pipeliner.api.enums import DbBackend, Mode
from scenario_pipeliner.api.settings import ScenarioPipelinerConfig
from scenario_pipeliner.worker.runtime.registry import (
    build_worker_registry_from_manifests,
)


def _write_plugin(
    tmp_path: Path,
    *,
    plugin_name: str,
    entrypoint_object: str,
    scenarios: list[str],
    plugin_code: str,
) -> Path:
    plugins_root = tmp_path / "plugins"
    plugin_dir = plugins_root / plugin_name
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "__init__.py").write_text("", encoding="utf-8")
    (plugin_dir / "plugin.py").write_text(plugin_code, encoding="utf-8")
    (plugin_dir / "migrations").mkdir()
    (plugin_dir / "migrations" / "postgresql.sql").write_text(
        "-- migration", encoding="utf-8"
    )
    manifest = {
        "plugin_name": plugin_name,
        "plugin_version": "1.0.0",
        "plugin_api_version": "v1",
        "core_compat": ">=0.1,<1.0",
        "checksum": {
            "algorithm": "sha256",
            "scope": "unpacked",
            "value": "0" * 64,
        },
        "entrypoint": f"{plugin_name}.plugin:{entrypoint_object}",
        "scenarios": scenarios,
        "migrations": {
            "migration_order": "20260713120000",
            "postgresql": "migrations/postgresql.sql",
        },
    }
    (plugin_dir / "plugin.manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    return plugins_root


def test_build_worker_registry_from_manifests_loads_entrypoint_registration(
    tmp_path: Path,
) -> None:
    plugin_name = "acme_docs_runtime"
    plugins_root = _write_plugin(
        tmp_path,
        plugin_name=plugin_name,
        entrypoint_object="register",
        scenarios=[f"{plugin_name}.scenario_9"],
        plugin_code=(
            "from dataclasses import dataclass\n"
            "from scenario_pipeliner.worker.core.pipeline import AsyncPipeline\n"
            "from scenario_pipeliner.worker.core.states import TaskState\n"
            "from scenario_pipeliner.worker.plugin_registry import ScenarioPluginDefinition\n\n"
            "@dataclass\n"
            "class ScenarioState(TaskState):\n"
            "    marker: str = 'ok'\n\n"
            "class ScenarioPipeline(AsyncPipeline):\n"
            "    async def execute(self, state=None) -> None:\n"
            "        return\n\n"
            "def register(registry):\n"
            f"    registry.register(ScenarioPluginDefinition(\n"
            f"        scenario='{plugin_name}.scenario_9',\n"
            "        pipeline_factory=lambda: ScenarioPipeline(steps=[]),\n"
            "        state_cls=ScenarioState,\n"
            "    ))\n"
        ),
    )
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=plugins_root,
        core_version="0.1.0",
    )

    runtime_registry = build_worker_registry_from_manifests(config)

    assert runtime_registry.scenarios == [f"{plugin_name}.scenario_9"]
    assert f"{plugin_name}.scenario_9" in runtime_registry.pipeline_factories
    assert f"{plugin_name}.scenario_9" in runtime_registry.state_classes


def test_build_worker_registry_from_manifests_rejects_manifest_scenario_mismatch(
    tmp_path: Path,
) -> None:
    plugin_name = "acme_docs_runtime_bad"
    plugins_root = _write_plugin(
        tmp_path,
        plugin_name=plugin_name,
        entrypoint_object="register",
        scenarios=[f"{plugin_name}.scenario_expected"],
        plugin_code=(
            "from dataclasses import dataclass\n"
            "from scenario_pipeliner.worker.core.pipeline import AsyncPipeline\n"
            "from scenario_pipeliner.worker.core.states import TaskState\n"
            "from scenario_pipeliner.worker.plugin_registry import ScenarioPluginDefinition\n\n"
            "@dataclass\n"
            "class ScenarioState(TaskState):\n"
            "    marker: str = 'bad'\n\n"
            "class ScenarioPipeline(AsyncPipeline):\n"
            "    async def execute(self, state=None) -> None:\n"
            "        return\n\n"
            "def register(registry):\n"
            f"    registry.register(ScenarioPluginDefinition(\n"
            f"        scenario='{plugin_name}.scenario_actual',\n"
            "        pipeline_factory=lambda: ScenarioPipeline(steps=[]),\n"
            "        state_cls=ScenarioState,\n"
            "    ))\n"
        ),
    )
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=plugins_root,
        core_version="0.1.0",
    )

    with pytest.raises(ValueError, match="registration scenarios mismatch"):
        build_worker_registry_from_manifests(config)


def test_build_worker_registry_from_manifests_passes_plugin_context(
    tmp_path: Path,
) -> None:
    plugin_name = "acme_docs_runtime_ctx"
    plugins_root = _write_plugin(
        tmp_path,
        plugin_name=plugin_name,
        entrypoint_object="register",
        scenarios=[f"{plugin_name}.scenario_9"],
        plugin_code=(
            "from dataclasses import dataclass\n"
            "from scenario_pipeliner.worker.core.pipeline import AsyncPipeline\n"
            "from scenario_pipeliner.worker.core.states import TaskState\n"
            "from scenario_pipeliner.worker.plugin_registry import ScenarioPluginDefinition\n\n"
            "@dataclass\n"
            "class ScenarioState(TaskState):\n"
            "    marker: str = 'ctx'\n\n"
            "class ScenarioPipeline(AsyncPipeline):\n"
            "    async def execute(self, state=None) -> None:\n"
            "        return\n\n"
            "def register(registry, context):\n"
            "    marker = context.services.get(context.plugin_name, {}).get('marker')\n"
            "    if marker != 'ok':\n"
            "        raise ValueError('context marker missing')\n"
            f"    registry.register(ScenarioPluginDefinition(\n"
            f"        scenario='{plugin_name}.scenario_9',\n"
            "        pipeline_factory=lambda: ScenarioPipeline(steps=[]),\n"
            "        state_cls=ScenarioState,\n"
            "    ))\n"
        ),
    )
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=plugins_root,
        core_version="0.1.0",
    )

    runtime_registry = build_worker_registry_from_manifests(
        config,
        plugin_services={plugin_name: {"marker": "ok"}},
    )

    assert runtime_registry.scenarios == [f"{plugin_name}.scenario_9"]


def test_two_plugins_load_isolated_packages(tmp_path: Path) -> None:
    def _code(plugin_name: str) -> str:
        return (
            "from dataclasses import dataclass\n"
            "from scenario_pipeliner.worker.core.pipeline import AsyncPipeline\n"
            "from scenario_pipeliner.worker.core.states import TaskState\n"
            "from scenario_pipeliner.worker.plugin_registry import ScenarioPluginDefinition\n"
            f"from {plugin_name}.marker import MARKER\n\n"
            "@dataclass\n"
            "class ScenarioState(TaskState):\n"
            "    marker: str = MARKER\n\n"
            "class ScenarioPipeline(AsyncPipeline):\n"
            "    async def execute(self, state=None) -> None:\n"
            "        return\n\n"
            "def register(registry):\n"
            f"    registry.register(ScenarioPluginDefinition(\n"
            f"        scenario='{plugin_name}.' + MARKER,\n"
            "        pipeline_factory=lambda: ScenarioPipeline(steps=[]),\n"
            "        state_cls=ScenarioState,\n"
            "    ))\n"
        )

    plugins_root = tmp_path / "plugins"
    for name, marker in (("alpha_plug", "left"), ("beta_plug", "right")):
        plugin_dir = plugins_root / name
        plugin_dir.mkdir(parents=True)
        (plugin_dir / "__init__.py").write_text("", encoding="utf-8")
        (plugin_dir / "marker.py").write_text(
            f"MARKER = {marker!r}\n", encoding="utf-8"
        )
        (plugin_dir / "plugin.py").write_text(_code(name), encoding="utf-8")
        (plugin_dir / "plugin.manifest.json").write_text(
            json.dumps(
                {
                    "plugin_name": name,
                    "plugin_version": "1.0.0",
                    "plugin_api_version": "v1",
                    "core_compat": ">=0.1,<1.0",
                    "checksum": {
                        "algorithm": "sha256",
                        "scope": "unpacked",
                        "value": "0" * 64,
                    },
                    "entrypoint": f"{name}.plugin:register",
                    "scenarios": [f"{name}.{marker}"],
                }
            ),
            encoding="utf-8",
        )
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=plugins_root,
        core_version="0.1.0",
    )
    runtime_registry = build_worker_registry_from_manifests(config)
    assert sorted(runtime_registry.scenarios) == [
        "alpha_plug.left",
        "beta_plug.right",
    ]
