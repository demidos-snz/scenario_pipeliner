from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from sqlalchemy import insert, select

from scenario_pipeliner.api.enums import DbBackend, Mode
from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.worker.core.enums import TaskStatus
from scenario_pipeliner.worker.core.states import TaskPayload
from scenario_pipeliner.worker.runtime.app import RunnerApp


def _write_ping_plugin(plugins_root: Path) -> str:
    plugin_name = "smoke_ping"
    scenario = f"{plugin_name}.ping"
    plugin_dir = plugins_root / plugin_name
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "__init__.py").write_text("", encoding="utf-8")
    (plugin_dir / "plugin.py").write_text(
        "from dataclasses import dataclass\n"
        "from scenario_pipeliner.worker.core.pipeline import AsyncPipeline\n"
        "from scenario_pipeliner.worker.core.states import TaskState\n"
        "from scenario_pipeliner.worker.plugin_registry import ScenarioPluginDefinition\n\n"
        "@dataclass\n"
        "class PingState(TaskState):\n"
        "    marker: str = 'ping'\n\n"
        "class PingPipeline(AsyncPipeline):\n"
        "    async def execute(self, state=None) -> None:\n"
        "        if state is not None:\n"
        "            state.result.ok = True\n\n"
        "def register(registry):\n"
        "    registry.register(ScenarioPluginDefinition(\n"
        f"        scenario='{scenario}',\n"
        "        pipeline_factory=lambda: PingPipeline(steps=[]),\n"
        "        state_cls=PingState,\n"
        "    ))\n",
        encoding="utf-8",
    )
    (plugin_dir / "plugin.manifest.json").write_text(
        json.dumps(
            {
                "plugin_name": plugin_name,
                "plugin_version": "1.0.0",
                "plugin_api_version": "v1",
                "core_compat": ">=0.0.1,<1.0",
                "checksum": {
                    "algorithm": "sha256",
                    "scope": "unpacked",
                    "value": "0" * 64,
                },
                "entrypoint": f"{plugin_name}.plugin:register",
                "scenarios": [scenario],
            }
        ),
        encoding="utf-8",
    )
    return scenario


def test_runner_app_executes_inserted_linear_task(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    core_db: CoreDb,
    postgres_params: dict,
) -> None:
    plugins_root = tmp_path / "plugins"
    scenario = _write_ping_plugin(plugins_root)
    monkeypatch.setenv("SCENARIO_PIPELINER_PLUGINS_ROOT", str(plugins_root))
    monkeypatch.setenv("SCENARIO_PIPELINER_DB_SCHEMA", core_db.core.schema_name)
    monkeypatch.setenv("SCENARIO_PIPELINER_CORE_VERSION", "0.0.1")
    monkeypatch.setenv("SCENARIO_PIPELINER_MODE", Mode.DEV.value)
    monkeypatch.setenv("RUNNER_DB_BACKEND", DbBackend.POSTGRESQL.value)
    monkeypatch.setenv("RUNNER_APPLY_MIGRATIONS", "false")
    monkeypatch.setenv("RUNNER_RUN_SECONDS", "4")
    monkeypatch.setenv("RUNNER_POLL_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("SCENARIO_PIPELINER_RUNNER_MODE", "db")
    monkeypatch.setenv("POSTGRES_HOST", str(postgres_params["host"]))
    monkeypatch.setenv("POSTGRES_PORT", str(postgres_params["port"]))
    monkeypatch.setenv("POSTGRES_DB", str(postgres_params["database"]))
    monkeypatch.setenv("POSTGRES_USER", str(postgres_params["user"]))
    monkeypatch.setenv("POSTGRES_PASSWORD", str(postgres_params["password"]))
    monkeypatch.delenv("POSTGRES_URL", raising=False)

    async def _insert() -> int:
        async with core_db.engine.begin() as conn:
            return int(
                (
                    await conn.execute(
                        insert(core_db.core.tasks)
                        .values(
                            scenario=scenario,
                            max_executions=1,
                            payload=TaskPayload().model_dump(mode="json"),
                        )
                        .returning(core_db.core.tasks.c.id)
                    )
                ).scalar_one()
            )

    async def _status(task_id: int) -> str:
        async with core_db.engine.connect() as conn:
            value = (
                await conn.execute(
                    select(core_db.core.tasks.c.status).where(
                        core_db.core.tasks.c.id == task_id
                    )
                )
            ).scalar_one()
        return str(value)

    async def _run() -> str:
        task_id = await _insert()
        app = await RunnerApp.from_env()
        await app.run()
        return await _status(task_id)

    status = asyncio.run(_run())
    assert status == TaskStatus.FINISHED.value
