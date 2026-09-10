from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from scenario_pipeliner.core.checksum import sha256_directory
from scenario_pipeliner.core.plugin_checksum import write_checksum_to_manifest
from scenario_pipeliner.db.settings import CoreDb
from scenario_pipeliner.worker.core.enums import TaskStatus, TaskType
from scenario_pipeliner.worker.runtime.runner_helpers import TaskSeed, create_task


def _cli(
    *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "scenario_pipeliner.cli", *args],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )


def test_cli_plugin_init_scaffolds_and_checksum(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "demo_ping"
    result = _cli("plugin", "init", str(plugin_dir))
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["plugin_name"] == "demo_ping"
    assert payload["scenario"] == "demo_ping.ping"
    assert (plugin_dir / "plugin.py").is_file()
    assert (plugin_dir / "migration.sql").is_file()
    manifest = json.loads(
        (plugin_dir / "plugin.manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["entrypoint"] == "demo_ping.plugin:register"
    assert manifest["core_compat"] == ">=0.0.1,<1.0"
    assert manifest["checksum"]["value"] == sha256_directory(plugin_dir)
    assert "migrations" in manifest


def test_cli_plugin_init_fail_if_exists(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "demo_ping"
    assert _cli("plugin", "init", str(plugin_dir)).returncode == 0
    again = _cli("plugin", "init", str(plugin_dir))
    assert again.returncode == 2
    assert "already exists" in again.stderr


def test_cli_plugin_init_checksum_only(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "demo_ping"
    assert _cli("plugin", "init", str(plugin_dir)).returncode == 0
    manifest_path = plugin_dir / "plugin.manifest.json"
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    raw["checksum"]["value"] = "0" * 64
    manifest_path.write_text(json.dumps(raw, indent=2) + "\n", encoding="utf-8")
    expected = sha256_directory(plugin_dir)
    result = _cli("plugin", "init", str(plugin_dir), "--if-exists", "checksum-only")
    assert result.returncode == 0, result.stderr
    written = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert written["checksum"]["value"] == expected


def test_cli_plugin_init_rejects_hyphen_name(tmp_path: Path) -> None:
    result = _cli("plugin", "init", str(tmp_path / "hello-world"))
    assert result.returncode == 2
    assert "underscores" in result.stderr


def test_cli_plugin_init_no_migrations(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "demo_ping"
    result = _cli("plugin", "init", str(plugin_dir), "--no-migrations")
    assert result.returncode == 0, result.stderr
    manifest = json.loads(
        (plugin_dir / "plugin.manifest.json").read_text(encoding="utf-8")
    )
    assert "migrations" not in manifest
    assert not (plugin_dir / "migration.sql").exists()


def test_cli_task_list_reads_manifests(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "demo_ping"
    assert _cli("plugin", "init", str(plugin_dir)).returncode == 0
    env = os.environ.copy()
    env["SCENARIO_PIPELINER_PLUGINS_ROOT"] = str(tmp_path)
    result = _cli("task", "list", env=env)
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["scenarios"][0]["scenario"] == "demo_ping.ping"
    assert payload["scenarios"][0]["plugin_name"] == "demo_ping"


def test_cli_task_create_inserts_linear_task(
    core_db: CoreDb,
    postgres_params: dict[str, Any],
    tmp_path: Path,
) -> None:
    plugin_dir = tmp_path / "demo_ping"
    assert _cli("plugin", "init", str(plugin_dir)).returncode == 0
    env = os.environ.copy()
    env.update(
        {
            "SCENARIO_PIPELINER_PLUGINS_ROOT": str(tmp_path),
            "SCENARIO_PIPELINER_DB_SCHEMA": core_db.core.schema_name,
            "POSTGRES_HOST": str(postgres_params["host"]),
            "POSTGRES_PORT": str(postgres_params["port"]),
            "POSTGRES_DB": str(postgres_params["database"]),
            "POSTGRES_USER": str(postgres_params["user"]),
            "POSTGRES_PASSWORD": str(postgres_params["password"]),
        }
    )
    env.pop("POSTGRES_URL", None)
    result = _cli(
        "task",
        "create",
        "--scenario",
        "demo_ping.ping",
        "--alias",
        "from-cli",
        env=env,
    )
    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["type_task"] == "LINEAR"
    task_id = int(payload["task_id"])

    async def _fetch() -> dict[str, Any]:
        from sqlalchemy import select

        async with core_db.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        select(core_db.core.tasks).where(
                            core_db.core.tasks.c.id == task_id
                        )
                    )
                )
                .mappings()
                .one()
            )
            return dict(row)

    row = asyncio.run(_fetch())
    assert row["scenario"] == "demo_ping.ping"
    assert row["status"] == TaskStatus.NEW.value
    assert row["type_task"] == TaskType.LINEAR.value
    assert row["alias"] == "from-cli"


def test_cli_task_create_unknown_scenario(tmp_path: Path) -> None:
    env = os.environ.copy()
    env["SCENARIO_PIPELINER_PLUGINS_ROOT"] = str(tmp_path)
    result = _cli("task", "create", "--scenario", "missing.ping", env=env)
    assert result.returncode == 2
    assert "unknown scenario" in result.stderr


def test_create_task_helper_linear(core_db: CoreDb) -> None:
    task_id = asyncio.run(
        create_task(
            db=core_db,
            seed=TaskSeed(scenario="helper.ping", alias="helper"),
        )
    )
    assert task_id > 0


def test_write_checksum_after_init_is_stable(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "demo_ping"
    assert _cli("plugin", "init", str(plugin_dir)).returncode == 0
    first = sha256_directory(plugin_dir)
    write_checksum_to_manifest(plugin_dir)
    assert sha256_directory(plugin_dir) == first
