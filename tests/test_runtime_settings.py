from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from scenario_pipeliner.api.enums import DbBackend, Mode
from scenario_pipeliner.worker.runtime.settings import (
    PostgresRuntimeSettings,
    RuntimeEnvSettings,
)


def test_runtime_env_settings_from_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    plugins = tmp_path / "plugins"
    plugins.mkdir()
    monkeypatch.setenv("SCENARIO_PIPELINER_MODE", "prod")
    monkeypatch.setenv("RUNNER_DB_BACKEND", "postgresql")
    monkeypatch.setenv("SCENARIO_PIPELINER_PLUGINS_ROOT", str(plugins))
    monkeypatch.setenv("SCENARIO_PIPELINER_CORE_VERSION", "0.2.0")
    monkeypatch.setenv("RUNNER_POLL_INTERVAL_SECONDS", "5")
    monkeypatch.setenv("RUNNER_TASKS_LIMIT", "20")
    monkeypatch.setenv("RUNNER_RUN_SECONDS", "30")
    monkeypatch.setenv("RUNNER_APPLY_MIGRATIONS", "false")

    settings = RuntimeEnvSettings.from_env()

    assert settings.mode == Mode.PROD
    assert settings.db_backend == DbBackend.POSTGRESQL
    assert settings.plugins_root == plugins.resolve()
    assert settings.core_version == "0.2.0"
    assert settings.runner_poll_interval_seconds == 5
    assert settings.runner_tasks_limit == 20
    assert settings.run_seconds == 30
    assert settings.apply_migrations is False


def test_runtime_env_settings_rejects_non_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RUNNER_DB_BACKEND", "sqlite")

    with pytest.raises(ValidationError):
        RuntimeEnvSettings.from_env()


def test_postgres_runtime_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_HOST", "db.local")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "sp")
    monkeypatch.setenv("POSTGRES_USER", "user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("DB_POOL_MIN_SIZE", "2")
    monkeypatch.setenv("DB_POOL_MAX_SIZE", "4")

    settings = PostgresRuntimeSettings.from_env()

    assert settings.POSTGRES_HOST == "db.local"
    assert settings.POSTGRES_PORT == 5433
    assert "user:secret@db.local:5433/sp" in settings.sync_url
    assert settings.DB_POOL_MIN_SIZE == 2
    assert settings.DB_POOL_MAX_SIZE == 4
