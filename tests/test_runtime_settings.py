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
    monkeypatch.setenv("RUNNER_SHUTDOWN_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("RUNNER_TASKS_LIMIT", "20")
    monkeypatch.setenv("RUNNER_ZOMBIE_TASKS_TIMEOUT_MINUTES", "12")
    monkeypatch.setenv("RUNNER_MAX_CONCURRENT_TASKS", "8")
    monkeypatch.setenv("RUNNER_HEARTBEAT_INTERVAL_SECONDS", "15")
    monkeypatch.setenv("RUNNER_RUN_SECONDS", "30")
    monkeypatch.setenv("RUNNER_APPLY_MIGRATIONS", "false")
    monkeypatch.setenv("SCENARIO_PIPELINER_RUNNER_MODE", "broker")

    settings = RuntimeEnvSettings.from_env()

    assert settings.mode == Mode.PROD
    assert settings.db_backend == DbBackend.POSTGRESQL
    assert settings.plugins_root == plugins.resolve()
    assert settings.core_version == "0.2.0"
    assert settings.runner_poll_interval_seconds == 5
    assert settings.runner_shutdown_timeout_seconds == 45
    assert settings.runner_tasks_limit == 20
    assert settings.runner_zombie_tasks_timeout_minutes == 12
    assert settings.runner_max_concurrent_tasks == 8
    assert settings.runner_heartbeat_interval_seconds == 15
    assert settings.run_seconds == 30
    assert settings.apply_migrations is False
    assert settings.runner_mode == "broker"
    runner = settings.runner_settings()
    assert runner.ZOMBIE_TASKS_TIMEOUT_MINUTES == 12
    assert runner.MAX_CONCURRENT_TASKS == 8
    assert runner.HEARTBEAT_INTERVAL_SECONDS == 15


def test_runtime_env_settings_core_version_defaults_to_package(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scenario_pipeliner.version import __version__

    monkeypatch.delenv("SCENARIO_PIPELINER_CORE_VERSION", raising=False)
    settings = RuntimeEnvSettings(_env_file=None)
    assert settings.core_version == __version__


def test_runtime_env_settings_rejects_non_postgres(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RUNNER_DB_BACKEND", "sqlite")

    with pytest.raises(ValidationError):
        RuntimeEnvSettings.from_env()


def test_runtime_env_settings_accepts_legacy_shutdown_timeout_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("RUNNER_DB_BACKEND", "postgresql")
    monkeypatch.setenv("SHUTDOWN_TIMEOUT_SECONDS", "60")

    settings = RuntimeEnvSettings.from_env()
    runner_settings = settings.runner_settings()

    assert settings.runner_shutdown_timeout_seconds == 60
    assert runner_settings.SHUTDOWN_TIMEOUT_SECONDS == 60
    assert runner_settings.ZOMBIE_TASKS_TIMEOUT_MINUTES == 30
    assert runner_settings.MAX_CONCURRENT_TASKS == 5


def test_postgres_runtime_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("POSTGRES_URL", "")
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
    assert "postgresql+asyncpg://" in settings.async_url
    assert settings.DB_POOL_MIN_SIZE == 2
    assert settings.DB_POOL_MAX_SIZE == 4


def test_postgres_runtime_settings_from_url(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "POSTGRES_URL",
        "postgresql+asyncpg://urluser:urlpass@urlhost:5999/urldb",
    )
    monkeypatch.setenv("POSTGRES_HOST", "ignored")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "ignored")
    monkeypatch.setenv("POSTGRES_USER", "ignored")
    monkeypatch.setenv("POSTGRES_PASSWORD", "ignored")

    settings = PostgresRuntimeSettings.from_env()

    assert "urluser:urlpass@urlhost:5999/urldb" in settings.sync_url
    assert settings.async_url.startswith("postgresql+asyncpg://")
    assert "ignored" not in settings.sync_url


def test_postgres_runtime_settings_invalid_url_falls_back(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POSTGRES_URL", "not-a-postgres-url")
    monkeypatch.setenv("POSTGRES_HOST", "db.local")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("POSTGRES_DB", "sp")
    monkeypatch.setenv("POSTGRES_USER", "user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")

    settings = PostgresRuntimeSettings.from_env()

    assert "user:secret@db.local:5433/sp" in settings.sync_url
