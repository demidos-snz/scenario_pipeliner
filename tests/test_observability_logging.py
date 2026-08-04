from __future__ import annotations

import logging
import sys
from types import ModuleType

import pytest

import scenario_pipeliner.observability.logging as logging_module
from scenario_pipeliner.observability import LoggingSettings, setup_logging


@pytest.fixture(autouse=True)
def reset_logging_configured() -> None:
    logging_module._CONFIGURED = False
    yield
    logging_module._CONFIGURED = False


def test_setup_logging_reads_level(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LEVEL_LOGGING", "warning")
    monkeypatch.delenv("SENTRY_DSN", raising=False)

    settings = setup_logging()

    assert settings.level == "WARNING"
    assert logging.getLogger().getEffectiveLevel() == logging.WARNING


def test_setup_logging_accepts_scenario_pipeliner_log_level_alias(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LEVEL_LOGGING", raising=False)
    monkeypatch.setenv("SCENARIO_PIPELINER_LOG_LEVEL", "ERROR")
    monkeypatch.delenv("SENTRY_DSN", raising=False)

    settings = LoggingSettings.from_env()

    assert settings.level == "ERROR"


def test_setup_logging_initializes_sentry_when_dsn_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    fake = ModuleType("sentry_sdk")

    def _init(**kwargs: object) -> None:
        calls.append(kwargs)

    fake.init = _init  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "sentry_sdk", fake)
    monkeypatch.setenv("SENTRY_DSN", "https://example.invalid/1")
    monkeypatch.setenv("ENV", "staging")
    monkeypatch.setenv("SENTRY_TRACES_SAMPLE_RATE", "0.2")
    monkeypatch.setenv("SENTRY_PROFILES_SAMPLE_RATE", "0.3")

    setup_logging()

    assert len(calls) == 1
    assert calls[0]["dsn"] == "https://example.invalid/1"
    assert calls[0]["environment"] == "staging"
    assert calls[0]["traces_sample_rate"] == 0.2
    assert calls[0]["profiles_sample_rate"] == 0.3


def test_setup_logging_warns_when_sentry_dsn_without_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SENTRY_DSN", "https://example.invalid/1")
    monkeypatch.delitem(sys.modules, "sentry_sdk", raising=False)

    real_import = __import__

    def _blocked_import(name: str, globals=None, locals=None, fromlist=(), level=0):  # type: ignore[no-untyped-def]
        if name == "sentry_sdk":
            raise ImportError("blocked for test")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr("builtins.__import__", _blocked_import)

    with pytest.warns(UserWarning, match="sentry-sdk is not installed"):
        setup_logging()
