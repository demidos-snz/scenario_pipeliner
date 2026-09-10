from __future__ import annotations

import logging
import warnings

from scenario_pipeliner.observability.settings import LoggingSettings

_CONFIGURED = False


def setup_logging(settings: LoggingSettings | None = None) -> LoggingSettings:
    """Configure root logging once; optionally init Sentry when DSN is set.

    Sentry requires the optional extra: ``pip install scenario-pipeliner[sentry]``.
    """
    global _CONFIGURED
    resolved = settings or LoggingSettings.from_env()
    if _CONFIGURED:
        return resolved

    level = getattr(logging, resolved.level, logging.INFO)
    logging.basicConfig(level=level, format=resolved.format)
    _configure_sentry(resolved)
    _CONFIGURED = True
    return resolved


def _configure_sentry(settings: LoggingSettings) -> None:
    if not settings.sentry_dsn:
        return
    try:
        import sentry_sdk
    except ImportError:
        warnings.warn(
            "SENTRY_DSN is set but sentry-sdk is not installed; "
            "install with: pip install 'scenario-pipeliner[sentry]'",
            stacklevel=2,
        )
        return

    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        environment=settings.environment,
        traces_sample_rate=settings.sentry_traces_sample_rate,
        profiles_sample_rate=settings.sentry_profiles_sample_rate,
    )
