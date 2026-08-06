import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from scenario_pipeliner.api.models import PluginManifestV1
from scenario_pipeliner.core.manifest_loader import load_manifest


def _base_manifest() -> dict:
    return {
        "plugin_name": "acme_docs",
        "plugin_version": "1.0.0",
        "plugin_api_version": "v1",
        "core_compat": ">=0.1,<1.0",
        "checksum": {
            "algorithm": "sha256",
            "scope": "unpacked",
            "value": "a" * 64,
        },
        "entrypoint": "acme_docs.plugin:register",
        "scenarios": ["acme_docs.scenario_9"],
        "migrations": {
            "migration_order": "20260713120000",
            "sqlite": "migrations/sqlite.sql",
        },
    }


def test_manifest_accepts_valid_v1_schema() -> None:
    manifest = PluginManifestV1.model_validate(_base_manifest())
    assert manifest.plugin_name == "acme_docs"
    assert manifest.migrations.migration_order == "20260713120000"


def test_manifest_accepts_missing_migrations_section() -> None:
    payload = _base_manifest()
    payload.pop("migrations", None)

    manifest = PluginManifestV1.model_validate(payload)

    assert manifest.plugin_name == "acme_docs"
    assert manifest.migrations is None


def test_manifest_rejects_invalid_core_compat() -> None:
    payload = _base_manifest()
    payload["core_compat"] = "not-a-specifier"
    with pytest.raises(ValidationError):
        PluginManifestV1.model_validate(payload)


def test_manifest_rejects_invalid_scenario_prefix() -> None:
    payload = _base_manifest()
    payload["scenarios"] = ["another_plugin.scenario_9"]
    with pytest.raises(ValidationError):
        PluginManifestV1.model_validate(payload)


def test_manifest_rejects_invalid_migration_order_calendar_datetime() -> None:
    payload = _base_manifest()
    payload["migrations"]["migration_order"] = "20261399999999"
    with pytest.raises(ValidationError):
        PluginManifestV1.model_validate(payload)


def test_load_manifest_sets_manifest_path_during_validation(
    tmp_path: Path,
) -> None:
    manifest_path = tmp_path / "plugin.manifest.json"
    manifest_path.write_text(json.dumps(_base_manifest()), encoding="utf-8")

    manifest = load_manifest(manifest_path)

    assert manifest.manifest_path == manifest_path
