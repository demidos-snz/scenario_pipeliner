import json
from pathlib import Path

import pytest

from scenario_pipeliner.api.enums import DryRunErrorCode, Mode
from scenario_pipeliner.core.checksum import sha256_directory
from scenario_pipeliner.core.manifest_loader import load_manifest
from scenario_pipeliner.core.registry import PluginRegistryV1, RegistryError


def _write_manifest(plugin_dir: Path, *, checksum_value: str) -> Path:
    manifest = {
        "plugin_name": plugin_dir.name,
        "plugin_version": "1.0.0",
        "plugin_api_version": "v1",
        "core_compat": ">=0.1,<1.0",
        "checksum": {
            "algorithm": "sha256",
            "scope": "unpacked",
            "value": checksum_value,
        },
        "entrypoint": f"{plugin_dir.name}.plugin:register",
        "scenarios": [f"{plugin_dir.name}.scenario_9"],
        "migrations": {
            "migration_order": "20260713120000",
            "postgresql": "migrations/postgresql.sql",
        },
    }
    manifest_path = plugin_dir / "plugin.manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_path


def _prepare_plugin_dir(tmp_path: Path, plugin_name: str) -> Path:
    plugin_dir = tmp_path / plugin_name
    (plugin_dir / "migrations").mkdir(parents=True)
    (plugin_dir / "migrations" / "postgresql.sql").write_text(
        "-- migration", encoding="utf-8"
    )
    (plugin_dir / "plugin.py").write_text(
        "def register():\n    return None\n", encoding="utf-8"
    )
    return plugin_dir


def test_registry_dev_mode_warns_on_checksum_mismatch(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin_dir(tmp_path, "acme_docs")
    manifest_path = _write_manifest(plugin_dir, checksum_value="f" * 64)
    manifest = load_manifest(manifest_path)

    registry = PluginRegistryV1()
    result = registry.register(
        manifest,
        mode=Mode.DEV,
        core_version="0.1.0",
        plugin_path=plugin_dir,
    )
    assert result.warnings


def test_registry_prod_mode_fails_on_checksum_mismatch(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin_dir(tmp_path, "acme_docs")
    manifest_path = _write_manifest(plugin_dir, checksum_value="f" * 64)
    manifest = load_manifest(manifest_path)

    registry = PluginRegistryV1()
    with pytest.raises(RegistryError):
        registry.register(
            manifest,
            mode=Mode.PROD,
            core_version="0.1.0",
            plugin_path=plugin_dir,
        )


def test_registry_rejects_duplicate_plugin_name(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin_dir(tmp_path, "acme_docs")
    manifest_path = _write_manifest(
        plugin_dir, checksum_value=sha256_directory(plugin_dir)
    )
    manifest = load_manifest(manifest_path)

    registry = PluginRegistryV1()
    registry.register(
        manifest, mode=Mode.DEV, core_version="0.1.0", plugin_path=plugin_dir
    )
    with pytest.raises(RegistryError) as exc:
        registry.register(
            manifest, mode=Mode.DEV, core_version="0.1.0", plugin_path=plugin_dir
        )
    assert exc.value.code is DryRunErrorCode.PLUGIN_NAME_CONFLICT


def test_registry_rejects_plugin_schema_collision(tmp_path: Path) -> None:
    first_dir = _prepare_plugin_dir(tmp_path, "track-documents")
    second_dir = _prepare_plugin_dir(tmp_path, "track.documents")
    first = load_manifest(
        _write_manifest(first_dir, checksum_value=sha256_directory(first_dir))
    )
    second = load_manifest(
        _write_manifest(second_dir, checksum_value=sha256_directory(second_dir))
    )
    registry = PluginRegistryV1()
    registry.register(first, mode=Mode.DEV, core_version="0.1.0", plugin_path=first_dir)
    with pytest.raises(RegistryError) as exc:
        registry.register(
            second, mode=Mode.DEV, core_version="0.1.0", plugin_path=second_dir
        )
    assert exc.value.code is DryRunErrorCode.PLUGIN_SCHEMA_CONFLICT
    assert "track_documents" in str(exc.value)


def test_sha256_directory_ignores_symlinked_external_files(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin_dir(tmp_path, "acme_docs")
    external_file = tmp_path / "external.txt"
    external_file.write_text("v1", encoding="utf-8")
    (plugin_dir / "linked_external.txt").symlink_to(external_file)

    digest_before = sha256_directory(plugin_dir)
    external_file.write_text("v2-changed", encoding="utf-8")
    digest_after = sha256_directory(plugin_dir)

    assert digest_before == digest_after


def test_sha256_directory_includes_hardlinked_files(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin_dir(tmp_path, "acme_docs")
    external_file = tmp_path / "external.txt"
    external_file.write_text("v1", encoding="utf-8")
    (plugin_dir / "hardlinked_external.txt").hardlink_to(external_file)

    digest_before = sha256_directory(plugin_dir)
    external_file.write_text("v2-changed", encoding="utf-8")
    digest_after = sha256_directory(plugin_dir)

    assert digest_before != digest_after


def test_sha256_directory_not_empty_with_only_hardlinked_file(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "acme_docs"
    plugin_dir.mkdir(parents=True)
    external_file = tmp_path / "external.txt"
    external_file.write_text("payload", encoding="utf-8")
    (plugin_dir / "only_hardlink.txt").hardlink_to(external_file)

    digest = sha256_directory(plugin_dir)

    assert digest != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def test_sha256_directory_ignores_ds_store_and_stray_pyc(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin_dir(tmp_path, "acme_docs")
    digest_before = sha256_directory(plugin_dir)

    (plugin_dir / ".DS_Store").write_text("macos metadata", encoding="utf-8")
    (plugin_dir / "random.pyc").write_bytes(b"\x00\x01\x02\x03")
    digest_after = sha256_directory(plugin_dir)

    assert digest_before == digest_after


def test_sha256_directory_ignore_list_is_relative_to_plugin(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "__pycache__" / "acme_docs"
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "plugin.py").write_text("x = 1\n", encoding="utf-8")

    digest = sha256_directory(plugin_dir)

    assert digest != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
