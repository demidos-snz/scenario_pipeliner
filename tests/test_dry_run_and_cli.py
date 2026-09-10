import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from scenario_pipeliner.api.core_migrate import apply_core_migrations
from scenario_pipeliner.api.enums import DbBackend, DryRunErrorCode, Mode
from scenario_pipeliner.api.migrate import apply_migrations
from scenario_pipeliner.api.settings import CoreMigrationConfig, ScenarioPipelinerConfig
from scenario_pipeliner.core import dry_run as dry_run_module
from scenario_pipeliner.core.checksum import sha256_directory
from scenario_pipeliner.core.plugin_checksum import write_checksum_to_manifest
from scenario_pipeliner.db.settings import PostgresUrls


def _prepare_plugin(
    tmp_path: Path,
    plugin_name: str,
    *,
    dirname: str | None = None,
    scenarios: list[str] | None = None,
) -> Path:
    plugin_dir = tmp_path / (dirname or plugin_name)
    (plugin_dir / "migrations").mkdir(parents=True)
    (plugin_dir / "migrations" / "postgresql.sql").write_text(
        "-- postgresql", encoding="utf-8"
    )
    (plugin_dir / "plugin.py").write_text(
        "def register():\n    return None\n", encoding="utf-8"
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
        "entrypoint": f"{plugin_dir.name}.plugin:register",
        "scenarios": scenarios or [f"{plugin_name}.scenario_9"],
        "migrations": {
            "migration_order": "20260713120000",
            "postgresql": "migrations/postgresql.sql",
        },
    }
    (plugin_dir / "plugin.manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
    write_checksum_to_manifest(plugin_dir)
    return plugin_dir


def test_top_level_public_api_surface_is_stable() -> None:
    import scenario_pipeliner

    assert scenario_pipeliner.__all__ == [
        "CoreMigrationConfig",
        "ScenarioPipelinerConfig",
        "Mode",
        "DbBackend",
        "apply_migrations",
        "apply_core_migrations",
        "apply_core_migrations_async",
        "__version__",
    ]


def test_api_public_surface_is_stable() -> None:
    import scenario_pipeliner.api as public_api

    assert public_api.__all__ == [
        "CoreMigrationConfig",
        "ScenarioPipelinerConfig",
        "Mode",
        "DbBackend",
        "apply_migrations",
        "apply_core_migrations",
        "apply_core_migrations_async",
        "DryRunReport",
        "CoreMigrationReport",
    ]


def test_top_level_apply_migrations_export_works(tmp_path: Path) -> None:
    _prepare_plugin(tmp_path, "acme_docs")
    from scenario_pipeliner import apply_migrations as top_level_apply_migrations

    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    report = top_level_apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "ok"


def test_apply_core_migrations_rejects_running_event_loop() -> None:
    from scenario_pipeliner import apply_core_migrations, apply_core_migrations_async

    config = CoreMigrationConfig(
        db_backend=DbBackend.POSTGRESQL,
        db_schema="sp",
        postgres_host="localhost",
        postgres_db="db",
        postgres_user="user",
        postgres_password="password",
    )

    async def _inside() -> None:
        apply_core_migrations(config)

    with pytest.raises(RuntimeError, match="running event loop"):
        import asyncio

        asyncio.run(_inside())
    assert apply_core_migrations_async is not None


def test_apply_migrations_dry_run_returns_json_report(tmp_path: Path) -> None:
    _prepare_plugin(tmp_path, "acme_docs")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True)
    payload = report.model_dump(mode="json")

    assert payload["status"] == "ok"
    assert payload["summary"]["plugins_loaded"] == 1
    assert payload["summary"]["migrations_planned"] == 1
    assert payload["plugins"][0]["checksum"] == {"status": "ok", "scope": "unpacked"}


def test_cli_migrate_dry_run_json(tmp_path: Path) -> None:
    _prepare_plugin(tmp_path, "acme_docs")
    src_root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["SCENARIO_PIPELINER_PLUGINS_ROOT"] = str(tmp_path)
    env["SCENARIO_PIPELINER_CORE_VERSION"] = "0.1.0"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "scenario_pipeliner.cli",
            "migrate",
            "--dry-run",
            "--format",
            "json",
            "--db-backend",
            "postgresql",
        ],
        cwd=src_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["status"] in {"ok", "warning"}
    assert report["db_backend"] == "postgresql"


def test_cli_returns_1_on_fatal_error(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    (plugin_dir / "migrations" / "postgresql.sql").unlink()
    src_root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["SCENARIO_PIPELINER_PLUGINS_ROOT"] = str(tmp_path)
    env["SCENARIO_PIPELINER_CORE_VERSION"] = "0.1.0"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "scenario_pipeliner.cli",
            "migrate",
            "--dry-run",
            "--format",
            "json",
            "--db-backend",
            "postgresql",
        ],
        cwd=src_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 1, result.stderr
    report = json.loads(result.stdout)
    assert report["status"] == "error"


def test_manifest_invalid_error_code(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "acme_docs"
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "plugin.manifest.json").write_text("{}", encoding="utf-8")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["errors"][0]["code"] == "MANIFEST_INVALID"


def test_manifest_not_found_error_code(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    missing_manifest = tmp_path / "missing_plugin" / "plugin.manifest.json"
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    monkeypatch.setattr(
        dry_run_module, "find_manifest_files", lambda _: [missing_manifest]
    )
    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["errors"][0]["code"] == "MANIFEST_NOT_FOUND"


def test_nested_manifest_is_ignored_by_discovery(tmp_path: Path) -> None:
    _prepare_plugin(tmp_path, "acme_docs")
    nested_dir = tmp_path / "acme_docs" / "nested"
    nested_dir.mkdir(parents=True)
    (nested_dir / "plugin.manifest.json").write_text("{}", encoding="utf-8")
    write_checksum_to_manifest(tmp_path / "acme_docs")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "ok"
    assert report["summary"]["plugins_discovered"] == 1
    assert report["summary"]["plugins_loaded"] == 1
    assert not report["errors"]


def test_core_compat_mismatch_error_code(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    manifest_path = plugin_dir / "plugin.manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["core_compat"] = ">=9.0,<10.0"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["plugins"][0]["plugin_name"] == "acme_docs"
    assert report["plugins"][0]["plugin_version"] == "1.0.0"
    assert report["errors"][0]["code"] == "CORE_COMPAT_MISMATCH"


def test_core_compat_mismatch_does_not_increment_migrations_planned(
    tmp_path: Path,
) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    manifest_path = plugin_dir / "plugin.manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["core_compat"] = ">=9.0,<10.0"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["summary"]["plugins_loaded"] == 0
    assert report["summary"]["migrations_planned"] == 0
    assert report["plugins"][0]["migration_plan"] == []
    assert report["errors"][0]["code"] == "CORE_COMPAT_MISMATCH"


def test_checksum_mismatch_error_code_in_prod(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    (plugin_dir / "plugin.py").write_text(
        "def register():\n    return 'tampered'\n",
        encoding="utf-8",
    )
    config = ScenarioPipelinerConfig(
        mode=Mode.PROD,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["errors"][0]["code"] == "CHECKSUM_MISMATCH"


def test_checksum_status_warning_in_dev_on_mismatch(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    (plugin_dir / "plugin.py").write_text(
        "def register():\n    return 'tampered'\n",
        encoding="utf-8",
    )
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "warning"
    assert report["plugins"][0]["checksum"] == {
        "status": "mismatch",
        "scope": "unpacked",
    }


def test_wheel_scope_with_directory_fails_in_prod(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    manifest_path = plugin_dir / "plugin.manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["checksum"]["scope"] = "wheel"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    config = ScenarioPipelinerConfig(
        mode=Mode.PROD,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["errors"][0]["code"] == "CHECKSUM_SCOPE_MISMATCH"


def test_wheel_scope_with_directory_warns_in_dev(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    manifest_path = plugin_dir / "plugin.manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["checksum"]["scope"] = "wheel"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "warning"
    assert report["summary"]["plugins_loaded"] == 1
    assert report["plugins"][0]["checksum"] == {"status": "mismatch", "scope": "wheel"}


def test_plugin_api_mismatch_error_code(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    manifest_path = plugin_dir / "plugin.manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["plugin_api_version"] = "v2"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["errors"][0]["code"] == "PLUGIN_API_MISMATCH"


def test_missing_backend_migration_file_is_error(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    manifest_path = plugin_dir / "plugin.manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["migrations"]["postgresql"] = "migrations/only_postgres.sql"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["plugins"][0]["load_status"] == "error"
    assert any("does not exist" in reason for reason in report["plugins"][0]["reasons"])


def test_missing_migrations_section_is_skipped_with_warning(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    manifest_path = plugin_dir / "plugin.manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload.pop("migrations", None)
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "warning"
    assert report["summary"]["plugins_loaded"] == 0
    assert report["summary"]["plugins_skipped"] == 1
    assert report["summary"]["migrations_planned"] == 0
    assert report["plugins"][0]["load_status"] == "skipped"
    assert "no migration path for backend=postgresql" in report["plugins"][0]["reasons"]


def test_skipped_plugin_keeps_checksum_warning_reason(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    manifest_path = plugin_dir / "plugin.manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload.pop("migrations", None)
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    (plugin_dir / "plugin.py").write_text(
        "def register():\n    return 'tampered'\n",
        encoding="utf-8",
    )
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")
    reasons = report["plugins"][0]["reasons"]

    assert report["status"] == "warning"
    assert report["plugins"][0]["load_status"] == "skipped"
    assert report["plugins"][0]["checksum"]["status"] == "mismatch"
    assert any("checksum mismatch" in reason for reason in reasons)
    assert any(
        "no migration path for backend=postgresql" in reason for reason in reasons
    )


def test_apply_migrations_dry_run_handles_io_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _prepare_plugin(tmp_path, "acme_docs")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    def _raise_os_error(_: Path) -> object:
        raise OSError("permission denied while reading manifest")

    monkeypatch.setattr(dry_run_module, "load_manifest", _raise_os_error)
    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["summary"]["plugins_loaded"] == 0
    assert report["errors"][0]["code"] == "PLUGIN_LOAD_ERROR"


def test_apply_migrations_dry_run_marks_missing_migration_path(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    (plugin_dir / "migrations" / "postgresql.sql").unlink()
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["summary"]["migrations_planned"] == 0
    assert report["plugins"][0]["load_status"] == "error"
    assert report["errors"][0]["code"] == "MIGRATION_PATH_MISSING"


def test_missing_migration_does_not_poison_registry_for_same_plugin_name(
    tmp_path: Path,
) -> None:
    failed_dir = tmp_path / "a_failed"
    (failed_dir / "migrations").mkdir(parents=True)
    (failed_dir / "plugin.py").write_text(
        "def register():\n    return None\n",
        encoding="utf-8",
    )
    failed_checksum = sha256_directory(failed_dir)
    (failed_dir / "plugin.manifest.json").write_text(
        json.dumps(
            {
                "plugin_name": "acme_docs",
                "plugin_version": "1.0.0",
                "plugin_api_version": "v1",
                "core_compat": ">=0.1,<1.0",
                "checksum": {
                    "algorithm": "sha256",
                    "scope": "unpacked",
                    "value": failed_checksum,
                },
                "entrypoint": "acme_docs.plugin:register",
                "scenarios": ["acme_docs.scenario_9"],
                "migrations": {
                    "migration_order": "20260713110000",
                    "postgresql": "migrations/missing.sql",
                },
            }
        ),
        encoding="utf-8",
    )

    valid_dir = tmp_path / "b_valid"
    (valid_dir / "migrations").mkdir(parents=True)
    (valid_dir / "migrations" / "postgresql.sql").write_text(
        "-- postgresql", encoding="utf-8"
    )
    (valid_dir / "plugin.py").write_text(
        "def register():\n    return None\n", encoding="utf-8"
    )
    valid_checksum = sha256_directory(valid_dir)
    (valid_dir / "plugin.manifest.json").write_text(
        json.dumps(
            {
                "plugin_name": "acme_docs",
                "plugin_version": "1.0.1",
                "plugin_api_version": "v1",
                "core_compat": ">=0.1,<1.0",
                "checksum": {
                    "algorithm": "sha256",
                    "scope": "unpacked",
                    "value": valid_checksum,
                },
                "entrypoint": "acme_docs.plugin:register",
                "scenarios": ["acme_docs.scenario_9"],
                "migrations": {
                    "migration_order": "20260713120000",
                    "postgresql": "migrations/postgresql.sql",
                },
            }
        ),
        encoding="utf-8",
    )

    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["summary"]["plugins_loaded"] == 1
    assert report["summary"]["migrations_planned"] == 1
    assert any(error["code"] == "MIGRATION_PATH_MISSING" for error in report["errors"])
    assert not any(
        "duplicate plugin_name" in error["message"] for error in report["errors"]
    )


def test_migration_path_traversal_is_rejected(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "acme_docs"
    (plugin_dir / "migrations").mkdir(parents=True)
    (plugin_dir / "plugin.py").write_text(
        "def register():\n    return None\n", encoding="utf-8"
    )
    (tmp_path / "outside.sql").write_text("-- outside", encoding="utf-8")
    checksum = sha256_directory(plugin_dir)
    (plugin_dir / "plugin.manifest.json").write_text(
        json.dumps(
            {
                "plugin_name": "acme_docs",
                "plugin_version": "1.0.0",
                "plugin_api_version": "v1",
                "core_compat": ">=0.1,<1.0",
                "checksum": {
                    "algorithm": "sha256",
                    "scope": "unpacked",
                    "value": checksum,
                },
                "entrypoint": "acme_docs.plugin:register",
                "scenarios": ["acme_docs.scenario_9"],
                "migrations": {
                    "migration_order": "20260713120000",
                    "postgresql": "../outside.sql",
                },
            }
        ),
        encoding="utf-8",
    )

    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["summary"]["plugins_loaded"] == 0
    assert report["summary"]["migrations_planned"] == 0
    assert report["errors"][0]["code"] == "MIGRATION_PATH_TRAVERSAL"


def test_absolute_migration_path_is_rejected(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    manifest_path = plugin_dir / "plugin.manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["migrations"]["postgresql"] = str(
        (plugin_dir / "migrations" / "postgresql.sql").resolve()
    )
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["summary"]["plugins_loaded"] == 0
    assert report["summary"]["migrations_planned"] == 0
    assert report["errors"][0]["code"] == "MIGRATION_PATH_ABSOLUTE"


def test_migration_path_directory_is_rejected_as_not_file(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "acme_docs"
    (plugin_dir / "migrations" / "postgresql").mkdir(parents=True)
    (plugin_dir / "plugin.py").write_text(
        "def register():\n    return None\n", encoding="utf-8"
    )
    checksum = sha256_directory(plugin_dir)
    (plugin_dir / "plugin.manifest.json").write_text(
        json.dumps(
            {
                "plugin_name": "acme_docs",
                "plugin_version": "1.0.0",
                "plugin_api_version": "v1",
                "core_compat": ">=0.1,<1.0",
                "checksum": {
                    "algorithm": "sha256",
                    "scope": "unpacked",
                    "value": checksum,
                },
                "entrypoint": "acme_docs.plugin:register",
                "scenarios": ["acme_docs.scenario_9"],
                "migrations": {
                    "migration_order": "20260713120000",
                    "postgresql": "migrations/postgresql",
                },
            }
        ),
        encoding="utf-8",
    )

    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["summary"]["plugins_loaded"] == 0
    assert report["summary"]["migrations_planned"] == 0
    assert report["errors"][0]["code"] == "MIGRATION_PATH_NOT_FILE"


def test_plugins_sorted_globally_by_migration_order(tmp_path: Path) -> None:
    first = _prepare_plugin(tmp_path, "z_plugin")
    second = _prepare_plugin(tmp_path, "a_plugin")

    first_manifest = first / "plugin.manifest.json"
    first_payload = json.loads(first_manifest.read_text(encoding="utf-8"))
    first_payload["migrations"]["migration_order"] = "20260713130000"
    first_manifest.write_text(json.dumps(first_payload), encoding="utf-8")

    second_manifest = second / "plugin.manifest.json"
    second_payload = json.loads(second_manifest.read_text(encoding="utf-8"))
    second_payload["migrations"]["migration_order"] = "20260713110000"
    second_manifest.write_text(json.dumps(second_payload), encoding="utf-8")

    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    loaded_plugins = [p for p in report["plugins"] if p["load_status"] == "loaded"]
    assert [p["plugin_name"] for p in loaded_plugins] == ["a_plugin", "z_plugin"]


def test_cli_build_parser_handles_empty_postgres_port_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scenario_pipeliner.cli import build_parser

    monkeypatch.setenv("POSTGRES_PORT", "")
    parser = build_parser()
    args = parser.parse_args(
        [
            "db",
            "migrate-core",
            "--db-backend",
            "postgresql",
        ]
    )
    assert args.postgres_port == 5432


def test_cli_build_parser_rejects_invalid_postgres_port_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from scenario_pipeliner.cli import build_parser, main

    monkeypatch.setenv("POSTGRES_PORT", "abc")
    with pytest.raises(ValueError, match="POSTGRES_PORT"):
        build_parser()
    assert main(["db", "migrate-core", "--db-backend", "postgresql"]) == 2


def test_apply_core_migrations_postgres_creates_schema_tables(
    postgres_params: dict,
    isolated_schema: str,
) -> None:
    report = apply_core_migrations(
        CoreMigrationConfig(
            db_backend=DbBackend.POSTGRESQL,
            db_schema=isolated_schema,
            postgres_host=str(postgres_params["host"]),
            postgres_port=int(postgres_params["port"]),
            postgres_db=str(postgres_params["database"]),
            postgres_user=str(postgres_params["user"]),
            postgres_password=str(postgres_params["password"]),
        )
    )
    assert report.db_backend == DbBackend.POSTGRESQL
    assert report.db_schema == isolated_schema
    assert set(report.tables) >= {
        "tasks",
        "settings",
        "results",
        "broker_ingress",
        "broker_outbox",
        "plugin_migrations",
    }
    apply_core_migrations(
        CoreMigrationConfig(
            db_backend=DbBackend.POSTGRESQL,
            db_schema=isolated_schema,
            postgres_host=str(postgres_params["host"]),
            postgres_port=int(postgres_params["port"]),
            postgres_db=str(postgres_params["database"]),
            postgres_user=str(postgres_params["user"]),
            postgres_password=str(postgres_params["password"]),
        )
    )


def test_cli_db_migrate_core_postgresql_smoke(
    postgres_params: dict,
    isolated_schema: str,
) -> None:
    src_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "scenario_pipeliner.cli",
            "db",
            "migrate-core",
            "--db-backend",
            "postgresql",
            "--db-schema",
            isolated_schema,
            "--postgres-host",
            str(postgres_params["host"]),
            "--postgres-port",
            str(postgres_params["port"]),
            "--postgres-db",
            str(postgres_params["database"]),
            "--postgres-user",
            str(postgres_params["user"]),
            "--postgres-password",
            str(postgres_params["password"]),
        ],
        cwd=src_root,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["db_backend"] == "postgresql"
    assert payload["db_schema"] == isolated_schema
    assert set(payload["tables"]) >= {"tasks", "settings", "results"}


def test_cli_db_migrate_core_postgresql_url_smoke(
    postgres_urls: PostgresUrls,
    isolated_schema: str,
) -> None:
    src_root = Path(__file__).resolve().parents[2]
    url = postgres_urls.async_url
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "scenario_pipeliner.cli",
            "db",
            "migrate-core",
            "--db-backend",
            "postgresql",
            "--db-schema",
            isolated_schema,
            "--postgres-url",
            url,
        ],
        cwd=src_root,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["db_schema"] == isolated_schema


def test_apply_plugin_migrations_uses_schema_placeholders(
    tmp_path: Path,
    postgres_params: dict,
    postgres_urls: PostgresUrls,
    isolated_schema: str,
    schema_janitor: list[str],
) -> None:
    import asyncio

    from sqlalchemy import text

    from scenario_pipeliner.core.plugin_migrate import apply_plugin_migrations_async
    from scenario_pipeliner.db.engine import create_async_engine_for_schema
    from scenario_pipeliner.db.schemes_names import plugin_schema_name

    plugin_name = "acme_docs"
    plugin_schema = plugin_schema_name(plugin_name)
    schema_janitor.append(plugin_schema)
    plugin_dir = _prepare_plugin(tmp_path, plugin_name)
    (plugin_dir / "migrations" / "postgresql.sql").write_text(
        "CREATE TABLE IF NOT EXISTS {{plugin_schema}}.events (\n"
        "    id BIGSERIAL PRIMARY KEY,\n"
        "    task_id BIGINT NULL REFERENCES {{core_schema}}.tasks(id)\n"
        ");\n",
        encoding="utf-8",
    )
    apply_core_migrations(
        CoreMigrationConfig(
            db_backend=DbBackend.POSTGRESQL,
            db_schema=isolated_schema,
            postgres_host=str(postgres_params["host"]),
            postgres_port=int(postgres_params["port"]),
            postgres_db=str(postgres_params["database"]),
            postgres_user=str(postgres_params["user"]),
            postgres_password=str(postgres_params["password"]),
        )
    )
    engine = create_async_engine_for_schema(
        postgres_urls.async_url,
        isolated_schema,
    )
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
        db_schema=isolated_schema,
    )

    async def _apply_and_check() -> str | None:
        try:
            applied = await apply_plugin_migrations_async(config, engine)
            assert applied == [plugin_name]
            async with engine.connect() as conn:
                exists = await conn.execute(
                    text(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = :schema AND table_name = 'events'"
                    ),
                    {"schema": plugin_schema},
                )
                return exists.scalar_one_or_none()
        finally:
            await engine.dispose()

    assert asyncio.run(_apply_and_check()) == 1


def test_apply_plugin_migrations_names_sql_file_on_template_error(
    tmp_path: Path,
) -> None:
    import asyncio
    from unittest.mock import MagicMock

    from scenario_pipeliner.core.plugin_migrate import apply_plugin_migrations_async
    from scenario_pipeliner.db.exceptions import PluginSqlTemplateError

    _prepare_plugin(tmp_path, "acme_docs")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
        db_schema="sp",
    )
    with pytest.raises(PluginSqlTemplateError, match=r"acme_docs .*must contain both"):
        asyncio.run(apply_plugin_migrations_async(config, MagicMock()))


def test_collect_plugin_migration_plans_rejects_traversal(tmp_path: Path) -> None:
    from scenario_pipeliner.core.exceptions import DryRunPluginError
    from scenario_pipeliner.core.plugin_migrate import collect_plugin_migration_plans

    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    payload = json.loads(
        (plugin_dir / "plugin.manifest.json").read_text(encoding="utf-8")
    )
    payload["migrations"]["postgresql"] = "../outside.sql"
    (plugin_dir / "plugin.manifest.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    with pytest.raises(DryRunPluginError) as exc:
        collect_plugin_migration_plans(config)
    assert exc.value.code is DryRunErrorCode.MIGRATION_PATH_TRAVERSAL


def test_collect_plugin_migration_plans_rejects_duplicate_plugin_name(
    tmp_path: Path,
) -> None:
    from scenario_pipeliner.core.exceptions import DryRunPluginError
    from scenario_pipeliner.core.plugin_migrate import collect_plugin_migration_plans

    _prepare_plugin(tmp_path, "acme_docs", dirname="acme_a")
    _prepare_plugin(tmp_path, "acme_docs", dirname="acme_b")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    with pytest.raises(DryRunPluginError) as exc:
        collect_plugin_migration_plans(config)
    assert exc.value.code is DryRunErrorCode.PLUGIN_NAME_CONFLICT


def test_collect_plugin_migration_plans_rejects_schema_collision(
    tmp_path: Path,
) -> None:
    from scenario_pipeliner.core.exceptions import DryRunPluginError
    from scenario_pipeliner.core.plugin_migrate import collect_plugin_migration_plans

    _prepare_plugin(tmp_path, "track-documents")
    _prepare_plugin(tmp_path, "track.documents")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    with pytest.raises(DryRunPluginError) as exc:
        collect_plugin_migration_plans(config)
    assert exc.value.code is DryRunErrorCode.PLUGIN_SCHEMA_CONFLICT
    assert "track_documents" in str(exc.value)


def test_dry_run_rejects_plugin_schema_collision(tmp_path: Path) -> None:
    _prepare_plugin(tmp_path, "track-documents")
    _prepare_plugin(tmp_path, "track.documents")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    report = apply_migrations(config, dry_run=True)
    assert report.status.value == "error"
    assert any(
        error.code is DryRunErrorCode.PLUGIN_SCHEMA_CONFLICT for error in report.errors
    )


def test_apply_plugin_migrations_skips_unchanged_sql(
    tmp_path: Path,
    postgres_params: dict,
    postgres_urls: PostgresUrls,
    isolated_schema: str,
    schema_janitor: list[str],
) -> None:
    import asyncio

    from sqlalchemy import text

    from scenario_pipeliner.core.plugin_migrate import apply_plugin_migrations_async
    from scenario_pipeliner.db.engine import create_async_engine_for_schema
    from scenario_pipeliner.db.exceptions import PluginMigrationConflictError
    from scenario_pipeliner.db.schemes_names import plugin_schema_name

    plugin_name = "acme_docs"
    plugin_schema = plugin_schema_name(plugin_name)
    schema_janitor.append(plugin_schema)
    plugin_dir = _prepare_plugin(tmp_path, plugin_name)
    sql_path = plugin_dir / "migrations" / "postgresql.sql"
    sql_path.write_text(
        "-- {{core_schema}} / {{plugin_schema}}\n"
        "CREATE TABLE IF NOT EXISTS {{plugin_schema}}.events (\n"
        "    id BIGSERIAL PRIMARY KEY,\n"
        "    marker TEXT NOT NULL\n"
        ");\n"
        "INSERT INTO {{plugin_schema}}.events (marker) VALUES ('once');\n",
        encoding="utf-8",
    )
    apply_core_migrations(
        CoreMigrationConfig(
            db_backend=DbBackend.POSTGRESQL,
            db_schema=isolated_schema,
            postgres_host=str(postgres_params["host"]),
            postgres_port=int(postgres_params["port"]),
            postgres_db=str(postgres_params["database"]),
            postgres_user=str(postgres_params["user"]),
            postgres_password=str(postgres_params["password"]),
        )
    )
    engine = create_async_engine_for_schema(
        postgres_urls.async_url,
        isolated_schema,
    )
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.POSTGRESQL,
        plugins_root=tmp_path,
        core_version="0.1.0",
        db_schema=isolated_schema,
    )

    async def _run() -> tuple[list[str], list[str], int, int]:
        try:
            first = await apply_plugin_migrations_async(config, engine)
            second = await apply_plugin_migrations_async(config, engine)
            async with engine.connect() as conn:
                count = (
                    await conn.execute(
                        text(f"SELECT count(*) FROM {plugin_schema}.events")
                    )
                ).scalar_one()
                ledger = (
                    await conn.execute(text("SELECT count(*) FROM plugin_migrations"))
                ).scalar_one()
            sql_path.write_text(
                sql_path.read_text(encoding="utf-8") + "-- changed\n",
                encoding="utf-8",
            )
            with pytest.raises(PluginMigrationConflictError, match="without bumping"):
                await apply_plugin_migrations_async(config, engine)
            return first, second, int(count), int(ledger)
        finally:
            await engine.dispose()

    first, second, count, ledger = asyncio.run(_run())
    assert first == [plugin_name]
    assert second == []
    assert count == 1
    assert ledger == 1
