import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import asyncpg
import pytest

from scenario_pipeliner.api.config import CoreMigrationConfig, ScenarioPipelinerConfig
from scenario_pipeliner.api.core_migrate import apply_core_migrations
from scenario_pipeliner.api.enums import DbBackend, Mode
from scenario_pipeliner.api.migrate import apply_migrations
from scenario_pipeliner.core import dry_run as dry_run_module
from scenario_pipeliner.core.checksum import sha256_directory


def _prepare_plugin(tmp_path: Path, plugin_name: str) -> Path:
    plugin_dir = tmp_path / plugin_name
    (plugin_dir / "migrations").mkdir(parents=True)
    (plugin_dir / "migrations" / "sqlite.sql").write_text("-- sqlite", encoding="utf-8")
    (plugin_dir / "plugin.py").write_text(
        "def register():\n    return None\n", encoding="utf-8"
    )
    checksum = sha256_directory(plugin_dir)

    manifest = {
        "plugin_name": plugin_name,
        "plugin_version": "1.0.0",
        "plugin_api_version": "v1",
        "core_compat": ">=0.1,<1.0",
        "checksum": {
            "algorithm": "sha256",
            "scope": "unpacked",
            "value": checksum,
        },
        "entrypoint": f"{plugin_name}.plugin:register",
        "scenarios": [f"{plugin_name}.scenario_9"],
        "migrations": {
            "migration_order": "20260713120000",
            "sqlite": "migrations/sqlite.sql",
        },
    }
    (plugin_dir / "plugin.manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )
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
        "DryRunReport",
        "CoreMigrationReport",
    ]


def test_top_level_apply_migrations_export_works(tmp_path: Path) -> None:
    _prepare_plugin(tmp_path, "acme_docs")
    from scenario_pipeliner import apply_migrations as top_level_apply_migrations

    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.SQLITE,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    report = top_level_apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "ok"


def test_apply_migrations_dry_run_returns_json_report(tmp_path: Path) -> None:
    _prepare_plugin(tmp_path, "acme_docs")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.SQLITE,
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
            "sqlite",
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
    assert report["db_backend"] == "sqlite"


def test_cli_returns_1_on_fatal_error(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    (plugin_dir / "migrations" / "sqlite.sql").unlink()
    src_root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env["SCENARIO_PIPELINER_PLUGINS_ROOT"] = str(tmp_path)

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
            "sqlite",
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
        db_backend=DbBackend.SQLITE,
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
        db_backend=DbBackend.SQLITE,
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
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.SQLITE,
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
        db_backend=DbBackend.SQLITE,
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
        db_backend=DbBackend.SQLITE,
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
        db_backend=DbBackend.SQLITE,
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
        db_backend=DbBackend.SQLITE,
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
        db_backend=DbBackend.SQLITE,
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
        db_backend=DbBackend.SQLITE,
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
        db_backend=DbBackend.SQLITE,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "error"
    assert report["errors"][0]["code"] == "PLUGIN_API_MISMATCH"


def test_missing_backend_migration_path_is_skipped_with_warning(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    manifest_path = plugin_dir / "plugin.manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["migrations"].pop("sqlite", None)
    payload["migrations"]["postgresql"] = "migrations/only_postgres.sql"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.SQLITE,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    assert report["status"] == "warning"
    assert report["summary"]["plugins_loaded"] == 0
    assert report["summary"]["plugins_skipped"] == 1
    assert report["summary"]["migrations_planned"] == 0
    assert report["plugins"][0]["load_status"] == "skipped"


def test_skipped_plugin_keeps_checksum_warning_reason(tmp_path: Path) -> None:
    plugin_dir = _prepare_plugin(tmp_path, "acme_docs")
    manifest_path = plugin_dir / "plugin.manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["migrations"].pop("sqlite", None)
    payload["migrations"]["postgresql"] = "migrations/only_postgres.sql"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    (plugin_dir / "plugin.py").write_text(
        "def register():\n    return 'tampered'\n",
        encoding="utf-8",
    )
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.SQLITE,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )

    report = apply_migrations(config, dry_run=True).model_dump(mode="json")
    reasons = report["plugins"][0]["reasons"]

    assert report["status"] == "warning"
    assert report["plugins"][0]["load_status"] == "skipped"
    assert report["plugins"][0]["checksum"]["status"] == "mismatch"
    assert any("checksum mismatch" in reason for reason in reasons)
    assert any("no migration path for backend=sqlite" in reason for reason in reasons)


def test_apply_migrations_dry_run_handles_io_errors(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _prepare_plugin(tmp_path, "acme_docs")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.SQLITE,
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
    (plugin_dir / "migrations" / "sqlite.sql").unlink()
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.SQLITE,
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
                    "sqlite": "migrations/missing.sql",
                },
            }
        ),
        encoding="utf-8",
    )

    valid_dir = tmp_path / "b_valid"
    (valid_dir / "migrations").mkdir(parents=True)
    (valid_dir / "migrations" / "sqlite.sql").write_text("-- sqlite", encoding="utf-8")
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
                    "sqlite": "migrations/sqlite.sql",
                },
            }
        ),
        encoding="utf-8",
    )

    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.SQLITE,
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
                    "sqlite": "../outside.sql",
                },
            }
        ),
        encoding="utf-8",
    )

    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.SQLITE,
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
    payload["migrations"]["sqlite"] = str(
        (plugin_dir / "migrations" / "sqlite.sql").resolve()
    )
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.SQLITE,
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
    (plugin_dir / "migrations" / "sqlite").mkdir(parents=True)
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
                    "sqlite": "migrations/sqlite",
                },
            }
        ),
        encoding="utf-8",
    )

    config = ScenarioPipelinerConfig(
        mode=Mode.DEV,
        db_backend=DbBackend.SQLITE,
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
        db_backend=DbBackend.SQLITE,
        plugins_root=tmp_path,
        core_version="0.1.0",
    )
    report = apply_migrations(config, dry_run=True).model_dump(mode="json")

    loaded_plugins = [p for p in report["plugins"] if p["load_status"] == "loaded"]
    assert [p["plugin_name"] for p in loaded_plugins] == ["a_plugin", "z_plugin"]


def test_apply_core_migrations_sqlite_creates_tables_and_default_settings(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "core.sqlite3"

    report = apply_core_migrations(
        CoreMigrationConfig(
            db_backend=DbBackend.SQLITE,
            sqlite_path=db_path,
        )
    )

    assert report.db_backend == DbBackend.SQLITE
    assert set(report.tables) == {"tasks", "settings", "results"}
    assert report.default_settings["pipeline_active_default"] == "1"
    assert report.default_settings["worker_enabled"] == "1"
    # A second apply must be idempotent.
    apply_core_migrations(
        CoreMigrationConfig(
            db_backend=DbBackend.SQLITE,
            sqlite_path=db_path,
        )
    )

    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND "
                "name IN ('tasks', 'settings', 'results', 'alembic_version')"
            )
        }
        assert tables == {"tasks", "settings", "results", "alembic_version"}

        seeded = dict(
            conn.execute(
                "SELECT key, value FROM settings WHERE key IN (?, ?)",
                ("pipeline_active_default", "worker_enabled"),
            ).fetchall()
        )
        assert seeded == {"pipeline_active_default": "1", "worker_enabled": "1"}
        seeded_count = conn.execute(
            "SELECT COUNT(1) FROM settings WHERE key IN (?, ?)",
            ("pipeline_active_default", "worker_enabled"),
        ).fetchone()
        assert seeded_count is not None
        assert seeded_count[0] == 2

        version = conn.execute(
            "SELECT version_num FROM alembic_version LIMIT 1"
        ).fetchone()
        assert version is not None
        assert version[0] == "0001_core_tables"


def test_cli_db_migrate_core_sqlite(tmp_path: Path) -> None:
    src_root = Path(__file__).resolve().parents[2]
    db_path = tmp_path / "cli_core.sqlite3"
    env = os.environ.copy()

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "scenario_pipeliner.cli",
            "db",
            "migrate-core",
            "--db-backend",
            "sqlite",
            "--sqlite-path",
            str(db_path),
        ],
        cwd=src_root,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["db_backend"] == "sqlite"
    assert set(payload["tables"]) == {"tasks", "settings", "results"}

    with sqlite3.connect(db_path) as conn:
        count = conn.execute("SELECT COUNT(1) FROM settings").fetchone()
        assert count is not None
        assert count[0] >= 2


def test_apply_core_migrations_backward_compatible_kwargs(tmp_path: Path) -> None:
    db_path = tmp_path / "core_kwargs.sqlite3"

    report = apply_core_migrations(
        db_backend=DbBackend.SQLITE,
        sqlite_path=str(db_path),
    )

    assert report.db_backend == DbBackend.SQLITE
    with sqlite3.connect(db_path) as conn:
        table = conn.execute(
            "SELECT name FROM sqlite_master WHERE name='alembic_version'"
        ).fetchone()
        assert table is not None


def test_cli_db_migrate_core_postgresql_smoke() -> None:
    if os.getenv("SCENARIO_PIPELINER_RUN_POSTGRES_TESTS") != "1":
        pytest.skip("set SCENARIO_PIPELINER_RUN_POSTGRES_TESTS=1 to run postgres smoke")

    host = os.getenv("POSTGRES_HOST", "172.17.0.5")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "db")
    user = os.getenv("POSTGRES_USER", "user")
    password = os.getenv("POSTGRES_PASSWORD", "password")

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
            "--postgres-host",
            host,
            "--postgres-port",
            port,
            "--postgres-db",
            database,
            "--postgres-user",
            user,
            "--postgres-password",
            password,
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
    assert set(payload["tables"]) == {"tasks", "settings", "results"}

    async def _verify() -> None:
        conn = await asyncpg.connect(
            host=host,
            port=int(port),
            database=database,
            user=user,
            password=password,
        )
        try:
            tables = await conn.fetch(
                """
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                  AND tablename = ANY($1::text[])
                """,
                ["tasks", "settings", "results", "alembic_version"],
            )
            assert {row["tablename"] for row in tables} == {
                "tasks",
                "settings",
                "results",
                "alembic_version",
            }
            version = await conn.fetchval(
                "SELECT version_num FROM alembic_version LIMIT 1"
            )
            assert version == "0001_core_tables"
        finally:
            await conn.close()

    import asyncio

    asyncio.run(_verify())
