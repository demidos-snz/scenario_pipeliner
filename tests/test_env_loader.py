from __future__ import annotations

import json
import os
from pathlib import Path

from scenario_pipeliner.cli import main
from scenario_pipeliner.core.checksum import sha256_directory
from scenario_pipeliner.env_loader import load_environment_file


def test_load_environment_file_reads_dotenv(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SCENARIO_PIPELINER_PLUGINS_ROOT", raising=False)
    monkeypatch.delenv("SCENARIO_PIPELINER_CORE_VERSION", raising=False)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "SCENARIO_PIPELINER_PLUGINS_ROOT=/tmp/from-dotenv-plugins",
                "SCENARIO_PIPELINER_CORE_VERSION=0.0.1",
                "",
            ]
        ),
        encoding="utf-8",
    )

    loaded = load_environment_file()

    assert loaded == (tmp_path / ".env").resolve()
    assert os.environ["SCENARIO_PIPELINER_PLUGINS_ROOT"] == "/tmp/from-dotenv-plugins"
    assert os.environ["SCENARIO_PIPELINER_CORE_VERSION"] == "0.0.1"


def test_load_environment_file_does_not_override_existing(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SCENARIO_PIPELINER_CORE_VERSION", "9.9.9")
    (tmp_path / ".env").write_text(
        "SCENARIO_PIPELINER_CORE_VERSION=0.0.1\n",
        encoding="utf-8",
    )

    load_environment_file(override=False)

    assert os.environ["SCENARIO_PIPELINER_CORE_VERSION"] == "9.9.9"


def test_cli_migrate_uses_dotenv_plugins_root(tmp_path: Path, monkeypatch) -> None:
    plugins_root = tmp_path / "plugins"
    plugin_dir = plugins_root / "acme_docs"
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "plugin.py").write_text(
        "def register():\n    return None\n", encoding="utf-8"
    )
    checksum = sha256_directory(plugin_dir)
    manifest = {
        "plugin_name": "acme_docs",
        "plugin_version": "1.0.0",
        "plugin_api_version": "v1",
        "core_compat": ">=0.0.1,<1.0",
        "checksum": {
            "algorithm": "sha256",
            "scope": "unpacked",
            "value": checksum,
        },
        "entrypoint": "acme_docs.plugin:register",
        "scenarios": ["acme_docs.scenario_9"],
        "migrations": {
            "migration_order": "20260713120000",
            "postgresql": "migration.sql",
        },
    }
    (plugin_dir / "migration.sql").write_text("-- ok\n", encoding="utf-8")
    (plugin_dir / "plugin.manifest.json").write_text(
        json.dumps(manifest), encoding="utf-8"
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SCENARIO_PIPELINER_PLUGINS_ROOT", raising=False)
    monkeypatch.delenv("SCENARIO_PIPELINER_CORE_VERSION", raising=False)
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                f"SCENARIO_PIPELINER_PLUGINS_ROOT={plugins_root.as_posix()}",
                "SCENARIO_PIPELINER_CORE_VERSION=0.0.1",
                "",
            ]
        ),
        encoding="utf-8",
    )

    code = main(
        [
            "migrate",
            "--dry-run",
            "--db-backend",
            "postgresql",
        ]
    )
    assert code == 0
