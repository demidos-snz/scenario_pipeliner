from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from scenario_pipeliner.core.checksum import sha256_directory
from scenario_pipeliner.core.plugin_checksum import (
    compute_plugin_checksum,
    write_checksum_to_manifest,
)


def _write_plugin(tmp_path: Path, plugin_name: str, *, checksum_value: str) -> Path:
    plugin_dir = tmp_path / plugin_name
    plugin_dir.mkdir(parents=True)
    (plugin_dir / "plugin.py").write_text(
        "def register(registry, context=None):\n    return None\n",
        encoding="utf-8",
    )
    (plugin_dir / "migration.sql").write_text("-- hello\n", encoding="utf-8")
    manifest = {
        "plugin_name": plugin_name,
        "plugin_version": "1.0.0",
        "plugin_api_version": "v1",
        "core_compat": ">=0.1,<1.0",
        "checksum": {
            "algorithm": "sha256",
            "scope": "unpacked",
            "value": checksum_value,
        },
        "entrypoint": f"{plugin_name}.plugin:register",
        "scenarios": [f"{plugin_name}.ping"],
        "migrations": {
            "migration_order": "20260803120000",
            "postgresql": "migration.sql",
            "sqlite": "migration.sql",
        },
    }
    (plugin_dir / "plugin.manifest.json").write_text(
        json.dumps(manifest, indent=2) + "\n",
        encoding="utf-8",
    )
    return plugin_dir


def test_compute_plugin_checksum_matches_sha256_directory(tmp_path: Path) -> None:
    plugin_dir = _write_plugin(tmp_path, "demo_plugin", checksum_value="0" * 64)
    assert compute_plugin_checksum(plugin_dir) == sha256_directory(plugin_dir)


def test_write_checksum_to_manifest_updates_value(tmp_path: Path) -> None:
    plugin_dir = _write_plugin(tmp_path, "demo_plugin", checksum_value="0" * 64)
    expected = sha256_directory(plugin_dir)

    manifest_path, value = write_checksum_to_manifest(plugin_dir)

    assert value == expected
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["checksum"]["value"] == expected
    assert payload["checksum"]["algorithm"] == "sha256"
    assert payload["checksum"]["scope"] == "unpacked"
    assert payload["plugin_name"] == "demo_plugin"


def test_cli_plugin_checksum_print(tmp_path: Path) -> None:
    plugin_dir = _write_plugin(tmp_path, "demo_plugin", checksum_value="0" * 64)
    expected = sha256_directory(plugin_dir)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "scenario_pipeliner.cli",
            "plugin",
            "checksum",
            str(plugin_dir),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["value"] == expected
    assert payload["written"] is False


def test_cli_plugin_checksum_write(tmp_path: Path) -> None:
    plugin_dir = _write_plugin(tmp_path, "demo_plugin", checksum_value="0" * 64)
    expected = sha256_directory(plugin_dir)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "scenario_pipeliner.cli",
            "plugin",
            "checksum",
            str(plugin_dir),
            "--write",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["written"] is True
    assert payload["value"] == expected
    written = json.loads(
        (plugin_dir / "plugin.manifest.json").read_text(encoding="utf-8")
    )
    assert written["checksum"]["value"] == expected


def test_cli_plugin_checksum_write_requires_manifest(tmp_path: Path) -> None:
    plugin_dir = tmp_path / "no_manifest"
    plugin_dir.mkdir()
    (plugin_dir / "plugin.py").write_text("x = 1\n", encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "scenario_pipeliner.cli",
            "plugin",
            "checksum",
            str(plugin_dir),
            "--write",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "manifest not found" in result.stderr
