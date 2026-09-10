from __future__ import annotations

import json
from pathlib import Path

from pydantic import ValidationError

from scenario_pipeliner.api.settings import PluginManifestV1
from scenario_pipeliner.core.exceptions import (
    ManifestInvalidError,
    ManifestNotFoundError,
)

MANIFEST_FILE_NAME = "plugin.manifest.json"


def find_manifest_files(plugins_root: Path) -> list[Path]:
    if not plugins_root.exists():
        return []
    return sorted(plugins_root.glob(f"*/{MANIFEST_FILE_NAME}"))


def load_manifest(path: Path) -> PluginManifestV1:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise ManifestNotFoundError(f"manifest not found: {path.as_posix()}") from e
    except json.JSONDecodeError as e:
        raise ManifestInvalidError(f"invalid JSON manifest {path}: {e}") from e
    if isinstance(raw, dict):
        raw = {**raw, "manifest_path": path}
    try:
        manifest = PluginManifestV1.model_validate(raw)
    except ValidationError as e:
        raise ManifestInvalidError(f"invalid manifest {path}: {e}") from e
    return manifest


def list_plugin_scenarios(plugins_root: Path) -> tuple[list[dict[str, str]], list[str]]:
    """Return ``(scenarios, errors)`` from manifests under ``plugins_root``.

    Does not import plugin Python. Invalid manifests are skipped and listed in
    ``errors``.
    """
    scenarios: list[dict[str, str]] = []
    errors: list[str] = []
    root = plugins_root.expanduser().resolve()
    for path in find_manifest_files(root):
        try:
            manifest = load_manifest(path)
        except (ManifestInvalidError, ManifestNotFoundError) as exc:
            errors.append(str(exc))
            continue
        plugin_dir = path.parent.as_posix()
        for scenario in manifest.scenarios:
            scenarios.append(
                {
                    "scenario": scenario,
                    "plugin_name": manifest.plugin_name,
                    "plugin_dir": plugin_dir,
                }
            )
    return scenarios, errors
