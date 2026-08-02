from __future__ import annotations

import json
from pathlib import Path

from pydantic import ValidationError

from scenario_pipeliner.api.models import PluginManifestV1
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
