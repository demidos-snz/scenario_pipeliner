from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scenario_pipeliner.api.enums import ChecksumAlgorithm, ChecksumManifestScope
from scenario_pipeliner.core.checksum import sha256_directory
from scenario_pipeliner.core.manifest_loader import MANIFEST_FILE_NAME


def compute_plugin_checksum(plugin_dir: Path) -> str:
    """Compute unpacked-tree sha256 for a plugin directory."""
    resolved = plugin_dir.expanduser().resolve()
    if not resolved.is_dir():
        raise FileNotFoundError(f"plugin directory not found: {resolved.as_posix()}")
    return sha256_directory(resolved)


def manifest_path_for_plugin(plugin_dir: Path) -> Path:
    return plugin_dir.expanduser().resolve() / MANIFEST_FILE_NAME


def write_checksum_to_manifest(
    plugin_dir: Path,
    *,
    value: str | None = None,
    algorithm: ChecksumAlgorithm = ChecksumAlgorithm.SHA256,
    scope: ChecksumManifestScope = ChecksumManifestScope.UNPACKED,
) -> tuple[Path, str]:
    """Update checksum fields in an existing plugin.manifest.json.

    Returns ``(manifest_path, checksum_value)``.
    """
    plugin_path = plugin_dir.expanduser().resolve()
    manifest_path = manifest_path_for_plugin(plugin_path)
    if not manifest_path.is_file():
        raise FileNotFoundError(
            f"manifest not found: {manifest_path.as_posix()} "
            "(create plugin.manifest.json before --write)"
        )

    checksum_value = (
        value if value is not None else compute_plugin_checksum(plugin_path)
    )
    raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"manifest root must be a JSON object: {manifest_path}")

    checksum_payload: dict[str, Any] = {
        "algorithm": algorithm.value,
        "scope": scope.value,
        "value": checksum_value,
    }
    existing = raw.get("checksum")
    if isinstance(existing, dict):
        merged = dict(existing)
        merged.update(checksum_payload)
        raw["checksum"] = merged
    else:
        raw["checksum"] = checksum_payload

    manifest_path.write_text(
        json.dumps(raw, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest_path, checksum_value
