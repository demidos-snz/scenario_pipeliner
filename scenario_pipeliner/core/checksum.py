from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from scenario_pipeliner.core.manifest_loader import MANIFEST_FILE_NAME

_IGNORED_NAMES = frozenset({"__pycache__", ".DS_Store"})
_IGNORED_SUFFIXES = frozenset({".pyc"})


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(65536)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def sha256_directory(path: Path) -> str:
    """Deterministic directory digest based on relative paths and file bytes.

    ``plugin.manifest.json`` is included as canonical JSON with
    ``checksum.value`` stripped so ``plugin checksum --write`` is stable.
    Ignore rules apply to paths relative to ``path``, not the absolute prefix.
    """
    hasher = hashlib.sha256()
    root = path.resolve()
    for file_path in sorted(root.rglob("*")):
        # Symlinks are excluded to keep digest bound to plugin tree bytes only.
        if file_path.is_symlink():
            continue
        if not file_path.is_file():
            continue
        rel = file_path.relative_to(root)
        if any(part in _IGNORED_NAMES for part in rel.parts):
            continue
        if file_path.suffix in _IGNORED_SUFFIXES:
            continue
        hasher.update(rel.as_posix().encode("utf-8"))
        hasher.update(b"\0")
        if rel.as_posix() == MANIFEST_FILE_NAME:
            hasher.update(_canonical_manifest_digest(file_path).encode("ascii"))
        else:
            hasher.update(sha256_file(file_path).encode("ascii"))
        hasher.update(b"\n")
    return hasher.hexdigest()


def _canonical_manifest_digest(path: Path) -> str:
    raw: Any = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        checksum = raw.get("checksum")
        if isinstance(checksum, dict):
            stripped = {key: value for key, value in checksum.items() if key != "value"}
            raw = {**raw, "checksum": stripped}
    canonical = json.dumps(
        raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
