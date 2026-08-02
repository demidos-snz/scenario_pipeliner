from __future__ import annotations

import hashlib
from pathlib import Path


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
    """Deterministic directory digest based on file names and bytes."""

    hasher = hashlib.sha256()
    ignored_names = {"plugin.manifest.json", "__pycache__", ".DS_Store"}
    ignored_suffixes = {".pyc"}
    for file_path in sorted(path.rglob("*")):
        # Symlinks are excluded to keep digest bound to plugin tree bytes only.
        if file_path.is_symlink():
            continue
        if not file_path.is_file():
            continue
        if any(part in ignored_names for part in file_path.parts):
            continue
        if file_path.suffix in ignored_suffixes:
            continue
        rel = file_path.relative_to(path).as_posix().encode("utf-8")
        hasher.update(rel)
        hasher.update(b"\0")
        hasher.update(sha256_file(file_path).encode("ascii"))
        hasher.update(b"\n")
    return hasher.hexdigest()
