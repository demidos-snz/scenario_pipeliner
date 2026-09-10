from __future__ import annotations

import re

from scenario_pipeliner.db.exceptions import InvalidSchemaNameError

DEFAULT_DB_SCHEMA = "sp"

_SCHEMA_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")
_RESERVED_SCHEMAS = frozenset({"public", "information_schema", "pg_catalog"})
_MAX_SCHEMA_LENGTH = 63


def validate_schema_name(name: str) -> str:
    """Return ``name`` if it is a safe unquoted PostgreSQL schema identifier."""
    candidate = name.strip()
    if not candidate:
        raise InvalidSchemaNameError("schema name must be a non-empty identifier")
    if len(candidate) > _MAX_SCHEMA_LENGTH:
        raise InvalidSchemaNameError(
            f"schema name exceeds {_MAX_SCHEMA_LENGTH} characters: {candidate!r}"
        )
    if not _SCHEMA_PATTERN.fullmatch(candidate):
        raise InvalidSchemaNameError(
            f"schema name must match [a-z][a-z0-9_]* (got {candidate!r})"
        )
    if candidate in _RESERVED_SCHEMAS or candidate.startswith("pg_"):
        raise InvalidSchemaNameError(f"schema name is reserved: {candidate!r}")
    return candidate


def plugin_schema_name(plugin_name: str) -> str:
    """Derive a schema identifier from ``plugin_name`` (no override in v0)."""
    raw = plugin_name.strip().lower()
    sanitized = re.sub(r"[^a-z0-9_]", "_", raw)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    if not sanitized:
        raise InvalidSchemaNameError(
            f"plugin_name {plugin_name!r} does not yield a schema identifier"
        )
    if sanitized[0].isdigit():
        sanitized = f"p_{sanitized}"
    return validate_schema_name(sanitized)
