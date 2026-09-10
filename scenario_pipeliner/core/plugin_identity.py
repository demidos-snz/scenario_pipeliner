"""Unique plugin_name and sanitized plugin schema across a plugin tree."""

from __future__ import annotations

from scenario_pipeliner.api.enums import DryRunErrorCode
from scenario_pipeliner.core.exceptions import DryRunPluginError
from scenario_pipeliner.db.schemes_names import plugin_schema_name


def claim_plugin_identity(
    *,
    plugin_name: str,
    seen_names: set[str],
    schema_owners: dict[str, str],
) -> str:
    """Record ``plugin_name`` / its schema. Raise if either is already taken."""
    if plugin_name in seen_names:
        raise DryRunPluginError(
            DryRunErrorCode.PLUGIN_NAME_CONFLICT,
            f"duplicate plugin_name: {plugin_name}",
        )
    schema = plugin_schema_name(plugin_name)
    owner = schema_owners.get(schema)
    if owner is not None and owner != plugin_name:
        raise DryRunPluginError(
            DryRunErrorCode.PLUGIN_SCHEMA_CONFLICT,
            f"plugin schema collision: {schema!r} for {owner!r} and {plugin_name!r}",
        )
    seen_names.add(plugin_name)
    schema_owners[schema] = plugin_name
    return schema
