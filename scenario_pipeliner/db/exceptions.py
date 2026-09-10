from __future__ import annotations


class InvalidSchemaNameError(ValueError):
    """Raised when a PostgreSQL schema name fails library validation."""


class SchemaMappingMismatchError(RuntimeError):
    """Persisted schema mapping does not match the current configuration."""


class PluginSqlTemplateError(ValueError):
    """Plugin SQL is missing required schema placeholders."""


class PluginSqlSplitError(ValueError):
    """Plugin SQL cannot be split into executable statements."""


class PluginMigrationConflictError(RuntimeError):
    """Current plugin SQL does not match the already-applied ledger row."""


class CoreSchemaMissingError(RuntimeError):
    """Library tables are not present in the configured PostgreSQL schema."""


class SchemaPrivilegeError(PermissionError):
    """The PostgreSQL role cannot CREATE SCHEMA in the target database."""
