"""PostgreSQL schema naming and SQLAlchemy core metadata."""

from scenario_pipeliner.db.engine import (
    create_async_engine_for_schema,
    create_asyncpg_pool_for_schema,
    resolve_postgres_urls,
)
from scenario_pipeliner.db.enums import (
    BrokerOutboxStatus,
    TaskSource,
    TaskStatus,
    TaskType,
)
from scenario_pipeliner.db.exceptions import (
    CoreSchemaMissingError,
    InvalidSchemaNameError,
    PluginSqlTemplateError,
    SchemaMappingMismatchError,
    SchemaPrivilegeError,
)
from scenario_pipeliner.db.schema_utils import (
    DB_SCHEMA_KEY,
    plugin_schema_settings_key,
    require_core_tables,
)
from scenario_pipeliner.db.schemes_names import (
    DEFAULT_DB_SCHEMA,
    plugin_schema_name,
    validate_schema_name,
)
from scenario_pipeliner.db.settings import (
    CoreSchema,
    PostgresPoolSettings,
    PostgresUrls,
)
from scenario_pipeliner.db.tables import build_core_schema

__all__ = [
    "DEFAULT_DB_SCHEMA",
    "DB_SCHEMA_KEY",
    "BrokerOutboxStatus",
    "CoreSchema",
    "CoreSchemaMissingError",
    "InvalidSchemaNameError",
    "PluginSqlTemplateError",
    "PostgresPoolSettings",
    "PostgresUrls",
    "SchemaMappingMismatchError",
    "SchemaPrivilegeError",
    "TaskSource",
    "TaskStatus",
    "TaskType",
    "build_core_schema",
    "create_async_engine_for_schema",
    "create_asyncpg_pool_for_schema",
    "plugin_schema_name",
    "plugin_schema_settings_key",
    "require_core_tables",
    "resolve_postgres_urls",
    "validate_schema_name",
]
