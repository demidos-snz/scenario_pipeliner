from __future__ import annotations

import asyncio
import re
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine, insert, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.pool import NullPool

from scenario_pipeliner.api.enums import DbBackend
from scenario_pipeliner.api.settings import CoreMigrationConfig
from scenario_pipeliner.db.engine import (
    create_async_engine_for_schema,
    create_asyncpg_pool_for_schema,
    create_schema_if_not_exists,
    resolve_postgres_urls,
    search_path_sql,
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
    persist_core_schema_on_engine,
    require_core_tables,
    sync_core_identity_sequences,
)
from scenario_pipeliner.db.schemes_names import (
    plugin_schema_name,
    validate_schema_name,
)
from scenario_pipeliner.db.settings import CoreDb, PostgresUrls
from scenario_pipeliner.db.sql_templates import render_plugin_sql, split_sql_statements
from scenario_pipeliner.db.tables import build_core_schema


def test_validate_schema_name_accepts_default() -> None:
    assert validate_schema_name("sp") == "sp"
    assert validate_schema_name("sp_test_abc") == "sp_test_abc"


@pytest.mark.parametrize(
    "name",
    ["", "Public", "1abc", "sp-test", "pg_toast", "public", "information_schema"],
)
def test_validate_schema_name_rejects_invalid(name: str) -> None:
    with pytest.raises(InvalidSchemaNameError):
        validate_schema_name(name)


def test_plugin_schema_name_sanitizes() -> None:
    assert plugin_schema_name("hello_scenario") == "hello_scenario"
    assert plugin_schema_name("Track.Documents") == "track_documents"
    assert plugin_schema_name("9lives") == "p_9lives"


def test_core_migration_config_rejects_reserved_schema() -> None:
    with pytest.raises((ValidationError, InvalidSchemaNameError)):
        CoreMigrationConfig(
            db_backend=DbBackend.POSTGRESQL,
            db_schema="public",
            postgres_host="localhost",
            postgres_db="db",
            postgres_user="user",
            postgres_password="password",
        )


def test_render_plugin_sql_requires_both_placeholders() -> None:
    with pytest.raises(PluginSqlTemplateError, match="must contain both"):
        render_plugin_sql(
            "CREATE TABLE {{plugin_schema}}.events (id int);",
            core_schema="sp",
            plugin_schema="hello_scenario",
        )
    rendered = render_plugin_sql(
        "CREATE TABLE {{plugin_schema}}.events ("
        "id bigint REFERENCES {{core_schema}}.tasks(id));",
        core_schema="sp",
        plugin_schema="hello_scenario",
    )
    assert "hello_scenario.events" in rendered
    assert "sp.tasks" in rendered
    assert "{{" not in rendered


def test_render_plugin_sql_rejects_unresolved_placeholders() -> None:
    with pytest.raises(PluginSqlTemplateError, match="unresolved"):
        render_plugin_sql(
            "CREATE TABLE {{plugin_schema}}.events ("
            "id int REFERENCES {{core_schema}}.tasks(id), x {{unknown}});",
            core_schema="sp",
            plugin_schema="demo",
        )


def test_plugin_sql_split_keeps_create_statements() -> None:
    sql = (
        "-- Core tables live in {{core_schema}}. This plugin uses {{plugin_schema}}.\n"
        "CREATE TABLE IF NOT EXISTS {{plugin_schema}}.events (\n"
        "    id BIGSERIAL PRIMARY KEY,\n"
        "    task_id BIGINT NULL REFERENCES {{core_schema}}.tasks(id)\n"
        ");\n"
        "CREATE INDEX IF NOT EXISTS ix_events_task\n"
        "    ON {{plugin_schema}}.events (task_id);\n"
    )
    rendered = render_plugin_sql(
        sql,
        core_schema="sp",
        plugin_schema="hello_scenario",
    )
    statements = split_sql_statements(rendered)
    bodies = [
        "\n".join(
            line
            for line in statement.splitlines()
            if not line.lstrip().startswith("--")
        ).strip()
        for statement in statements
    ]
    bodies = [body for body in bodies if body]
    assert bodies
    assert all(body.upper().startswith("CREATE") for body in bodies)
    assert "hello_scenario.events" in rendered
    assert "sp.tasks" in rendered


def test_plugin_sql_split_keeps_dollar_quoted_semicolons() -> None:
    from scenario_pipeliner.db.exceptions import PluginSqlSplitError

    sql = (
        "CREATE FUNCTION {{plugin_schema}}.ok() RETURNS void AS $body$\n"
        "BEGIN\n"
        "    PERFORM 1;\n"
        "END;\n"
        "$body$ LANGUAGE plpgsql;\n"
        "CREATE TABLE {{plugin_schema}}.t (id int);\n"
    )
    statements = split_sql_statements(sql)
    assert len(statements) == 2
    assert "PERFORM 1" in statements[0]
    assert statements[1].upper().startswith("CREATE TABLE")
    with pytest.raises(PluginSqlSplitError, match="BEGIN/COMMIT"):
        split_sql_statements("BEGIN; CREATE TABLE t (id int);")


def test_plugin_sql_split_keeps_quoted_semicolons() -> None:
    from scenario_pipeliner.db.exceptions import PluginSqlSplitError

    sql = (
        "INSERT INTO {{plugin_schema}}.events (marker) "
        "VALUES ('a;b');\n"
        'CREATE TABLE {{plugin_schema}}."odd;name" (id int);\n'
        "-- comment; still one line\n"
        "CREATE INDEX ix ON {{plugin_schema}}.events (id);\n"
    )
    statements = split_sql_statements(sql)
    assert len(statements) == 3
    assert "a;b" in statements[0]
    assert '"odd;name"' in statements[1]
    with pytest.raises(PluginSqlSplitError, match="unterminated string"):
        split_sql_statements("INSERT INTO t VALUES ('oops);")


def test_check_plugin_migration_replay_skips_identical() -> None:
    from scenario_pipeliner.db.exceptions import PluginMigrationConflictError
    from scenario_pipeliner.db.schema_utils import check_plugin_migration_replay

    assert (
        check_plugin_migration_replay(
            plugin_name="demo",
            migration_order="20260803120000",
            sql_sha256="abc",
            existing=None,
        )
        is False
    )
    assert (
        check_plugin_migration_replay(
            plugin_name="demo",
            migration_order="20260803120000",
            sql_sha256="abc",
            existing=("20260803120000", "abc"),
        )
        is True
    )
    with pytest.raises(PluginMigrationConflictError, match="without bumping"):
        check_plugin_migration_replay(
            plugin_name="demo",
            migration_order="20260803120000",
            sql_sha256="new",
            existing=("20260803120000", "old"),
        )
    assert (
        check_plugin_migration_replay(
            plugin_name="demo",
            migration_order="20260803120001",
            sql_sha256="abc",
            existing=("20260803120000", "abc"),
        )
        is True
    )
    assert (
        check_plugin_migration_replay(
            plugin_name="demo",
            migration_order="20260803120001",
            sql_sha256="new",
            existing=("20260803120000", "old"),
        )
        is False
    )
    with pytest.raises(PluginMigrationConflictError, match="older"):
        check_plugin_migration_replay(
            plugin_name="demo",
            migration_order="20260803120000",
            sql_sha256="abc",
            existing=("20260803120001", "abc"),
        )


def test_search_path_sql_uses_validated_identifier() -> None:
    assert search_path_sql("sp") == "SET search_path TO sp"
    with pytest.raises(InvalidSchemaNameError):
        search_path_sql("public")


def test_sqlalchemy_and_asyncpg_share_core_search_path(
    core_db: CoreDb, postgres_urls: PostgresUrls
) -> None:
    async def _check() -> tuple[str, str, int, int]:
        async with core_db.engine.connect() as conn:
            engine_schema = (
                await conn.execute(text("SELECT current_schema()"))
            ).scalar_one()
            engine_count = (
                await conn.execute(text("SELECT count(*) FROM settings"))
            ).scalar_one()
        pool = await create_asyncpg_pool_for_schema(
            postgres_urls.dsn,
            core_db.core.schema_name,
            min_size=1,
            max_size=1,
        )
        try:
            async with pool.acquire() as connection:
                pool_schema = await connection.fetchval("SELECT current_schema()")
                pool_count = await connection.fetchval("SELECT count(*) FROM settings")
        finally:
            await pool.close()
        return (
            str(engine_schema),
            str(pool_schema),
            int(engine_count or 0),
            int(pool_count or 0),
        )

    engine_schema, pool_schema, engine_count, pool_count = asyncio.run(_check())
    assert engine_schema == core_db.core.schema_name
    assert pool_schema == core_db.core.schema_name
    assert engine_count >= 1
    assert pool_count == engine_count


def test_core_migrate_is_single_revision(core_db: CoreDb) -> None:
    async def _version_and_tables() -> tuple[str, set[str]]:
        async with core_db.engine.connect() as conn:
            version = (
                await conn.execute(text("SELECT version_num FROM alembic_version"))
            ).scalar_one()
            tables = {
                str(row[0])
                for row in (
                    await conn.execute(
                        text(
                            "SELECT table_name FROM information_schema.tables "
                            "WHERE table_schema = :schema"
                        ),
                        {"schema": core_db.core.schema_name},
                    )
                )
            }
        return str(version), tables

    version, tables = asyncio.run(_version_and_tables())
    assert version == "0001_core_tables"
    assert {
        "tasks",
        "settings",
        "results",
        "broker_ingress",
        "broker_outbox",
        "plugin_migrations",
        "alembic_version",
    }.issubset(tables)


def test_persist_core_schema_mismatch_raises(core_db: CoreDb) -> None:
    async def _tamper_and_persist() -> None:
        async with core_db.engine.begin() as conn:
            await conn.execute(
                text("UPDATE settings SET value = 'other_schema' WHERE key = :key"),
                {"key": DB_SCHEMA_KEY},
            )
        await persist_core_schema_on_engine(core_db.engine, core_db.core)

    with pytest.raises(SchemaMappingMismatchError):
        asyncio.run(_tamper_and_persist())


def test_create_schema_if_not_exists_is_idempotent(
    postgres_urls: PostgresUrls, isolated_schema: str
) -> None:
    create_schema_if_not_exists(postgres_urls.sync_url, isolated_schema)
    create_schema_if_not_exists(postgres_urls.sync_url, isolated_schema)
    engine = create_engine(postgres_urls.sync_url)
    try:
        with engine.connect() as conn:
            found = conn.execute(
                text("SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = :schema"),
                {"schema": isolated_schema},
            ).scalar_one_or_none()
    finally:
        engine.dispose()
    assert found == 1


def test_create_schema_maps_missing_create_privilege(
    postgres_urls: PostgresUrls,
    postgres_params: dict[str, Any],
    isolated_schema: str,
) -> None:
    database = str(postgres_params["database"])
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        pytest.skip("database name is not a simple identifier")
    role = f"sp_nopriv_{isolated_schema.rsplit('_', 1)[-1]}"
    admin = create_engine(postgres_urls.sync_url, isolation_level="AUTOCOMMIT")
    created_role = False
    try:
        with admin.connect() as conn:
            try:
                conn.execute(text(f"CREATE ROLE {role} LOGIN PASSWORD 'nopriv'"))
            except DBAPIError as exc:
                pytest.skip(f"cannot CREATE ROLE for privilege test: {exc}")
            created_role = True
            conn.execute(text(f"GRANT CONNECT ON DATABASE {database} TO {role}"))
            conn.execute(text(f"REVOKE CREATE ON DATABASE {database} FROM {role}"))
        nopriv = resolve_postgres_urls(
            host=str(postgres_params["host"]),
            port=int(postgres_params["port"]),
            database=database,
            user=role,
            password="nopriv",
        )
        with pytest.raises(SchemaPrivilegeError, match="GRANT CREATE ON DATABASE"):
            create_schema_if_not_exists(nopriv.sync_url, isolated_schema)
    finally:
        if created_role:
            with admin.connect() as conn:
                conn.execute(text(f"REVOKE ALL ON DATABASE {database} FROM {role}"))
                conn.execute(text(f"DROP ROLE IF EXISTS {role}"))
        admin.dispose()


def test_require_core_tables_ok_when_migrated(core_db: CoreDb) -> None:
    asyncio.run(require_core_tables(core_db))


def test_sync_core_identity_sequences_advances_past_copied_ids(
    core_db: CoreDb,
) -> None:
    async def _run() -> None:
        schema = core_db.core.schema_name
        qualified = f"{schema}.tasks"
        async with core_db.engine.begin() as conn:
            max_id = (
                await conn.execute(text("SELECT COALESCE(MAX(id), 0) FROM tasks"))
            ).scalar_one()
            copied_id = int(max_id) + 5
            await conn.execute(
                text("INSERT INTO tasks (id, scenario) VALUES (:id, 'seq_desync')"),
                {"id": copied_id},
            )
            await conn.execute(
                text(
                    "SELECT setval("
                    "pg_get_serial_sequence(:regclass, 'id'), :seq_value, true)"
                ),
                {"regclass": qualified, "seq_value": copied_id - 1},
            )
        with pytest.raises(IntegrityError):
            async with core_db.engine.begin() as conn:
                await conn.execute(
                    insert(core_db.core.tasks).values(scenario="seq_collide")
                )
        await sync_core_identity_sequences(core_db.engine, core_db.core)
        async with core_db.engine.begin() as conn:
            task_id = (
                await conn.execute(
                    insert(core_db.core.tasks)
                    .values(scenario="seq_healed")
                    .returning(core_db.core.tasks.c.id)
                )
            ).scalar_one()
        assert int(task_id) > copied_id

    asyncio.run(_run())


def test_insert_linear_task_retries_after_sequence_desync(core_db: CoreDb) -> None:
    from scenario_pipeliner.worker.broker.settings import TaskDraft
    from scenario_pipeliner.worker.broker.storage import insert_linear_task_with_ingress

    async def _run() -> None:
        schema = core_db.core.schema_name
        qualified = f"{schema}.tasks"
        async with core_db.engine.begin() as conn:
            max_id = (
                await conn.execute(text("SELECT COALESCE(MAX(id), 0) FROM tasks"))
            ).scalar_one()
            copied_id = int(max_id) + 5
            await conn.execute(
                text("INSERT INTO tasks (id, scenario) VALUES (:id, 'seq_desync')"),
                {"id": copied_id},
            )
            await conn.execute(
                text(
                    "SELECT setval("
                    "pg_get_serial_sequence(:regclass, 'id'), :seq_value, true)"
                ),
                {"regclass": qualified, "seq_value": copied_id - 1},
            )
        task_id = await insert_linear_task_with_ingress(
            core_db,
            plugin_name="demo",
            draft=TaskDraft.model_validate(
                {"scenario": "demo.ping", "payload": {"k": "v"}}
            ),
            ingress_queue="q",
            message_id="msg-seq-retry",
            reply_to=None,
            content_type=None,
            headers=None,
        )
        assert task_id > copied_id

    asyncio.run(_run())


def test_require_core_tables_raises_when_settings_missing(
    postgres_urls: PostgresUrls, isolated_schema: str
) -> None:
    schema = isolated_schema
    sync = create_engine(postgres_urls.sync_url, isolation_level="AUTOCOMMIT")
    try:
        with sync.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA {schema}"))
    finally:
        sync.dispose()

    engine = create_async_engine_for_schema(
        postgres_urls.async_url,
        schema,
        poolclass=NullPool,
    )
    db = CoreDb(engine=engine, core=build_core_schema(schema))
    try:
        with pytest.raises(CoreSchemaMissingError, match="migrate-core"):
            asyncio.run(require_core_tables(db))
    finally:
        asyncio.run(engine.dispose())


def test_core_migrate_persists_db_schema_setting(core_db: CoreDb) -> None:
    async def _read() -> str | None:
        result = await core_db.engine.connect()
        try:
            value = (
                await result.execute(
                    select(core_db.core.settings.c.value).where(
                        core_db.core.settings.c.key == DB_SCHEMA_KEY
                    )
                )
            ).scalar_one_or_none()
        finally:
            await result.close()
        return str(value) if value is not None else None

    assert asyncio.run(_read()) == core_db.core.schema_name
    assert (
        build_core_schema(core_db.core.schema_name).schema_name
        == core_db.core.schema_name
    )


def test_core_migrate_creates_plugin_migrations_table(core_db: CoreDb) -> None:
    async def _exists() -> int | None:
        async with core_db.engine.connect() as conn:
            return (
                await conn.execute(
                    text(
                        "SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = :schema "
                        "AND table_name = 'plugin_migrations'"
                    ),
                    {"schema": core_db.core.schema_name},
                )
            ).scalar_one_or_none()

    assert asyncio.run(_exists()) == 1


def test_task_columns_use_postgres_enums(core_db: CoreDb) -> None:
    async def _column_types() -> dict[str, str]:
        async with core_db.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text(
                        "SELECT column_name, udt_name "
                        "FROM information_schema.columns "
                        "WHERE table_schema = :schema AND table_name = 'tasks' "
                        "AND column_name IN ('status', 'source', 'type_task')"
                    ),
                    {"schema": core_db.core.schema_name},
                )
            ).mappings()
            return {str(row["column_name"]): str(row["udt_name"]) for row in rows}

    assert asyncio.run(_column_types()) == {
        "status": "task_status",
        "source": "task_source",
        "type_task": "task_type",
    }


def test_task_type_defaults_to_linear(core_db: CoreDb) -> None:
    async def _insert_and_read() -> str:
        async with core_db.engine.begin() as conn:
            task_id = (
                await conn.execute(
                    insert(core_db.core.tasks)
                    .values(scenario="default_type")
                    .returning(core_db.core.tasks.c.id)
                )
            ).scalar_one()
            value = (
                await conn.execute(
                    select(core_db.core.tasks.c.type_task).where(
                        core_db.core.tasks.c.id == task_id
                    )
                )
            ).scalar_one()
        return str(value)

    assert asyncio.run(_insert_and_read()) == "LINEAR"
