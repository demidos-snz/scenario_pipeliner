from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy.dialects.postgresql import insert as pg_insert

from scenario_pipeliner.db.schemes_names import validate_schema_name
from scenario_pipeliner.db.tables import build_core_schema

revision: str = "0001_core_tables"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _schema_name() -> str:
    config = op.get_context().config
    if config is None:
        raise RuntimeError("Alembic config is required")
    schema = config.attributes.get("db_schema")
    if not schema:
        raise RuntimeError("db_schema Alembic attribute is required")
    return validate_schema_name(str(schema))


def upgrade() -> None:
    bind = op.get_bind()
    core = build_core_schema(_schema_name())
    # Empty installs only. Later schema changes need a new revision with ALTER,
    # not a tables.py-only edit on an already-applied database.
    for enum_type in core.enums:
        enum_type.create(bind, checkfirst=True)
    for table in core.tables:
        table.create(bind, checkfirst=True)
    seed = (
        pg_insert(core.settings)
        .values([{"key": "worker_enabled", "value": "1"}])
        .on_conflict_do_nothing(index_elements=[core.settings.c.key])
    )
    bind.execute(seed)


def downgrade() -> None:
    bind = op.get_bind()
    core = build_core_schema(_schema_name())
    for table in reversed(core.tables):
        table.drop(bind, checkfirst=True)
    for enum_type in reversed(core.enums):
        enum_type.drop(bind, checkfirst=True)
