from __future__ import annotations

from collections.abc import Sequence

from alembic import op
from sqlalchemy import text

from scenario_pipeliner.db.schemes_names import validate_schema_name

revision: str = "0002_outbox_claim_token"
down_revision: str | Sequence[str] | None = "0001_core_tables"
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
    schema = _schema_name()
    op.execute(
        text(
            f'ALTER TABLE "{schema}".broker_outbox '
            "ADD COLUMN IF NOT EXISTS claim_token BIGINT NOT NULL DEFAULT 0"
        )
    )


def downgrade() -> None:
    schema = _schema_name()
    op.execute(
        text(f'ALTER TABLE "{schema}".broker_outbox DROP COLUMN IF EXISTS claim_token')
    )
