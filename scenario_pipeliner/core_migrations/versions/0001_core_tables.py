from __future__ import annotations

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0001_core_tables"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_POSTGRES_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS tasks (
        id BIGSERIAL PRIMARY KEY,
        scenario VARCHAR NOT NULL,
        status VARCHAR NOT NULL DEFAULT 'NEW',
        source VARCHAR NOT NULL DEFAULT 'INNER',
        type_task VARCHAR NOT NULL DEFAULT 'CYCLICAL',
        interval_seconds INTEGER NOT NULL DEFAULT 1,
        max_executions INTEGER NULL DEFAULT 3,
        current_executions INTEGER NOT NULL DEFAULT 0,
        next_run_at TIMESTAMPTZ NULL,
        last_heartbeat_at TIMESTAMPTZ NULL,
        is_block BOOLEAN NOT NULL DEFAULT FALSE,
        parent_id BIGINT NULL REFERENCES tasks(id),
        payload JSONB NULL,
        alias VARCHAR NULL,
        steps_names VARCHAR[] NOT NULL DEFAULT '{}'::VARCHAR[],
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        deleted_at TIMESTAMP NULL
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_tasks_status ON tasks (status);",
    "CREATE INDEX IF NOT EXISTS ix_tasks_parent_id ON tasks (parent_id);",
    "CREATE INDEX IF NOT EXISTS ix_tasks_next_run_at ON tasks (next_run_at);",
    "CREATE INDEX IF NOT EXISTS ix_tasks_last_heartbeat_at ON tasks (last_heartbeat_at);",
    "CREATE INDEX IF NOT EXISTS ix_tasks_alias ON tasks (alias);",
    """
    CREATE TABLE IF NOT EXISTS settings (
        key VARCHAR PRIMARY KEY,
        value VARCHAR NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS results (
        id BIGSERIAL PRIMARY KEY,
        task_id BIGINT NOT NULL REFERENCES tasks(id),
        result JSONB NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_results_task_id ON results (task_id);",
    """
    INSERT INTO settings(key, value)
    VALUES ('pipeline_active_default', '1')
    ON CONFLICT(key) DO NOTHING;
    """,
    """
    INSERT INTO settings(key, value)
    VALUES ('worker_enabled', '1')
    ON CONFLICT(key) DO NOTHING;
    """,
)

_SQLITE_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scenario TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'NEW',
        source TEXT NOT NULL DEFAULT 'INNER',
        type_task TEXT NOT NULL DEFAULT 'CYCLICAL',
        interval_seconds INTEGER NOT NULL DEFAULT 1,
        max_executions INTEGER DEFAULT 3,
        current_executions INTEGER NOT NULL DEFAULT 0,
        next_run_at TEXT NULL,
        last_heartbeat_at TEXT NULL,
        is_block INTEGER NOT NULL DEFAULT 0,
        parent_id INTEGER NULL REFERENCES tasks(id),
        payload TEXT NULL,
        alias TEXT NULL,
        steps_names TEXT NOT NULL DEFAULT '[]',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        deleted_at TEXT NULL
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_tasks_status ON tasks (status);",
    "CREATE INDEX IF NOT EXISTS ix_tasks_parent_id ON tasks (parent_id);",
    "CREATE INDEX IF NOT EXISTS ix_tasks_next_run_at ON tasks (next_run_at);",
    "CREATE INDEX IF NOT EXISTS ix_tasks_last_heartbeat_at ON tasks (last_heartbeat_at);",
    "CREATE INDEX IF NOT EXISTS ix_tasks_alias ON tasks (alias);",
    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        task_id INTEGER NOT NULL REFERENCES tasks(id),
        result TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_results_task_id ON results (task_id);",
    """
    INSERT OR IGNORE INTO settings(key, value)
    VALUES ('pipeline_active_default', '1');
    """,
    """
    INSERT OR IGNORE INTO settings(key, value)
    VALUES ('worker_enabled', '1');
    """,
)


def upgrade() -> None:
    dialect_name = op.get_bind().dialect.name
    statements = (
        _POSTGRES_STATEMENTS if dialect_name == "postgresql" else _SQLITE_STATEMENTS
    )
    for statement in statements:
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS results;")
    op.execute("DROP TABLE IF EXISTS settings;")
    op.execute("DROP TABLE IF EXISTS tasks;")
