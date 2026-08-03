from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from scenario_pipeliner.api.config import ScenarioPipelinerConfig
from scenario_pipeliner.worker.bootstrap import (
    create_runner_db_from_config_with_native_repository,
)
from scenario_pipeliner.worker.core.settings import RunnerDBSettings
from scenario_pipeliner.worker.execution.runner_db import RunnerDB
from scenario_pipeliner.worker.postgres_task_repository import PostgresPoolProtocol


@dataclass(frozen=True, slots=True)
class CyclicalTaskSeed:
    scenario: str
    interval_seconds: int = 1
    max_executions: int | None = 3
    alias: str | None = None
    steps_names: tuple[str, ...] = ()
    payload: dict[str, Any] | None = None


async def ensure_worker_settings(
    *,
    pool: PostgresPoolProtocol,
    scenario: str,
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO settings(key, value)
            VALUES ($1, $2)
            ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
            """,
            f"pipeline_active_{scenario}",
            "1",
        )
        await conn.execute(
            """
            INSERT INTO settings(key, value)
            VALUES ($1, $2)
            ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
            """,
            "worker_enabled",
            "1",
        )


async def create_cyclical_task(
    *,
    pool: PostgresPoolProtocol,
    seed: CyclicalTaskSeed,
) -> int:
    payload_json = json.dumps(seed.payload or {}, ensure_ascii=False)
    alias = seed.alias or f"task-{seed.scenario}"
    steps_names = list(seed.steps_names)
    async with pool.acquire() as conn:
        task_id = await conn.fetchval(
            """
            INSERT INTO tasks (
                scenario, status, source, type_task, interval_seconds,
                max_executions, current_executions, payload, alias, steps_names
            ) VALUES (
                $1, 'NEW', 'INNER', 'CYCLICAL', $2,
                $3, 0, $4::jsonb, $5, $6::varchar[]
            )
            RETURNING id
            """,
            seed.scenario,
            seed.interval_seconds,
            seed.max_executions,
            payload_json,
            alias,
            steps_names,
        )
    if task_id is None:
        raise RuntimeError("failed to insert cyclical task")
    return int(task_id)


def build_postgres_runner(
    *,
    config: ScenarioPipelinerConfig,
    postgres_pool: PostgresPoolProtocol,
    runner_settings: RunnerDBSettings | None = None,
) -> RunnerDB:
    return create_runner_db_from_config_with_native_repository(
        config=config,
        postgres_pool=postgres_pool,
        runner_settings=runner_settings,
    )
