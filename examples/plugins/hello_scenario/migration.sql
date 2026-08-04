-- Optional plugin-owned tables for hello_scenario.
-- Core tables (tasks/settings/results) come from `scenario_pipeliner db migrate-core`.

CREATE TABLE IF NOT EXISTS hello_scenario_events (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    note TEXT NOT NULL DEFAULT 'hello'
);
