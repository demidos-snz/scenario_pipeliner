-- Optional plugin-owned tables for hello_scenario.
-- Core tables live in {{core_schema}}. This plugin uses {{plugin_schema}}.

CREATE TABLE IF NOT EXISTS {{plugin_schema}}.events (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    note TEXT NOT NULL DEFAULT 'hello',
    task_id BIGINT NULL REFERENCES {{core_schema}}.tasks(id)
);
