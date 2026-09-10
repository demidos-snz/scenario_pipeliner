from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from pydantic import ConfigDict, Field, field_validator, model_validator

from scenario_pipeliner.api.enums import (
    ChecksumAlgorithm,
    ChecksumManifestScope,
    ChecksumReportScope,
    ChecksumStatus,
    DbBackend,
    DryRunErrorCode,
    DryRunStatus,
    LoadStatus,
    Mode,
)
from scenario_pipeliner.base_settings import FrozenSettings, Settings
from scenario_pipeliner.db.engine import (
    normalize_postgres_url_env,
    try_parse_postgres_url,
)
from scenario_pipeliner.db.schemes_names import DEFAULT_DB_SCHEMA, validate_schema_name
from scenario_pipeliner.version import __version__

PLUGIN_NAME_PATTERN = re.compile(r"^[a-z0-9_.-]+$")
SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?$")
MIGRATION_ORDER_PATTERN = re.compile(r"^\d{14}$")


class ScenarioPipelinerConfig(Settings):
    """Runtime configuration for scenario_pipeliner APIs."""

    mode: Mode = Mode.DEV
    db_backend: DbBackend = DbBackend.POSTGRESQL
    plugins_root: Path = Field(default_factory=lambda: Path("plugins"))
    core_version: str = __version__
    db_schema: str = DEFAULT_DB_SCHEMA

    @field_validator("plugins_root", mode="before")
    @classmethod
    def normalize_plugins_root(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()

    @field_validator("db_schema")
    @classmethod
    def normalize_db_schema(cls, value: str) -> str:
        return validate_schema_name(value)


class CoreMigrationConfig(Settings):
    db_backend: DbBackend = DbBackend.POSTGRESQL
    db_schema: str = DEFAULT_DB_SCHEMA
    postgres_url: str | None = None
    postgres_host: str | None = None
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_db: str | None = None
    postgres_user: str | None = None
    postgres_password: str | None = None

    @field_validator("db_schema")
    @classmethod
    def normalize_core_db_schema(cls, value: str) -> str:
        return validate_schema_name(value)

    @field_validator("postgres_url", mode="before")
    @classmethod
    def normalize_postgres_url(cls, value: str | None) -> str | None:
        return normalize_postgres_url_env(value)

    @model_validator(mode="after")
    def validate_backend_requirements(self) -> CoreMigrationConfig:
        if try_parse_postgres_url(self.postgres_url) is not None:
            return self

        missing: list[str] = []
        if not self.postgres_host:
            missing.append("postgres_host")
        if not self.postgres_db:
            missing.append("postgres_db")
        if not self.postgres_user:
            missing.append("postgres_user")
        if not self.postgres_password:
            missing.append("postgres_password")
        if missing:
            raise ValueError(
                "POSTGRES_URL is missing or invalid; missing postgres parameters: "
                + ", ".join(sorted(missing))
            )
        return self


class ChecksumInfo(FrozenSettings):
    algorithm: ChecksumAlgorithm
    scope: ChecksumManifestScope
    value: str

    @field_validator("value")
    @classmethod
    def validate_checksum_value(cls, value: str) -> str:
        hex_value = value.strip().lower()
        if not re.fullmatch(r"[0-9a-f]{64}", hex_value):
            raise ValueError("checksum.value must be a 64-char sha256 hex string")
        return hex_value


class PluginMigrations(FrozenSettings):
    migration_order: str
    postgresql: str | None = None

    @field_validator("migration_order")
    @classmethod
    def validate_migration_order(cls, value: str) -> str:
        if not MIGRATION_ORDER_PATTERN.fullmatch(value):
            raise ValueError("migration_order must match YYYYMMDDHHMMSS")
        try:
            datetime.strptime(value, "%Y%m%d%H%M%S")
        except ValueError as e:
            raise ValueError(
                "migration_order must be a valid calendar datetime in YYYYMMDDHHMMSS"
            ) from e
        return value

    @model_validator(mode="after")
    def validate_backend_paths(self) -> "PluginMigrations":
        if not self.postgresql:
            raise ValueError("migrations.postgresql is required")
        return self


class PluginManifestV1(FrozenSettings):
    """Manifest schema v1 (source of truth for plugin metadata)."""

    model_config = ConfigDict(extra="forbid")

    plugin_name: str
    plugin_version: str
    plugin_api_version: str
    core_compat: str
    checksum: ChecksumInfo
    entrypoint: str
    scenarios: list[str]
    migrations: PluginMigrations | None = None
    manifest_path: Path | None = None

    @field_validator("plugin_name")
    @classmethod
    def validate_plugin_name(cls, value: str) -> str:
        normalized = value.strip()
        if not PLUGIN_NAME_PATTERN.fullmatch(normalized):
            raise ValueError("plugin_name must match [a-z0-9_.-]+")
        return normalized

    @field_validator("plugin_version")
    @classmethod
    def validate_plugin_version(cls, value: str) -> str:
        version = value.strip()
        if not SEMVER_PATTERN.fullmatch(version):
            raise ValueError("plugin_version must be SemVer-like (x.y.z)")
        return version

    @field_validator("plugin_api_version")
    @classmethod
    def validate_plugin_api_version(cls, value: str) -> str:
        version = value.strip()
        if not version:
            raise ValueError("plugin_api_version must not be empty")
        if not re.fullmatch(r"v\d+", version):
            raise ValueError("plugin_api_version must match v<number>")
        return version

    @field_validator("core_compat")
    @classmethod
    def validate_core_compat(cls, value: str) -> str:
        spec = value.strip()
        try:
            SpecifierSet(spec)
        except InvalidSpecifier as e:
            raise ValueError(f"invalid core_compat specifier: {spec}") from e
        return spec

    @field_validator("entrypoint")
    @classmethod
    def validate_entrypoint(cls, value: str) -> str:
        entrypoint = value.strip()
        if (
            ":" not in entrypoint
            or entrypoint.startswith(":")
            or entrypoint.endswith(":")
        ):
            raise ValueError("entrypoint must match module.path:object")
        return entrypoint

    @field_validator("scenarios")
    @classmethod
    def validate_scenarios_not_empty(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("at least one scenario is required")
        return value

    @model_validator(mode="after")
    def validate_scenario_names(self) -> "PluginManifestV1":
        expected_prefix = f"{self.plugin_name}."
        if len(self.scenarios) != len(set(self.scenarios)):
            raise ValueError("duplicate scenario keys in manifest")
        for scenario in self.scenarios:
            if not scenario.startswith(expected_prefix):
                raise ValueError(
                    f"scenario '{scenario}' must start with '{expected_prefix}'"
                )
        return self


class MigrationPlanItem(FrozenSettings):
    backend: DbBackend
    path: str
    order: str
    plugin_name: str


class ChecksumDryRunInfo(FrozenSettings):
    status: ChecksumStatus
    scope: ChecksumReportScope


class RegistryRegisterResult(FrozenSettings):
    warnings: list[str] = Field(default_factory=list)
    checksum_status: ChecksumStatus = ChecksumStatus.OK


class PluginDryRunResult(FrozenSettings):
    plugin_name: str
    plugin_version: str
    load_status: LoadStatus
    reasons: list[str] = Field(default_factory=list)
    scenarios: list[str] = Field(default_factory=list)
    checksum: ChecksumDryRunInfo
    migration_plan: list[MigrationPlanItem] = Field(default_factory=list)


class DryRunSummary(FrozenSettings):
    plugins_discovered: int = 0
    plugins_loaded: int = 0
    plugins_skipped: int = 0
    migrations_planned: int = 0


class DryRunError(FrozenSettings):
    code: DryRunErrorCode
    message: str


class DryRunReport(FrozenSettings):
    status: DryRunStatus
    mode: Mode
    db_backend: DbBackend
    core_version: str
    summary: DryRunSummary
    plugins: list[PluginDryRunResult]
    errors: list[DryRunError] = Field(default_factory=list)


class CoreMigrationReport(FrozenSettings):
    db_backend: DbBackend
    db_schema: str
    tables: list[str]
    default_settings: dict[str, str]
