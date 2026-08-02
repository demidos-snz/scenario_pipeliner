from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from packaging.specifiers import InvalidSpecifier, SpecifierSet
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

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

PLUGIN_NAME_PATTERN = re.compile(r"^[a-z0-9_.-]+$")
SEMVER_PATTERN = re.compile(r"^\d+\.\d+\.\d+(?:[-+][a-zA-Z0-9.-]+)?$")
MIGRATION_ORDER_PATTERN = re.compile(r"^\d{14}$")


class ChecksumInfo(BaseModel):
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


class PluginMigrations(BaseModel):
    migration_order: str
    sqlite: str | None = None
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
        if not self.sqlite and not self.postgresql:
            raise ValueError("at least one backend migration path is required")
        return self


class PluginManifestV1(BaseModel):
    """Manifest schema v1 (source of truth for plugin metadata)."""

    model_config = ConfigDict(extra="forbid")

    plugin_name: str
    plugin_version: str
    plugin_api_version: str
    core_compat: str
    checksum: ChecksumInfo
    entrypoint: str
    scenarios: list[str]
    migrations: PluginMigrations
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


class MigrationPlanItem(BaseModel):
    backend: DbBackend
    path: str
    order: str
    plugin_name: str


class ChecksumDryRunInfo(BaseModel):
    status: ChecksumStatus
    scope: ChecksumReportScope


@dataclass(slots=True)
class RegistryRegisterResult:
    warnings: list[str] = field(default_factory=list)
    checksum_status: ChecksumStatus = ChecksumStatus.OK


class PluginDryRunResult(BaseModel):
    plugin_name: str
    plugin_version: str
    load_status: LoadStatus
    reasons: list[str] = Field(default_factory=list)
    scenarios: list[str] = Field(default_factory=list)
    checksum: ChecksumDryRunInfo
    migration_plan: list[MigrationPlanItem] = Field(default_factory=list)


class DryRunSummary(BaseModel):
    plugins_discovered: int = 0
    plugins_loaded: int = 0
    plugins_skipped: int = 0
    migrations_planned: int = 0


class DryRunError(BaseModel):
    code: DryRunErrorCode
    message: str


class DryRunReport(BaseModel):
    status: DryRunStatus
    mode: Mode
    db_backend: DbBackend
    core_version: str
    summary: DryRunSummary
    plugins: list[PluginDryRunResult]
    errors: list[DryRunError] = Field(default_factory=list)


class CoreMigrationReport(BaseModel):
    db_backend: DbBackend
    tables: list[str]
    default_settings: dict[str, str]
