from pathlib import Path

from pydantic import BaseModel, Field, field_validator, model_validator

from scenario_pipeliner.api.enums import DbBackend, Mode


class ScenarioPipelinerConfig(BaseModel):
    """Runtime configuration for scenario_pipeliner APIs."""

    mode: Mode = Mode.DEV
    db_backend: DbBackend
    plugins_root: Path = Field(default_factory=lambda: Path("plugins"))
    core_version: str = "0.1.0"

    @field_validator("plugins_root", mode="before")
    @classmethod
    def normalize_plugins_root(cls, value: str | Path) -> Path:
        return Path(value).expanduser().resolve()


class CoreMigrationConfig(BaseModel):
    db_backend: DbBackend
    sqlite_path: Path | None = None
    postgres_host: str | None = None
    postgres_port: int = Field(default=5432, ge=1, le=65535)
    postgres_db: str | None = None
    postgres_user: str | None = None
    postgres_password: str | None = None

    @field_validator("sqlite_path", mode="before")
    @classmethod
    def normalize_sqlite_path(cls, value: str | Path | None) -> Path | None:
        if value is None:
            return None
        return Path(value).expanduser().resolve()

    @model_validator(mode="after")
    def validate_backend_requirements(self) -> "CoreMigrationConfig":
        if self.db_backend == DbBackend.SQLITE:
            if self.sqlite_path is None:
                raise ValueError("sqlite_path is required when db_backend=sqlite")
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
                "missing postgres parameters: " + ", ".join(sorted(missing))
            )
        return self
