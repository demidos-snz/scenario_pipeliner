"""Shared Pydantic bases for configuration and immutable DTOs.

Import ``Settings`` / ``FrozenSettings`` from here (or re-exported from
``scenario_pipeliner.worker.core.settings``). Do not add dataclasses for new
library models.
"""

from pydantic import BaseModel, ConfigDict


class Settings(BaseModel):
    """Pydantic base for configuration.

    ``extra=ignore`` so env / plugin dicts may contain unknown keys.
    ``arbitrary_types_allowed`` so fields may hold Path, SQLAlchemy, protocols.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")


class FrozenSettings(Settings):
    """Immutable ``Settings`` for DTOs: manifests, reports, DB handles, broker drafts."""

    model_config = ConfigDict(frozen=True)
