from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from scenario_pipeliner.track_documents.api_client import DiadocAPIClient
from scenario_pipeliner.track_documents.plugin import (
    TRACK_DOCUMENTS_SCENARIO,
    TrackDocumentsPluginOptions,
    configure_track_documents_plugin_with_postgres_pool,
    register,
)
from scenario_pipeliner.track_documents.settings import (
    DiadocAPIClientSettings,
    TrackDocumentStatusSettings,
)
from scenario_pipeliner.worker.plugin_registry import (
    MainPipelinePluginRegistry,
    PluginContext,
)


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(
        self,
        exc_type,
        exc_val,
        exc_tb,
    ) -> None:
        return None


class _FakeConnection:
    async def execute(self, query: str, *args) -> str:
        return "OK"

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction()


class _FakePool:
    @asynccontextmanager
    async def acquire(self):
        yield _FakeConnection()


def _build_diadoc_client() -> DiadocAPIClient:
    return DiadocAPIClient(
        settings=DiadocAPIClientSettings(
            API_BASE_URL="https://diadoc.example.test",
            DIADOC_API_CLIENT_ID="client-id",
            API_TOKEN="token",
        )
    )


def test_track_documents_plugin_registers_pipeline_with_postgres_pool() -> None:
    configure_track_documents_plugin_with_postgres_pool(
        postgres_pool=_FakePool(),
        diadoc_client_provider=_build_diadoc_client,
        status_settings_provider=lambda: TrackDocumentStatusSettings(
            DIADOC_BOX_IDS=["box-1"]
        ),
    )
    registry = MainPipelinePluginRegistry()
    register(registry)

    pipeline_factory = registry.pipeline_factories[TRACK_DOCUMENTS_SCENARIO]
    pipeline = pipeline_factory()

    assert len(pipeline.steps) == 2
    first_step = pipeline.steps[0]
    second_step = pipeline.steps[1]
    assert hasattr(first_step, "db_client")
    assert hasattr(second_step, "db_client")
    assert hasattr(first_step.db_client, "pool")


def test_track_documents_plugin_registers_from_plugin_context() -> None:
    registry = MainPipelinePluginRegistry()
    register(
        registry,
        PluginContext(
            plugin_name="track_documents",
            plugin_dir=Path("/tmp/plugins/track_documents"),
            plugins_root=Path("/tmp/plugins"),
            services={
                "track_documents": TrackDocumentsPluginOptions(
                    postgres_pool=_FakePool(),
                    diadoc_client_provider=_build_diadoc_client,
                    status_settings_provider=lambda: TrackDocumentStatusSettings(
                        DIADOC_BOX_IDS=["box-1"]
                    ),
                )
            },
        ),
    )

    pipeline = registry.pipeline_factories[TRACK_DOCUMENTS_SCENARIO]()
    assert len(pipeline.steps) == 2


def test_track_documents_plugin_context_supports_diadoc_settings() -> None:
    registry = MainPipelinePluginRegistry()
    register(
        registry,
        PluginContext(
            plugin_name="track_documents",
            plugin_dir=Path("/tmp/plugins/track_documents"),
            plugins_root=Path("/tmp/plugins"),
            services={
                "track_documents": TrackDocumentsPluginOptions(
                    postgres_pool=_FakePool(),
                    diadoc_client_settings=DiadocAPIClientSettings(
                        API_BASE_URL="https://diadoc.example.test",
                        DIADOC_API_CLIENT_ID="client-id",
                        API_TOKEN="token",
                    ),
                    status_settings_provider=lambda: TrackDocumentStatusSettings(
                        DIADOC_BOX_IDS=["box-1"]
                    ),
                )
            },
        ),
    )

    pipeline = registry.pipeline_factories[TRACK_DOCUMENTS_SCENARIO]()
    first_step = pipeline.steps[0]
    assert isinstance(first_step.diadoc_client, DiadocAPIClient)
