from __future__ import annotations

import sys
from collections.abc import Mapping
from contextlib import contextmanager, suppress
from importlib import import_module
from inspect import Signature, signature
from pathlib import Path
from typing import Any

from scenario_pipeliner.api.config import ScenarioPipelinerConfig
from scenario_pipeliner.api.models import PluginManifestV1
from scenario_pipeliner.core.manifest_loader import find_manifest_files, load_manifest
from scenario_pipeliner.core.registry import PluginRegistryV1
from scenario_pipeliner.worker.plugin_registry import (
    MainPipelinePluginRegistry,
    PluginContext,
    ScenarioPluginDefinition,
)


def build_worker_registry_from_manifests(
    config: ScenarioPipelinerConfig,
    *,
    plugin_services: Mapping[str, Any] | None = None,
) -> MainPipelinePluginRegistry:
    """Discover manifest plugins and build worker runtime registry."""
    policy_registry = PluginRegistryV1()
    runtime_registry = MainPipelinePluginRegistry()

    services = plugin_services or {}
    for manifest_path in find_manifest_files(config.plugins_root):
        manifest = load_manifest(manifest_path)
        plugin_dir = manifest_path.parent
        policy_registry.register(
            manifest,
            mode=config.mode,
            core_version=config.core_version,
            plugin_path=plugin_dir,
        )
        plugin_context = PluginContext(
            plugin_name=manifest.plugin_name,
            plugin_dir=plugin_dir,
            plugins_root=config.plugins_root,
            services=services,
        )
        plugin_registry = _build_plugin_registry_for_manifest(
            manifest=manifest,
            plugins_root=config.plugins_root,
            plugin_context=plugin_context,
        )
        _merge_runtime_registry(runtime_registry, plugin_registry)

    return runtime_registry


def _build_plugin_registry_for_manifest(
    *,
    manifest: PluginManifestV1,
    plugins_root: Path,
    plugin_context: PluginContext,
) -> MainPipelinePluginRegistry:
    plugin_registry = MainPipelinePluginRegistry()
    entrypoint_obj = _resolve_entrypoint(manifest.entrypoint, plugins_root=plugins_root)
    _invoke_entrypoint_register(
        entrypoint_obj,
        plugin_registry,
        manifest=manifest,
        plugin_context=plugin_context,
    )
    _validate_registered_scenarios(plugin_registry, manifest=manifest)
    return plugin_registry


def _resolve_entrypoint(entrypoint: str, *, plugins_root: Path) -> Any:
    if entrypoint.count(":") != 1:
        raise ValueError(f"invalid entrypoint format: {entrypoint!r}")
    module_name, object_path = entrypoint.split(":", 1)
    if not module_name or not object_path:
        raise ValueError(f"invalid entrypoint format: {entrypoint!r}")

    with _prepend_sys_path(plugins_root):
        module = import_module(module_name)

    target: Any = module
    for attribute in object_path.split("."):
        target = getattr(target, attribute)
    return target


def _invoke_entrypoint_register(
    entrypoint_obj: Any,
    registry: MainPipelinePluginRegistry,
    *,
    manifest: PluginManifestV1,
    plugin_context: PluginContext,
) -> None:
    candidate = entrypoint_obj() if isinstance(entrypoint_obj, type) else entrypoint_obj
    register_method = getattr(candidate, "register", None)
    if callable(register_method):
        _call_register(
            register_method, registry=registry, plugin_context=plugin_context
        )
        return
    if callable(candidate):
        _call_register(candidate, registry=registry, plugin_context=plugin_context)
        return
    raise TypeError(
        f"entrypoint {manifest.entrypoint!r} for plugin {manifest.plugin_name!r} "
        "must be a callable or expose callable register(registry)"
    )


def _call_register(
    callback: Any,
    *,
    registry: MainPipelinePluginRegistry,
    plugin_context: PluginContext,
) -> None:
    callback_signature = signature(callback)
    if _accepts_two_positionals(callback_signature):
        callback(registry, plugin_context)
        return
    callback(registry)


def _accepts_two_positionals(callback_signature: Signature) -> bool:
    params = list(callback_signature.parameters.values())
    positional_count = 0
    has_var_positional = False
    for param in params:
        if param.kind in (
            param.POSITIONAL_ONLY,
            param.POSITIONAL_OR_KEYWORD,
        ):
            positional_count += 1
        if param.kind == param.VAR_POSITIONAL:
            has_var_positional = True
    return has_var_positional or positional_count >= 2


def _validate_registered_scenarios(
    registry: MainPipelinePluginRegistry,
    *,
    manifest: PluginManifestV1,
) -> None:
    expected = set(manifest.scenarios)
    actual = set(registry.scenarios)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        details = []
        if missing:
            details.append(f"missing={missing}")
        if extra:
            details.append(f"extra={extra}")
        raise ValueError(
            f"entrypoint registration scenarios mismatch for {manifest.plugin_name}: "
            + ", ".join(details)
        )


def _merge_runtime_registry(
    target: MainPipelinePluginRegistry,
    source: MainPipelinePluginRegistry,
) -> None:
    factories = source.pipeline_factories
    state_classes = source.state_classes
    for scenario in source.scenarios:
        target.register(
            ScenarioPluginDefinition(
                scenario=scenario,
                pipeline_factory=factories[scenario],
                state_cls=state_classes[scenario],
            )
        )


@contextmanager
def _prepend_sys_path(path: Path):
    path_str = path.as_posix()
    sys.path.insert(0, path_str)
    try:
        yield
    finally:
        with suppress(ValueError):
            sys.path.remove(path_str)
