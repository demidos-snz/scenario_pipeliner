from __future__ import annotations

import hashlib
import importlib.util
import re
import sys
import types
from collections.abc import Mapping
from importlib import import_module
from inspect import Signature, signature
from pathlib import Path
from typing import Any

from scenario_pipeliner.api.settings import PluginManifestV1, ScenarioPipelinerConfig
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
            plugin_dir=plugin_dir,
            plugin_context=plugin_context,
        )
        _merge_runtime_registry(runtime_registry, plugin_registry)

    return runtime_registry


def _build_plugin_registry_for_manifest(
    *,
    manifest: PluginManifestV1,
    plugin_dir: Path,
    plugin_context: PluginContext,
) -> MainPipelinePluginRegistry:
    plugin_registry = MainPipelinePluginRegistry()
    entrypoint_obj = _resolve_entrypoint(
        manifest.entrypoint,
        plugin_dir=plugin_dir,
        plugin_name=manifest.plugin_name,
    )
    _invoke_entrypoint_register(
        entrypoint_obj,
        plugin_registry,
        manifest=manifest,
        plugin_context=plugin_context,
    )
    _validate_registered_scenarios(plugin_registry, manifest=manifest)
    return plugin_registry


def _safe_ident(value: str) -> str:
    ident = re.sub(r"[^0-9a-zA-Z_]", "_", value)
    if not ident or ident[0].isdigit():
        ident = f"p_{ident}"
    return ident


def _unique_package_name(plugin_name: str, plugin_dir: Path) -> str:
    digest = hashlib.sha256(str(plugin_dir.resolve()).encode("utf-8")).hexdigest()[:12]
    return f"_sp_plugin_{_safe_ident(plugin_name)}_{digest}"


def _install_plugin_package(name: str, plugin_dir: Path) -> types.ModuleType:
    existing = sys.modules.get(name)
    if existing is not None:
        return existing
    init_py = plugin_dir / "__init__.py"
    if init_py.is_file():
        spec = importlib.util.spec_from_file_location(
            name,
            init_py,
            submodule_search_locations=[str(plugin_dir)],
        )
        if spec is None or spec.loader is None:
            raise ImportError(
                f"cannot load plugin package {name} from {plugin_dir.as_posix()}"
            )
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module
    module = types.ModuleType(name)
    module.__file__ = str(plugin_dir / "__init__.py")
    module.__path__ = [str(plugin_dir)]
    sys.modules[name] = module
    return module


def _alias_public_package(
    public_name: str, package: types.ModuleType, plugin_dir: Path
) -> None:
    occupied = sys.modules.get(public_name)
    if occupied is None or occupied is package:
        sys.modules[public_name] = package
        return
    occupied_file = getattr(occupied, "__file__", None)
    if occupied_file is None:
        return
    if Path(occupied_file).resolve().parent == plugin_dir:
        sys.modules[public_name] = package


def _target_module_name(
    *,
    unique: str,
    entrypoint_module: str,
    plugin_dir: Path,
    plugin_name: str,
) -> str:
    parts = entrypoint_module.split(".")
    if parts[0] in {plugin_dir.name, plugin_name}:
        if len(parts) == 1:
            return unique
        return f"{unique}.{'.'.join(parts[1:])}"
    return f"{unique}.{entrypoint_module}"


def _resolve_entrypoint(entrypoint: str, *, plugin_dir: Path, plugin_name: str) -> Any:
    if entrypoint.count(":") != 1:
        raise ValueError(f"invalid entrypoint format: {entrypoint!r}")
    module_name, object_path = entrypoint.split(":", 1)
    if not module_name or not object_path:
        raise ValueError(f"invalid entrypoint format: {entrypoint!r}")

    plugin_dir = plugin_dir.resolve()
    unique = _unique_package_name(plugin_name, plugin_dir)
    package = _install_plugin_package(unique, plugin_dir)
    for public_name in (module_name.split(".", 1)[0], plugin_dir.name, plugin_name):
        if public_name:
            _alias_public_package(public_name, package, plugin_dir)
    target_name = _target_module_name(
        unique=unique,
        entrypoint_module=module_name,
        plugin_dir=plugin_dir,
        plugin_name=plugin_name,
    )
    module = import_module(target_name)
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
    for plugin_name, definition in source.broker_plugins.items():
        target.add_broker_hooks(
            plugin_name=plugin_name,
            hooks=definition.hooks,
            settings_provider=definition.settings_provider,
        )
