from __future__ import annotations

from pathlib import Path

from packaging.specifiers import SpecifierSet
from packaging.version import Version

from scenario_pipeliner.api.enums import (
    ChecksumManifestScope,
    ChecksumStatus,
    DryRunErrorCode,
    Mode,
)
from scenario_pipeliner.api.models import PluginManifestV1, RegistryRegisterResult
from scenario_pipeliner.core.checksum import sha256_directory, sha256_file
from scenario_pipeliner.core.exceptions import RegistryError


class PluginRegistryV1:
    def __init__(self) -> None:
        self._plugins: dict[str, PluginManifestV1] = {}
        self._scenario_to_plugin: dict[str, str] = {}

    @property
    def plugins(self) -> dict[str, PluginManifestV1]:
        return dict(self._plugins)

    def register(
        self,
        manifest: PluginManifestV1,
        *,
        mode: Mode,
        core_version: str,
        plugin_path: Path | None = None,
    ) -> RegistryRegisterResult:
        self._validate_plugin_api_version(manifest)
        if manifest.plugin_name in self._plugins:
            raise RegistryError(
                f"duplicate plugin_name: {manifest.plugin_name}",
                code=DryRunErrorCode.PLUGIN_NAME_CONFLICT,
            )
        self._validate_compatibility(manifest, core_version=core_version)
        warnings, checksum_status = self._validate_checksum(
            manifest, mode=mode, plugin_path=plugin_path
        )
        self._validate_scenario_collisions(manifest)
        self._plugins[manifest.plugin_name] = manifest
        for scenario in manifest.scenarios:
            self._scenario_to_plugin[scenario] = manifest.plugin_name
        return RegistryRegisterResult(
            warnings=warnings, checksum_status=checksum_status
        )

    def _validate_compatibility(
        self, manifest: PluginManifestV1, *, core_version: str
    ) -> None:
        spec = SpecifierSet(manifest.core_compat)
        if Version(core_version) not in spec:
            raise RegistryError(
                f"plugin {manifest.plugin_name} incompatible with core_version={core_version}",
                code=DryRunErrorCode.CORE_COMPAT_MISMATCH,
            )

    def _validate_plugin_api_version(self, manifest: PluginManifestV1) -> None:
        if manifest.plugin_api_version != "v1":
            raise RegistryError(
                (
                    f"plugin {manifest.plugin_name} has unsupported "
                    f"plugin_api_version={manifest.plugin_api_version}"
                ),
                code=DryRunErrorCode.PLUGIN_API_MISMATCH,
            )

    def _validate_checksum(
        self,
        manifest: PluginManifestV1,
        *,
        mode: Mode,
        plugin_path: Path | None,
    ) -> tuple[list[str], ChecksumStatus]:
        if plugin_path is None:
            return ([], ChecksumStatus.OK)

        if (
            manifest.checksum.scope == ChecksumManifestScope.WHEEL
            and not plugin_path.is_file()
        ):
            message = (
                f"checksum scope mismatch for {manifest.plugin_name}: "
                "scope=wheel requires a wheel file path"
            )
            if mode == Mode.PROD:
                raise RegistryError(
                    message, code=DryRunErrorCode.CHECKSUM_SCOPE_MISMATCH
                )
            return ([message], ChecksumStatus.MISMATCH)

        if (
            manifest.checksum.scope == ChecksumManifestScope.UNPACKED
            and not plugin_path.is_dir()
        ):
            message = (
                f"checksum scope mismatch for {manifest.plugin_name}: "
                "scope=unpacked requires a plugin directory path"
            )
            if mode == Mode.PROD:
                raise RegistryError(
                    message, code=DryRunErrorCode.CHECKSUM_SCOPE_MISMATCH
                )
            return ([message], ChecksumStatus.MISMATCH)

        actual: str
        if (
            manifest.checksum.scope == ChecksumManifestScope.WHEEL
            and plugin_path.is_file()
        ):
            actual = sha256_file(plugin_path)
        elif plugin_path.is_dir():
            actual = sha256_directory(plugin_path)
        elif plugin_path.is_file():
            actual = sha256_file(plugin_path)
        else:
            return (
                [f"checksum skipped: unsupported plugin_path={plugin_path}"],
                ChecksumStatus.NOT_CHECKED,
            )

        expected = manifest.checksum.value
        if actual == expected:
            return ([], ChecksumStatus.OK)

        message = f"checksum mismatch for {manifest.plugin_name}: expected={expected} actual={actual}"
        if mode == Mode.PROD:
            raise RegistryError(message, code=DryRunErrorCode.CHECKSUM_MISMATCH)
        return ([message], ChecksumStatus.MISMATCH)

    def _validate_scenario_collisions(self, manifest: PluginManifestV1) -> None:
        for scenario in manifest.scenarios:
            owner = self._scenario_to_plugin.get(scenario)
            if owner is not None:
                raise RegistryError(
                    f"scenario collision for {scenario}: {owner} vs {manifest.plugin_name}",
                    code=DryRunErrorCode.SCENARIO_CONFLICT,
                )
