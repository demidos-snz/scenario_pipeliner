from scenario_pipeliner.api.enums import DryRunErrorCode


class DryRunPluginError(ValueError):
    def __init__(self, code: DryRunErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


class RegistryError(DryRunPluginError):
    def __init__(
        self,
        message: str,
        *,
        code: DryRunErrorCode = DryRunErrorCode.REGISTRY_ERROR,
    ) -> None:
        super().__init__(code, message)


class ManifestInvalidError(DryRunPluginError):
    def __init__(self, message: str) -> None:
        super().__init__(DryRunErrorCode.MANIFEST_INVALID, message)


class ManifestNotFoundError(DryRunPluginError):
    def __init__(self, message: str) -> None:
        super().__init__(DryRunErrorCode.MANIFEST_NOT_FOUND, message)
