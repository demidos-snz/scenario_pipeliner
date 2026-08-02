from enum import StrEnum


class Mode(StrEnum):
    DEV = "dev"
    PROD = "prod"


class DbBackend(StrEnum):
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"


class DryRunStatus(StrEnum):
    OK = "ok"
    WARNING = "warning"
    ERROR = "error"


class LoadStatus(StrEnum):
    LOADED = "loaded"
    SKIPPED = "skipped"
    ERROR = "error"


class ChecksumStatus(StrEnum):
    OK = "ok"
    MISMATCH = "mismatch"
    NOT_CHECKED = "not_checked"


class ChecksumAlgorithm(StrEnum):
    SHA256 = "sha256"


class ChecksumManifestScope(StrEnum):
    WHEEL = "wheel"
    UNPACKED = "unpacked"


class ChecksumReportScope(StrEnum):
    WHEEL = "wheel"
    UNPACKED = "unpacked"
    UNKNOWN = "unknown"


class DryRunErrorCode(StrEnum):
    PLUGIN_LOAD_ERROR = "PLUGIN_LOAD_ERROR"
    REGISTRY_ERROR = "REGISTRY_ERROR"
    MANIFEST_NOT_FOUND = "MANIFEST_NOT_FOUND"
    MANIFEST_INVALID = "MANIFEST_INVALID"
    PLUGIN_NAME_CONFLICT = "PLUGIN_NAME_CONFLICT"
    SCENARIO_CONFLICT = "SCENARIO_CONFLICT"
    CORE_COMPAT_MISMATCH = "CORE_COMPAT_MISMATCH"
    PLUGIN_API_MISMATCH = "PLUGIN_API_MISMATCH"
    CHECKSUM_SCOPE_MISMATCH = "CHECKSUM_SCOPE_MISMATCH"
    CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH"
    MIGRATION_PATH_MISSING = "MIGRATION_PATH_MISSING"
    MIGRATION_PATH_NOT_FILE = "MIGRATION_PATH_NOT_FILE"
    MIGRATION_PATH_ABSOLUTE = "MIGRATION_PATH_ABSOLUTE"
    MIGRATION_PATH_TRAVERSAL = "MIGRATION_PATH_TRAVERSAL"
