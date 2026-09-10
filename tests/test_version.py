from pathlib import Path

from scenario_pipeliner import __version__
from scenario_pipeliner.api.settings import ScenarioPipelinerConfig
from scenario_pipeliner.core.plugin_init import DEFAULT_CORE_COMPAT
from scenario_pipeliner.version import __version__ as package_version


def test_package_exports_version() -> None:
    assert __version__ == package_version


def test_core_version_default_matches_package_version() -> None:
    config = ScenarioPipelinerConfig(plugins_root=Path("."))
    assert config.core_version == __version__


def test_plugin_init_core_compat_includes_package_version() -> None:
    expected = f">={__version__},<1.0"
    assert expected == DEFAULT_CORE_COMPAT
