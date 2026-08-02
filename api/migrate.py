from scenario_pipeliner.api.config import ScenarioPipelinerConfig
from scenario_pipeliner.api.models import DryRunReport
from scenario_pipeliner.core.dry_run import build_dry_run_report


def apply_migrations(
    config: ScenarioPipelinerConfig,
    *,
    dry_run: bool = False,
) -> DryRunReport:
    """Build migration plan report.

    v0 supports dry-run mode only.
    """

    if not dry_run:
        raise NotImplementedError("v0 supports only dry_run=True")
    return build_dry_run_report(config)
