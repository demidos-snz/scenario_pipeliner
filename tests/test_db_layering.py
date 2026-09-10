from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from scenario_pipeliner.db.enums import BrokerOutboxStatus, TaskStatus
from scenario_pipeliner.worker.broker.enums import (
    BrokerOutboxStatus as WorkerBrokerOutboxStatus,
)
from scenario_pipeliner.worker.core.enums import TaskStatus as WorkerTaskStatus


def test_db_tables_does_not_import_worker() -> None:
    script = (
        "import sys\n"
        "import scenario_pipeliner.db.tables  # noqa: F401\n"
        "loaded = [\n"
        "    key for key in sys.modules\n"
        "    if key == 'scenario_pipeliner.worker'\n"
        "    or key.startswith('scenario_pipeliner.worker.')\n"
        "]\n"
        "raise SystemExit(0 if not loaded else 1)\n"
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout


def test_worker_enums_reexport_db_enums() -> None:
    assert WorkerTaskStatus is TaskStatus
    assert WorkerBrokerOutboxStatus is BrokerOutboxStatus
