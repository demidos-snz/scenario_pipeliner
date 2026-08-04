from dataclasses import dataclass

from scenario_pipeliner.worker.core.states import TaskState


@dataclass
class HelloTaskState(TaskState):
    """Task state for the hello_scenario.ping pipeline."""
