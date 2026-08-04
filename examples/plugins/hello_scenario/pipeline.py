from scenario_pipeliner.worker.core.pipeline import AsyncPipeline

from hello_scenario.settings import HelloStepSettings
from hello_scenario.steps import HelloPingStep


def build_pipeline() -> AsyncPipeline:
    return AsyncPipeline(steps=[HelloPingStep(settings=HelloStepSettings())])
