from hello_scenario.settings import HelloStepSettings
from hello_scenario.steps import HelloPingStep
from scenario_pipeliner.worker.core.pipeline import AsyncPipeline


def build_pipeline() -> AsyncPipeline:
    return AsyncPipeline(steps=[HelloPingStep(settings=HelloStepSettings())])
