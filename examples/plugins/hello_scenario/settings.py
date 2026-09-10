from scenario_pipeliner.worker.core.settings import StepSettings


class HelloStepSettings(StepSettings):
    message: str = "hello from hello_scenario"
