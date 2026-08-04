from scenario_pipeliner.worker.core.clients import AsyncClient
from scenario_pipeliner.worker.core.step import AsyncStep

from hello_scenario.settings import HelloStepSettings
from hello_scenario.states import HelloTaskState


class HelloPingStep(AsyncStep[HelloTaskState, HelloStepSettings]):
    @property
    def clients(self) -> list[AsyncClient]:
        return []

    async def _run(self, state: HelloTaskState) -> None:
        state.result.ok = True
        state.result.message = {"ping": self.settings.message}
