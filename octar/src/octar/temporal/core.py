import typing as t

if t.TYPE_CHECKING:
    from uuid import UUID

from octar.environment import Environment


class TemporalWorkflowEnv(Environment):
    def __init__(self, workflow_module) -> None:
        super().__init__()
        self._workflow_module = workflow_module

    def _uuid4(self) -> "UUID":
        return self._workflow_module.uuid4()
