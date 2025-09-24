import typing as t
from abc import ABC, abstractmethod
from uuid import UUID

from .base import ActorId


class Environment(ABC):
    @abstractmethod
    def _uuid4(self) -> UUID:
        pass

    def create_actor_id(self) -> ActorId:
        return t.cast(ActorId, self._uuid4().hex)


class DefaultEnv(Environment):
    def _uuid4(self) -> UUID:
        from uuid import uuid4

        return uuid4()


class TemporalWorflowEnv(Environment):
    def __init__(self, workflow_module) -> None:
        super().__init__()
        self._workflow_module = workflow_module

    def _uuid4(self) -> "UUID":
        return self._workflow_module.uuid4()


def guess_env() -> Environment:
    try:
        from temporalio import workflow  # type: ignore
    except ImportError:
        pass
    else:
        if workflow.in_workflow():
            return TemporalWorflowEnv(workflow)
    return DefaultEnv()
