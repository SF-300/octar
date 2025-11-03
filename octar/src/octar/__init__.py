import logging
from logging import Logger, LoggerAdapter

from octar.core import Registrator
from octar.environment import DefaultEnv, Environment
from octar.runtime import (
    Actor,
    ActorId,
    ActorMessage,
    ActorState,
    BaseActorSystem,
)

__all__ = [
    "Actor",
    "ActorId",
    "ActorState",
    "ActorMessage",
    "ActorSystem",
    "Registrator",
]


class ActorSystem[S, M: ActorMessage](BaseActorSystem[S, M]):
    def __init__(
        self,
        environment: Environment | None = None,
        logger: Logger | LoggerAdapter | None = None,
    ) -> None:
        logger = logging.getLogger(__name__) if logger is None else logger
        environment = _guess_env() if environment is None else environment
        super().__init__(environment, logger)


def _guess_env() -> Environment:
    try:
        from temporalio import workflow  # type: ignore
    except ImportError:
        pass
    else:
        if workflow.in_workflow():
            from octar.temporal.core import TemporalWorkflowEnv

            return TemporalWorkflowEnv(workflow)
    return DefaultEnv()
