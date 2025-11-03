import typing as t
from abc import ABC, abstractmethod
from uuid import UUID

from octar.base import ActorId, MessageId


class Environment(ABC):
    @abstractmethod
    def _uuid4(self) -> UUID:
        pass

    def create_actor_id(self) -> ActorId:
        return t.cast(ActorId, self._uuid4().hex)

    def create_message_id(self) -> MessageId:
        return t.cast(MessageId, self._uuid4().hex)


class DefaultEnv(Environment):
    def _uuid4(self) -> UUID:
        from uuid import uuid4

        return uuid4()
