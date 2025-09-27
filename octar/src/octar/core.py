import asyncio as aio
import typing as t
from dataclasses import dataclass
from logging import Logger

from .base import ActorId, ActorMessage

if t.TYPE_CHECKING:
    from .runtime import Actor, ActorState


class Spawn(t.Protocol):
    def __call__[**P, R: "Actor"](
        self,
        create_actor: t.Callable[t.Concatenate["Registrator", P], R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...


class Send(t.Protocol):
    def __call__(
        self,
        sender_id: ActorId | None,
        receiver_id: ActorId,
        *msgs: ActorMessage,
    ) -> t.Awaitable[t.Any] | None: ...


@dataclass(frozen=True, slots=True)
class InternalActorSystem:
    logger: Logger
    send: Send
    spawn: Spawn
    on_message_processed: t.Callable[[ActorId, ActorMessage], None]
    on_actor_state_changed: t.Callable[[ActorId, "ActorState", "ActorState"], None]
    _get_current_actor_id: t.Callable[[], ActorId | None]

    @property
    def current_actor_id(self) -> ActorId | None:
        return self._get_current_actor_id()


class Receive(t.Protocol):
    def __call__(
        self,
        create_task: t.Callable[[t.Coroutine], aio.Future[t.Any]],
        *msgs: ActorMessage,
    ) -> aio.Future[t.Any]: ...


@dataclass(frozen=True, slots=True)
class InternalReceiver:
    actor: "Actor"
    _receive: Receive

    if t.TYPE_CHECKING:
        __call__: Receive = t.cast(Receive, ...)
    else:

        def __call__(self, *args, **kwds):
            return self._receive(*args, **kwds)


@dataclass(frozen=True, slots=True)
class Registrator:
    """Utility class to nudge users at type-level towards creating actors via ActorSystem.spawn()"""

    create_actor_id: t.Callable[[], ActorId]
    _register_impl: t.Callable[[ActorId, InternalReceiver], InternalActorSystem]

    def __call__(self, actor_id: ActorId, receiver: InternalReceiver) -> InternalActorSystem:
        return self._register_impl(actor_id, receiver)
