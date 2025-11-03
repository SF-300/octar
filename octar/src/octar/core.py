import asyncio as aio
import typing as t
from dataclasses import dataclass
from logging import Logger, LoggerAdapter

from .base import ActorId, ActorMessage, Request

if t.TYPE_CHECKING:
    from .runtime import Actor, ActorState


class Spawn(t.Protocol):
    def __call__[**P, S: "ActorState", R: "Actor"](
        self,
        create_actor: t.Callable[t.Concatenate["Registrator", S, P], R],
        state: S,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R: ...


class Tell(t.Protocol):
    def __call__(
        self,
        receiver_id: ActorId,
        *msgs: ActorMessage,
    ) -> None: ...


class Ask(t.Protocol):
    @t.overload
    def __call__[Response: ActorMessage](
        self,
        receiver_id: ActorId,
        msg: Request[Response],
    ) -> t.Awaitable[Response]: ...

    @t.overload
    def __call__(
        self,
        receiver_id: ActorId,
        msg: ActorMessage,
    ) -> t.Awaitable[t.Any]: ...


@dataclass(frozen=True, slots=True)
class InternalActorSystem:
    logger: Logger | LoggerAdapter
    ask: Ask
    tell: Tell
    spawn: Spawn
    on_message_processed: t.Callable[[ActorId, ActorMessage], None]
    on_actor_state_changed: t.Callable[[ActorId, "ActorState", "ActorState"], None]


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
