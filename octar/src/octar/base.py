import asyncio as aio
import typing as t
from dataclasses import dataclass, field
from enum import IntEnum

type Mailbox[M] = aio.PriorityQueue[Envelope[M]]


class MsgUrgency(IntEnum):
    NORMAL = 2
    HIGH = 1
    URGENT = 0


# TODO: Implement proper actor termination - now they can just be overwritten by new actors with the same ID.
# class PoisonPill:
#     pass


class Envelope[M](t.NamedTuple):
    urgency: MsgUrgency
    # NOTE: PriorityQueue does not maintain insertion order itself, so we have to enforce it manually.
    #       https://stackoverflow.com/a/47969819/3344105
    priority: int
    msg: M


ActorId = t.NewType("ActorId", str)
MessageId = t.NewType("MessageId", str)


def _create_message_id() -> MessageId:
    from octar.runtime import BaseActorSystem

    env = BaseActorSystem._current_env.get()
    if env is None:
        raise RuntimeError("No active environment found to generate message ID")
    message_id = env.create_message_id()
    return message_id


def _obtain_sender_id() -> ActorId | None:
    from octar.runtime import BaseActorSystem

    return BaseActorSystem._current_actor_id.get()


@dataclass(frozen=True, kw_only=True)
class ActorMessage:
    _actor_message_id: MessageId = field(
        default_factory=_create_message_id,
        kw_only=True,
        hash=False,
    )
    _actor_sender_id: ActorId | None = field(
        kw_only=True,
        hash=False,
        default_factory=_obtain_sender_id,
    )
    _actor_asker_id: ActorId | None = field(
        kw_only=True,
        hash=False,
        default=None,
    )


if t.TYPE_CHECKING:

    @dataclass(frozen=True, kw_only=True)
    class Request[Response: ActorMessage](ActorMessage):
        pass
else:

    class Request[Response: ActorMessage]:
        def __new__(cls, *args, **kwargs):
            if cls is Request:
                raise TypeError(
                    "'Request' class is a marker and cannot be instantiated directly - "
                    "mix it into your ActorMessage subclass that will be used as a request"
                )
            return super().__new__(cls)

        def __init_subclass__(cls) -> None:
            if not issubclass(cls, ActorMessage):
                raise TypeError("'Request' can only be mixed into ActorMessage subclasses")


# class ActorLike[M](t.Protocol):
#     def tell(self, *msgs: M) -> None: ...
#     # HACK: Python doesn't have proper intersection types, so we can't write M & Request[Response].
#     def ask[Response: ActorMessage](self, msg: Request[Response]) -> t.Awaitable[Response]: ...


# NOTE: Response is not necessary due to the same reasons as Request below.
# @dataclass(frozen=True, kw_only=True)
# class BaseResponse(ActorMessage):
#     _actor_request_id: MessageId


# @dataclass(frozen=True)
# class SuccessResponse[T](BaseResponse):
#     result: T


# @dataclass(frozen=True)
# class FailureResponse(BaseResponse):
#     exception: Exception


# type Response[T] = SuccessResponse[T] | FailureResponse

# NOTE: We don't need a dedicated Request class - and event can't have it due to expected typereg library integration:
#       * typereg requires us to define some root type for messages with its Registry to be mixed in, so that we can track
#         all message subtypes and get tagged union serialization/deserialization for free. If we introduce a Request class,
#         we will have to ask users to not only inherit from ActorMessage, but also re-declaring their own Request class
#         inheriting both from their own root message type and our Request class, which is rather inconvenient.
#       * Making Request await-able also seem a bit dubious, as it starts to carry some implicit state.
# class Request:
#     @functools.cached_property
#     def __f(self) -> aio.Future[T]:
#         return aio.Future()

#     @t.overload
#     def answer(self, msg: Response[T], /) -> None: ...
#     @t.overload
#     def answer(self, *, result: T) -> None: ...
#     @t.overload
#     def answer(self, *, exception: Exception) -> None: ...
#     def answer(self, msg: Response[T] | None = None, **kwargs: t.Any) -> None:
#         if msg is not None and kwargs:
#             raise ValueError("Cannot provide both positional 'msg' and keyword arguments")
#         unexpected = set(kwargs.keys()) - {"result", "exception"}
#         if len(kwargs) > 1 or unexpected:
#             raise TypeError("Can only provide one of 'result' or 'exception' as keyword argument")

#         if self.__f.done():
#             return

#         if isinstance(msg, SuccessResponse):
#             self.__f.set_result(msg.result)
#             return

#         if isinstance(msg, FailureResponse):
#             self.__f.set_exception(msg.exception)
#             return

#         try:
#             self.__f.set_result(kwargs["result"])
#         except KeyError:
#             pass
#         else:
#             return

#         try:
#             self.__f.set_exception(kwargs["exception"])
#         except KeyError:
#             pass
#         else:
#             return

#         raise ValueError("Must provide either positional 'msg' or keyword 'result'/'exception'")

#     def __await__(self) -> t.Generator[t.Any, None, T]:
#         return self.__f.__await__()


class ExternalReceiver[M: ActorMessage](t.Protocol):
    def __call__(self, msg: M, /) -> t.Any: ...
