import asyncio as aio
import dataclasses
import typing as t
from dataclasses import dataclass
from enum import IntEnum

type Mailbox[M] = aio.PriorityQueue[Envelope[M]]


class MsgPriority(IntEnum):
    NORMAL = 2
    HIGH = 1
    URGENT = 0


# TODO: Implement proper actor termination - now they can just be overwritten by new actors with the same ID.
# class PoisonPill:
#     pass


class Envelope[M](t.NamedTuple):
    priority: MsgPriority
    # NOTE: PriorityQueue does not maintain insertion order itself, so we have to enforce it manually.
    #       https://stackoverflow.com/a/47969819/3344105
    msg_idx: int
    msg: M


ActorId = t.NewType("ActorId", str)


class ActorLike[M](t.Protocol):
    def tell(self, *msgs: M) -> None: ...
    def ask(self, msg: M) -> t.Awaitable[t.Any]: ...


@dataclass(frozen=True)
class Request[Response](t.Awaitable[Response]):
    __response: aio.Future[Response] = dataclasses.field(
        default_factory=aio.Future,
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    def set_result(self, result: Response) -> None:
        self.__response.set_result(result)

    def set_exception(self, exception: Exception) -> None:
        self.__response.set_exception(exception)

    def cancel(self) -> None:
        self.__response.cancel()

    def __await__(self) -> t.Generator[t.Any, None, Response]:
        return self.__response.__await__()


@dataclass(frozen=True, kw_only=True)
class ActorMessage:
    # _actor_sender: ActorRef | None = None
    _actor_sender: None = None


class ExternalReceive[M: ActorMessage](t.Protocol):
    def __call__(self, msg: M, /) -> t.Any: ...


class Registration:
    def __init__(self, actor_id: ActorId, unregister: t.Callable[[ActorId], None]) -> None:
        self._registered = True
        self._actor_id = actor_id

        def unregister_wrapper(actor_id: ActorId) -> None:
            if not self._registered:
                return
            unregister(actor_id)
            self._registered = False

        self._unregister = unregister_wrapper

    def __enter__(self) -> t.Self:
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._unregister(self._actor_id)

    def unregister(self) -> None:
        self._unregister(self._actor_id)

    def __bool__(self) -> bool:
        return self._registered
