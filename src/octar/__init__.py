import asyncio as aio
import contextvars
import dataclasses
import inspect
import itertools
import logging
import typing as t
from contextvars import ContextVar
from dataclasses import dataclass
from enum import IntEnum
from uuid import uuid4

from generics import get_filled_type
from lazy_object_proxy import Proxy  # type: ignore

# from pydantic import GetCoreSchemaHandler, ValidationInfo
# from pydantic_core import core_schema, PydanticCustomError
from .utils import create_resolved_f, running

_logger = logging.getLogger(__name__)


class MsgPriority(IntEnum):
    NORMAL = 2
    HIGH = 1
    URGENT = 0


# TODO: Implement proper actor termination - now they can just be overwritten by new actors with the same ID.
# class PoisonPill:
#     pass


class _Envelope[M](t.NamedTuple):
    priority: MsgPriority
    # NOTE: PriorityQueue does not maintain insertion order itself, so we have to enforce it manually.
    #       https://stackoverflow.com/a/47969819/3344105
    msg_idx: int
    msg: M


type Mailbox[M] = aio.PriorityQueue[_Envelope[M]]


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


type ActorId = str


class ActorLike[M](t.Protocol):
    def tell(self, *msgs: M) -> None: ...
    def ask(self, msg: M) -> t.Awaitable[t.Any]: ...


class _ActorRefDecl[M](ActorLike[M]):
    def __init__(self, actor_id: ActorId, actor_system: "ActorSystem") -> None: ...
    def tell(self, *msgs) -> t.Any: ...
    def ask(self, msg) -> t.Any: ...


class _ActorRefImpl(Proxy):
    def __init__(self, actor_id: ActorId, actor_system: "ActorSystem") -> None:
        super().__init__(lambda: actor_system[actor_id])

    # @classmethod
    # def __get_pydantic_core_schema__(cls, handler: GetCoreSchemaHandler) -> core_schema.CoreSchema:
    #     # Validation: accept ActorRef as-is, or a mapping with {"actor_id": ...}
    #     def _validate(v: t.Any, info: ValidationInfo) -> "ActorRef":
    #         if isinstance(v, cls):
    #             return v
    #         if isinstance(v, t.Mapping) and "actor_id" in v:
    #             actor_id = v["actor_id"]
    #             ctx = info.context or {}
    #             system = ctx.get("actor_system")
    #             if system is None:
    #                 # Scaffolding: signal that caller must supply context with actor_system.
    #                 raise PydanticCustomError(
    #                     "actorref_missing_context",
    #                     'ActorRef deserialization needs "actor_system" in context',
    #                 )
    #             return cls(actor_id=actor_id, actor_system=system)
    #         raise PydanticCustomError("actorref_type", 'Expected ActorRef or {"actor_id": ...}')

    #     # Serialization: always dump to {"actor_id": ...}
    #     def _serialize(
    #         v: "_ActorRefImpl", _info: core_schema.SerializationInfo
    #     ) -> t.Mapping[str, t.Any]:
    #         return {"actor_id": v.actor_id}

    #     return core_schema.no_info_wrap_validator_function(
    #         _validate,
    #         serialization=core_schema.plain_serializer_function_ser_schema(
    #             _serialize, when_used="json"
    #         ),
    #     )


if t.TYPE_CHECKING:
    ActorRef = _ActorRefDecl
else:
    ActorRef = _ActorRefImpl


@dataclass(frozen=True, kw_only=True)
class ActorState[M]:
    _actor_id: ActorId | None = None
    _actor_stash: tuple[M, ...] = tuple()

    def __init_subclass__(cls, *args, **kwargs) -> None:
        super().__init_subclass__(*args, **kwargs)

        # NOTE: A super-hacky workaround to make generic base classes work with pydantic deserialization.
        # TODO: Get rid of this once this issue is resolved:
        #       https://github.com/copilot/c/cef433fd-22b4-43e4-8bc8-ede6b0c6c646
        try:
            resolved = get_filled_type(cls, ActorState, M)
        except TypeError:
            return
        cls.__dataclass_fields__["_actor_stash"].type = tuple[resolved, ...]


@dataclass(frozen=True, kw_only=True)
class ActorMessage:
    # _actor_sender: ActorRef | None = None
    _actor_sender: None = None


class _Sender[M: ActorMessage](ActorLike[M]):
    def __init__(self, actor_id: ActorId, actor_system: "ActorSystem") -> None:
        self.__actor_id = actor_id
        self.__actor_system = actor_system

    @property
    def actor_id(self) -> ActorId:
        return self.__actor_id

    @property
    def _actor_system(self) -> "ActorSystem":
        return self.__actor_system

    def tell(self, *msgs: M) -> None:
        if len(msgs) == 0:
            return
        if any(isinstance(msg, Request) for msg in msgs):
            raise ValueError("Cannot send Request messages using tell()")
        self.__actor_system.send(
            self.__actor_system.current_actor_id, self.__actor_id, *msgs
        )

    def ask(self, msg):
        result = self.__actor_system.send(
            self.__actor_system.current_actor_id,
            self.__actor_id,
            msg,
        )
        assert isinstance(result, t.Awaitable)
        return result


class LiveStash[M: ActorMessage](t.MutableSequence[M]):
    def __init__(
        self,
        check_is_processing: t.Callable[[], bool],
        mailbox: Mailbox[M],
        buffer: t.Sequence[M],
        msg_idx_iter: t.Iterator[int],
    ) -> None:
        self._check_is_processing = check_is_processing
        self._mailbox = mailbox
        self._buffer = list(buffer)
        self._msg_idx_iter = msg_idx_iter

    def __getitem__(self, index):
        return self._buffer[index]

    def __len__(self):
        return len(self._buffer)

    def __setitem__(self, index, value):
        if not self._check_is_processing():
            raise RuntimeError("Cannot modify LiveStash while actor is not processing")
        self._buffer[index] = value

    def __delitem__(self, index):
        if not self._check_is_processing():
            raise RuntimeError("Cannot modify LiveStash while actor is not processing")
        del self._buffer[index]

    def insert(self, index, value):
        if not self._check_is_processing():
            raise RuntimeError("Cannot modify LiveStash while actor is not processing")
        self._buffer.insert(index, value)

    def rindex(self, value: M) -> int:
        self.reverse()
        i = self.index(value)
        self.reverse()
        return len(self) - i - 1

    def unstash_all(self) -> None:
        if not self._check_is_processing():
            raise RuntimeError("Cannot modify LiveStash while actor is not processing")
        while self._buffer:
            msg = self._buffer.pop(0)
            self._mailbox.put_nowait(
                _Envelope(MsgPriority.HIGH, next(self._msg_idx_iter), msg)
            )

    def __call__(self, msg: M) -> None:
        self.append(msg)


class Actor[S: ActorState, M: ActorMessage](_Sender[M]):
    def __init__(
        self,
        register_actor: "RegisterActor",
        state: S,
        actor_id: ActorId | None = None,
    ) -> None:
        actor_id = state._actor_id or actor_id or uuid4().hex
        if state._actor_id is None:
            state = dataclasses.replace(state, _actor_id=actor_id)

        self.__state = state
        self.__processing = None
        self.__mailbox = mailbox = aio.PriorityQueue()
        self.__msg_idx_iter = msg_idx_iter = itertools.count()

        def check_is_processing() -> bool:
            if self.__processing is None:
                return False
            if self.__processing.done():
                return False
            return True

        self.__stash = LiveStash[M](
            check_is_processing, mailbox, state._actor_stash, msg_idx_iter
        )

        actor_system, registration = register_actor(actor_id, self)
        self.__registration = registration
        super().__init__(actor_id, actor_system)

    if t.TYPE_CHECKING:

        @t.overload
        def ask[Response](self, msg: Request[Response]) -> t.Awaitable[Response]: ...
        @t.overload
        def ask(self, msg: M) -> t.Awaitable[t.Any]: ...
        def ask(self, msg):
            return super().ask(msg)

    @property
    def state(self) -> S:
        return self.__state

    @property
    def _stash(self) -> LiveStash[M]:
        return self.__stash

    async def _step(self, state: S, *msgs: M) -> S:
        return state

    async def __process(self) -> None:
        state = self.__state
        try:
            while not self.__mailbox.empty():
                msgs = []
                while not self.__mailbox.empty():
                    *_, msg = await self.__mailbox.get()
                    # if isinstance(msg, PoisonPill):
                    #     self.__registration.unregister()
                    msgs.append(msg)
                state = await self._step(state, *msgs)
                for _ in msgs:
                    self.__mailbox.task_done()
            if any(isinstance(msg, Request) for msg in self.__stash):
                raise ValueError("Requests cannot be persisted in stash")
            self.__state = dataclasses.replace(state, _actor_stash=tuple(self.__stash))
        finally:
            self.__processing = None

    def __receive(
        self,
        create_task: t.Callable[[t.Coroutine], aio.Future[t.Any]],
        *msgs: M,
    ) -> aio.Future[t.Any]:
        if not self.__registration:
            # TODO: Dead letter queue?
            return self.__processing or create_resolved_f(None)
        for msg in msgs:
            self.__mailbox.put_nowait(
                _Envelope(
                    priority=MsgPriority.NORMAL,
                    msg_idx=next(self.__msg_idx_iter),
                    msg=msg,
                )
            )
        if self.__processing is None:
            self.__processing = create_task(self.__process())
        return self.__processing


_actor_receive_func_name = f"_{Actor.__name__}__receive"
assert hasattr(Actor, _actor_receive_func_name)


class _Scheduled(t.NamedTuple):
    receiver_id: ActorId
    receiver: Actor
    msgs: t.Sequence[t.Any]


class _ExternalReceive[M: ActorMessage](t.Protocol):
    def __call__(self, msg: M, /) -> t.Any: ...


class Registration:
    def __init__(
        self, actor_id: ActorId, unregister: t.Callable[[ActorId], None]
    ) -> None:
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


class RegisterActor(t.Protocol):
    def __call__(
        self, actor_id: ActorId, impl: Actor
    ) -> tuple["ActorSystem", Registration]: ...


class ExternalReceiver[M: ActorMessage](_Sender[M]):
    def __init__(
        self,
        actor_id: str,
        actor_system: "ActorSystem",
        registration: Registration,
    ) -> None:
        super().__init__(actor_id, actor_system)
        self.__registration = registration

    # def unregister(self) -> None:
    #     self.__registration.unregister()


# Taken from https://stackoverflow.com/a/76301341/3344105
# class _classproperty:
#     def __init__(self, func):
#         self.fget = func

#     def __get__(self, instance, owner):
#         return self.fget(owner)


class ActorSystem:
    # _current_system: ContextVar["ActorSystem | None"] = ContextVar("current_system", default=None)
    _current_actor_id: ContextVar[ActorId | None] = ContextVar(
        "current_actor", default=None
    )
    _max_batch_msgs: int = 42

    # @_classproperty
    # def current_system(cls) -> "ActorSystem | None":
    #     return cls._current_system.get()

    def __init__(self, logger=_logger) -> None:
        self.__scheduled = aio.Queue[_Scheduled]()
        self.__logger = logger
        self.__receivers = dict[ActorId, Actor | _ExternalReceive]()
        self.__pendings_ops = set[aio.Future]()
        self.__running = False

    def __getitem__(self, actor_id: ActorId) -> Actor:
        receiver = self.__receivers[actor_id]
        if not isinstance(receiver, Actor):
            raise TypeError(f"Actor with id {actor_id} is not an Actor instance")
        return receiver

    def __len__(self) -> int:
        return len(self.__receivers)

    @property
    def current_actor_id(self) -> ActorId | None:
        return self._current_actor_id.get()

    @property
    def is_running(self) -> bool:
        return self.__running

    def register_external[M: ActorMessage](
        self, receiver: _ExternalReceive[M], actor_id: ActorId | None = None
    ) -> ActorLike[M]:
        # TODO: Detect, that we're running under Temporal and use workflow.uuid instead as uuid from stdlib is non-deterministic
        actor_id = actor_id if actor_id is not None else uuid4().hex
        # if actor_id in self.__receivers:
        #     raise ValueError(f"Actor with id {actor_id} already exists")
        self.__receivers[actor_id] = receiver
        return _Sender(actor_id, self)

    def spawn[**P, R: Actor](
        self,
        create_actor: t.Callable[t.Concatenate[RegisterActor, P], R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        def register_actor(
            actor_id: ActorId, impl: Actor
        ) -> tuple["ActorSystem", Registration]:
            # if actor_id in self.__receivers:
            #     raise ValueError(f"Actor with id {actor_id} already exists")

            self.__receivers[actor_id] = impl

            return self, Registration(actor_id, self._unregister)

        actor = create_actor(register_actor, *args, **kwargs)
        return t.cast(R, ActorRef(actor.actor_id, self))  # type: ignore

    def _unregister(self, actor_id: ActorId) -> None:
        if actor_id not in self.__receivers:
            raise KeyError(f"Actor with id {actor_id} does not exist")
        del self.__receivers[actor_id]

    def send(
        self,
        sender_id: ActorId | None,
        receiver_id: ActorId,
        *msgs: t.Any,
    ) -> t.Awaitable[t.Any] | None:
        receiver = self.__receivers[receiver_id]

        if isinstance(receiver, Actor):
            if len(msgs) == 0:
                return None
            elif len(msgs) > 1 and any(isinstance(msg, Request) for msg in msgs):
                raise ValueError("Cannot send multiple Request messages at once")

            self.__scheduled.put_nowait(
                _Scheduled(
                    receiver_id=receiver_id,
                    receiver=receiver,
                    msgs=msgs,
                )
            )

            if not (len(msgs) == 1 and isinstance(result := msgs[0], Request)):
                return None

            return result
        else:
            # TODO: Get rid of multiple messages per send altogether and use stash inside of actors instead.
            assert len(msgs) == 1, "External receiver can only handle single message"
            external_result = receiver(*msgs)
            if inspect.isawaitable(external_result):
                f = aio.ensure_future(external_result)
                # NOTE: Add it here so that .step does not exit prematurely.
                self.__pendings_ops.add(f)
            else:
                f = create_resolved_f(external_result)
            return f

    async def step(self) -> None:
        if self.__running:
            raise RuntimeError("Actor system is already running")
        self.__running = True
        try:
            async with aio.TaskGroup() as tg:

                async def wait_exhausted():
                    while True:
                        if not self.__scheduled.empty():
                            became_empty = tg.create_task(self.__scheduled.join())
                            self.__pendings_ops.add(became_empty)
                        if len(self.__pendings_ops) == 0 and self.__scheduled.empty():
                            break
                        done, _ = await aio.wait(
                            self.__pendings_ops,
                            return_when=aio.FIRST_COMPLETED,
                        )
                        self.__pendings_ops -= done

                async def dispatch():
                    def do_step(
                        receiver_id: ActorId, receiver: Actor, msgs
                    ) -> aio.Future:
                        self._current_actor_id.set(receiver_id)
                        receive_func = getattr(receiver, _actor_receive_func_name)
                        f = receive_func(tg.create_task, *msgs)
                        return f

                    while True:
                        receiver_id, receiver, msgs = await self.__scheduled.get()
                        ctx = contextvars.copy_context()
                        msgs, remaining = (
                            msgs[: self._max_batch_msgs],
                            msgs[self._max_batch_msgs :],
                        )
                        f = ctx.run(do_step, receiver_id, receiver, msgs)
                        self.__pendings_ops.add(f)
                        self.__scheduled.task_done()
                        if not remaining:
                            continue
                        self.__scheduled.put_nowait(
                            _Scheduled(
                                receiver_id=receiver_id,
                                receiver=receiver,
                                msgs=remaining,
                            )
                        )

                # NOTE: Wrap with TaskGroup' tasks to ensure that exceptions are correctly propagated.
                async with running(tg.create_task(dispatch())):
                    await tg.create_task(wait_exhausted())
        except BaseException as e:
            raise e
        finally:
            self.__running = False
