import asyncio as aio
import contextlib
import contextvars
import dataclasses
import functools
import inspect
import itertools
import typing as t
import warnings
from abc import ABC, abstractmethod
from contextvars import ContextVar
from dataclasses import dataclass
from logging import Logger, LoggerAdapter

from generics import get_filled_type

from octar.base import (
    ActorId,
    # ActorLike,
    ActorMessage,
    Envelope,
    ExternalReceiver,
    MsgUrgency,
    Request,
)
from octar.core import InternalActorSystem, InternalReceiver, Registrator
from octar.environment import Environment
from octar.stash import LiveStash
from octar.utils import running


@dataclass(frozen=True, kw_only=True)
class ActorState[M]:
    _actor_id: ActorId = None  # type: ignore
    _actor_stash: tuple[M, ...] = ()

    def __init_subclass__(cls, *args, **kwargs) -> None:
        super().__init_subclass__(*args, **kwargs)
        # HACK: A super-hacky workaround to make generic base classes work with pydantic deserialization.
        #       Seems to be related:
        #       * https://github.com/pydantic/pydantic/issues/10648
        #       * https://github.com/pydantic/pydantic/issues/12128
        #       * https://github.com/pydantic/pydantic/issues/8489
        try:
            resolved = get_filled_type(cls, ActorState, M)
        except TypeError:
            return
        cls.__dataclass_fields__["_actor_stash"].type = tuple[resolved, ...]


# class Sender[M: ActorMessage](ActorLike[M]):
#     def __init__(self, actor_id: ActorId, actor_system: InternalActorSystem) -> None:
#         self.__actor_id = actor_id
#         self.__actor_system = actor_system

#     @property
#     def actor_id(self) -> ActorId:
#         assert self.__actor_id
#         return self.__actor_id

#     def tell(self, *msgs: M) -> None:
#         self.__actor_system.tell(
#             self.__actor_system.current_actor_id,
#             self.__actor_id,
#             *msgs,
#         )

#     async def ask(self, msg):
#         return await self.__actor_system.ask(
#             self.__actor_system.current_actor_id,
#             self.__actor_id,
#             msg,
#         )


# MARK: Actor
class Actor[SS: ActorState, M: ActorMessage]:
    def __init__(self, registrator: "Registrator", state: SS) -> None:
        actor_id = state._actor_id
        assert actor_id is not None, "ActorState must have _actor_id set"
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

        self.__stash = LiveStash[M](check_is_processing, mailbox, state._actor_stash, msg_idx_iter)

        self.__actor_system = registrator(actor_id, InternalReceiver(self, self.__receive))
        self.__actor_id = actor_id

    @property
    def actor_id(self) -> ActorId:
        return self.__actor_id

    @property
    def state(self) -> SS:
        return self.__state

    @property
    def _stash(self) -> LiveStash[M]:
        return self.__stash

    @property
    def _logger(self) -> Logger | LoggerAdapter:
        return self.__actor_system.logger

    def _spawn[**P, S: ActorState, R: Actor](
        self,
        create_actor: t.Callable[t.Concatenate[Registrator, S, P], R],
        state: S,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        return self.__actor_system.spawn(create_actor, state, *args, **kwargs)

    def _tell(
        self,
        receiver_id: ActorId,
        *msgs: M,
    ) -> None:
        def updated_msgs_iter():
            for msg in msgs:
                if msg._actor_sender_id != self.actor_id:
                    msg = dataclasses.replace(msg, _actor_sender_id=self.actor_id)
                yield msg

        self.__actor_system.tell(receiver_id, *updated_msgs_iter())

    @t.overload
    def _ask(self, receiver_id: ActorId, msg: M) -> t.Awaitable[M]: ...
    @t.overload
    def _ask[Response: ActorMessage](
        self, receiver_id: ActorId, msg: Request[Response]
    ) -> t.Awaitable[Response]: ...
    def _ask(self, receiver_id, msg):
        if msg._actor_sender_id != self.actor_id:
            msg = dataclasses.replace(msg, _actor_sender_id=self.actor_id)
        return self.__actor_system.ask(receiver_id, msg)

    def _answer[Response: ActorMessage](self, request: Request[Response], msg: Response) -> None:
        receiver_id = request._actor_asker_id or request._actor_sender_id
        if receiver_id is None:
            raise RuntimeError("Cannot answer request without asker_id or sender_id")
        # HACK: Cast to M to satisfy the type checker, as it cannot infer that Response is a subtype of M.
        return self._tell(receiver_id, t.cast(M, msg))

    async def _step(self, state: SS, *msgs: M) -> SS:
        return state

    async def __process(self) -> None:
        orig_state = state = self.__state
        mbox = self.__mailbox
        try:
            while not mbox.empty():
                msgs = []
                while not mbox.empty():
                    *_, msg = mbox.get_nowait()
                    # if isinstance(msg, PoisonPill):
                    #     self.__registration.unregister()
                    msgs.append(msg)
                state = await self._step(state, *msgs)
                assert state._actor_id == self.actor_id, (
                    "State returned from _step must have same _actor_id"
                )
                for msg in msgs:
                    try:
                        self.__actor_system.on_message_processed(self.actor_id, msg)
                    except Exception as e:
                        self._logger.exception(
                            f"Exception in on_message_processed for actor {self.actor_id}: {e}"
                        )
                    # mbox.task_done()
            stash = tuple(self.__stash)

            state = dataclasses.replace(state, _actor_stash=stash)
            if state != orig_state:
                try:
                    self.__actor_system.on_actor_state_changed(self.actor_id, orig_state, state)
                except Exception as e:
                    self._logger.exception(
                        f"Exception in on_actor_state_changed for actor {self.actor_id}: {e}"
                    )
            self.__state = state
        finally:
            self.__processing = None

    def __receive(
        self,
        create_task: t.Callable[[t.Coroutine], aio.Future[t.Any]],
        *msgs: ActorMessage,
    ) -> aio.Future[t.Any]:
        # if not self.__registration:
        #     # TODO: Dead letter queue?
        #     return self.__processing or create_resolved_f(None)
        for msg in msgs:
            self.__mailbox.put_nowait(
                Envelope(
                    urgency=MsgUrgency.NORMAL,
                    priority=next(self.__msg_idx_iter),
                    msg=msg,
                )
            )
        if self.__processing is None:
            self.__processing = create_task(self.__process())
        return self.__processing


class Scheduled(t.NamedTuple):
    receiver_id: ActorId
    receiver: InternalReceiver
    msgs: t.Sequence[t.Any]


# class ExternalSender[M: ActorMessage](Sender[M]):
#     def __init__(
#         self,
#         actor_id: ActorId,
#         actor_system: InternalActorSystem,
#     ) -> None:
#         self.__actor_system = actor_system
#         super().__init__(actor_id, actor_system)

#     # def unregister(self) -> None:
#     #     self.__registration.unregister()


# Taken from https://stackoverflow.com/a/76301341/3344105
# class _classproperty:
#     def __init__(self, func):
#         self.fget = func

#     def __get__(self, instance, owner):
#         return self.fget(owner)


# MARK: ActorSystem
class BaseActorSystem[SS, Msg: ActorMessage](ABC):
    # _current_system: ContextVar["ActorSystem | None"] = ContextVar("current_system", default=None)
    _current_actor_id: ContextVar[ActorId | None] = ContextVar("current_actor", default=None)
    _current_env: ContextVar[Environment | None] = ContextVar("current_env", default=None)

    # @_classproperty
    # def current_system(cls) -> "ActorSystem | None":
    #     return cls._current_system.get()

    def __init__(self, environment: Environment, logger: Logger | LoggerAdapter) -> None:
        self.__scheduled = aio.Queue[Scheduled]()
        self.__logger = logger
        self.__receivers = dict[ActorId, InternalReceiver | ExternalReceiver]()
        self.__pendings_ops = set[aio.Future]()
        self.__running = None
        self.__environment = environment
        self._current_env.set(environment)

    @property
    @abstractmethod
    def state(self) -> SS:
        pass

    def __getitem__(self, actor_id: ActorId) -> Actor:
        receiver = self.__receivers[actor_id]
        if not isinstance(receiver, InternalReceiver):
            raise KeyError(f"Actor with id {actor_id} is not an Actor instance")
        return receiver.actor

    def __iter__(self) -> t.Iterator[Actor]:
        return (r.actor for r in self.__receivers.values() if isinstance(r, InternalReceiver))

    def __len__(self) -> int:
        return len(self.__receivers)

    @property
    def _env(self) -> Environment:
        return self.__environment

    @property
    def current_actor_id(self) -> ActorId | None:
        return self._current_actor_id.get()

    @property
    def is_running(self) -> bool:
        return self.__running is not None

    def _on_message_enqueued(  # noqa: B027
        self,
        actor_id: ActorId,
        msg: ActorMessage,
    ) -> None:
        pass

    def _on_message_processed(  # noqa: B027
        self,
        actor_id: ActorId,
        msg: ActorMessage,
    ) -> None:
        pass

    def _on_actor_state_changed(  # noqa: B027
        self,
        actor_id: ActorId,
        old: ActorState,
        new: ActorState,
    ) -> None:
        pass

    def _on_receiver_registered(  # noqa: B027
        self,
        actor_id: ActorId,
    ) -> None:
        pass

    def _on_receiver_unregistered(  # noqa: B027
        self,
        actor_id: ActorId,
    ) -> None:
        pass

    @functools.cached_property
    def __actor_facing_api(self) -> InternalActorSystem:
        return InternalActorSystem(
            logger=self.__logger,
            ask=self.ask,
            tell=self.tell,
            spawn=self.spawn,
            on_message_processed=self._on_message_processed,
            on_actor_state_changed=self._on_actor_state_changed,
        )

    def register_external(
        self,
        receiver: ExternalReceiver[Msg],
        actor_id: ActorId | None = None,
        once: bool = False,
    ) -> ActorId:
        actor_id = actor_id if actor_id is not None else self.__environment.create_actor_id()
        # if actor_id in self.__receivers:
        #     raise ValueError(f"Actor with id {actor_id} already exists")

        if once:
            orig_receiver = receiver

            def once_receiver(*args, **kwargs):
                try:
                    orig_receiver(*args, **kwargs)
                finally:
                    try:
                        self.unregister(actor_id)
                    except KeyError as e:
                        # TODO: Log
                        pass

            receiver = once_receiver

        self.__receivers[actor_id] = receiver
        return actor_id

    @functools.cached_property
    def __actor_registrator(self) -> Registrator:
        def register_actor(actor_id: ActorId, receiver: InternalReceiver):
            # if actor_id in self.__receivers:
            #     raise ValueError(f"Actor with id {actor_id} already exists")

            self.__receivers[actor_id] = receiver

            self._on_receiver_registered(actor_id)

            return self.__actor_facing_api

        return Registrator(self.__environment.create_actor_id, register_actor)

    # NOTE: Approach with wrapping Request won't work because serialization and deserialization roundtrip will
    #       leave us with the original Request class, not the wrapper (when e.g. a messages is stored in stash).
    # @functools.cached_property
    # def __request_wrapper_cls(self):
    #     system = self

    #     class RequestWrapper(ObjectProxy):
    #         def __init__(self, request: Request, transient_sender_id: ActorId):
    #             super().__init__(request)
    #             self._self_sender_id = transient_sender_id

    #         @property
    #         def _actor_sender_id(self) -> ActorId:
    #             return self._self_sender_id

    #         def answer(self, msg=None, **kwargs) -> None:
    #             if msg is not None and kwargs:
    #                 raise TypeError("Cannot specify both msg and kwargs")
    #             if len(kwargs) > 1:
    #                 raise TypeError("Can only specify one of 'result' or 'exception'")

    #             # Build Response message if needed
    #             if "result" in kwargs:
    #                 msg = SuccessResponse(
    #                     _actor_request_id=self.__wrapped__._actor_message_id,
    #                     result=kwargs["result"],
    #                 )
    #             elif "exception" in kwargs:
    #                 msg = FailureResponse(
    #                     _actor_request_id=self.__wrapped__._actor_message_id,
    #                     exception=kwargs["exception"],
    #                 )

    #             # Send response message
    #             sender_id = system.current_actor_id
    #             system.send(sender_id, self._actor_sender_id, msg)

    #         def __await__(self):
    #             return self.__wrapped__.__await__()

    #     return RequestWrapper

    def spawn[**P, S: ActorState, R: Actor](
        self,
        create_actor: t.Callable[t.Concatenate[Registrator, S, P], R],
        state: S,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        if state._actor_id is None:
            state = dataclasses.replace(state, _actor_id=self.__environment.create_actor_id())
        return create_actor(self.__actor_registrator, state, *args, **kwargs)

    def unregister(self, actor_id: ActorId) -> None:
        if actor_id not in self.__receivers:
            raise KeyError(f"Actor with id {actor_id} does not exist")
        # unregister only external receivers for now
        receiver = self.__receivers[actor_id]
        if isinstance(receiver, InternalReceiver):
            raise RuntimeError("Cannot unregister internal actor receivers directly")
        del self.__receivers[actor_id]

    @t.overload
    def ask[Response: ActorMessage](
        self,
        receiver_id: ActorId,
        msg: Request[Response],
    ) -> t.Awaitable[Response]: ...

    @t.overload
    def ask(
        self,
        receiver_id: ActorId,
        msg: ActorMessage,
    ) -> t.Awaitable[t.Any]: ...

    def ask[Response: ActorMessage](
        self,
        receiver_id,
        msg: Request[Response] | ActorMessage,
    ) -> t.Awaitable[Response] | t.Awaitable[t.Any]:
        # TODO: Handle the situation when receiver_id is not registered (send to dead letters queue?)
        receiver_impl = self.__receivers[receiver_id]

        transient_sender_id = self.__environment.create_actor_id()
        msg = dataclasses.replace(msg, _actor_asker_id=transient_sender_id)

        try:
            self._on_message_enqueued(receiver_id, msg)
        except Exception as e:
            self.__logger.exception(
                f"Exception in on_message_enqueued for actor {receiver_id}: {e}"
            )

        if isinstance(receiver_impl, InternalReceiver):
            self.__scheduled.put_nowait(
                Scheduled(
                    receiver_id=receiver_id,
                    receiver=receiver_impl,
                    msgs=(msg,),
                )
            )

            f = aio.Future()

            def transient_receiver(msg: Response) -> None:
                assert f.done() is False
                try:
                    if isinstance(msg, Exception):
                        f.set_exception(msg)
                    else:
                        f.set_result(msg)
                finally:
                    try:
                        del self.__receivers[transient_sender_id]
                    except KeyError as e:
                        # TODO: Send to dead letters queue?
                        self.__logger.warning(
                            f"Exception in cleaning up transient receiver {transient_sender_id}",
                            exc_info=e,
                        )

            self.__receivers[transient_sender_id] = transient_receiver
        else:

            async def external_receive_wrapper(msg):
                error = None
                try:
                    result = receiver_impl(msg)
                    if inspect.isawaitable(result):
                        result = await result
                    response = result
                except Exception as e:
                    if not isinstance(e, ActorMessage):
                        # TODO: Lift into some kind of ActorMessage that indicates that user hasn't properly wrapped
                        #       exception into ActorMessage at his/her side and deliever it to the caller instead of
                        #       crashing the whole actor system.
                        raise e
                    response = error = e

                try:
                    self._on_message_processed(receiver_id, response)
                except Exception as e:
                    self.__logger.exception(
                        f"Exception in on_message_processed for actor {receiver_id}: {e}"
                    )

                if error is not None:
                    raise error
                return response

            f = aio.ensure_future(external_receive_wrapper(msg))

        # NOTE: Add it here so that .step does not exit prematurely.
        self.__pendings_ops.add(f)

        return f

    def tell(
        self,
        receiver_id: ActorId,
        *msgs: ActorMessage,
    ) -> None:
        for msg in msgs:
            try:
                self._on_message_enqueued(receiver_id, msg)
            except Exception as e:
                self.__logger.exception(
                    f"Exception in on_message_enqueued for actor {receiver_id}: {e}"
                )

        # TODO: Handle the situation when receiver_id is not registered (send to dead letters queue?)
        receiver = self.__receivers[receiver_id]

        if isinstance(receiver, InternalReceiver):
            self.__scheduled.put_nowait(
                Scheduled(
                    receiver_id=receiver_id,
                    receiver=receiver,
                    msgs=msgs,
                )
            )
        else:
            # NOTE: As both tell and ask of ActorSystem can be called either from inside an actor or outside of an actor
            #       system, exceptions should be either handeld at actor level, or outside of the actor system.
            # TODO: Implement error handling at actor level, when tell/ask is called from inside an actor.
            try:
                receiver(*msgs)
            finally:
                for msg in msgs:
                    try:
                        self._on_message_processed(receiver_id, msg)
                    except Exception as e:
                        self.__logger.exception(
                            f"Exception in on_message_processed for actor {receiver_id}: {e}"
                        )

    # NOTE: Approach with uniform send method due to tell and ask being really different beasts.
    # def send(
    #     self,
    #     sender_id: ActorId | None,
    #     receiver_id: ActorId,
    #     *msgs: t.Any,
    # ) -> t.Awaitable[t.Any] | None:
    #     if len(msgs) == 0:
    #         return None
    #     request = None
    #     if isinstance(msgs[0], Request):
    #         if len(msgs) > 1:
    #             raise ValueError("Cannot send Request messages along with other messages")
    #         request = msgs[0]

    #     receiver = self.__receivers[receiver_id]

    #     for msg in msgs:
    #         try:
    #             self._on_message_enqueued(receiver_id, msg)
    #         except Exception as e:
    #             self.__logger.exception(
    #                 f"Exception in on_message_enqueued for actor {receiver_id}: {e}"
    #             )

    #     if isinstance(receiver, InternalReceiver):
    #         self.__scheduled.put_nowait(
    #             Scheduled(
    #                 receiver_id=receiver_id,
    #                 receiver=receiver,
    #                 msgs=msgs,
    #             )
    #         )

    #         if not isinstance(msgs[0], Request):
    #             return None
    #         request = msgs[0]

    #         # NOTE: The whole idea with request wrapper stems from the constraint that we want to be able to serialize
    #         #       and deserialize Request messages freely outside of the actor system, but still make it possible to
    #         #       await them and receive answers from actors through them. For example, when update handler of some
    #         #       temporal workflow receives and deserializes a Request message, it can feed it into a queue to be
    #         #       processed later by an actor system, while update handler code can still await the Request and get
    #         #       the answer back to respond with it.
    #         #       The other motivation is to funnel all the request answering logic through the .send() method to e.g.
    #         #       centralize hook points for monitoring, logging, etc.
    #         transient_sender_id = self.__environment.create_actor_id()

    #         f = aio.Future()

    #         def transient_receive(msg: Response) -> None:
    #             # TODO: Consider the situation when not Response, but simple ActorMessage is received - is this possible?
    #             try:
    #                 request
    #             except Exception as e:
    #                 self.__logger.warning(
    #                     f"Exception in answering request {request} for actor {receiver_id}",
    #                     exc_info=e,
    #                 )
    #             finally:
    #                 try:
    #                     del self.__receivers[transient_sender_id]
    #                 except KeyError as e:
    #                     self.__logger.warning(
    #                         f"Exception in cleaning up transient receiver {transient_sender_id}",
    #                         exc_info=e,
    #                     )

    #         self.__receivers[transient_sender_id] = transient_receive

    #         return result
    #     else:
    #         assert len(msgs) == 1, "External receiver can only handle single message"
    #         external_result = receiver(*msgs)
    #         if inspect.isawaitable(external_result):
    #             f = aio.ensure_future(external_result)
    #             # NOTE: Add it here so that .step does not exit prematurely.
    #             self.__pendings_ops.add(f)
    #         else:
    #             f = create_resolved_f(external_result)

    #         @f.add_done_callback
    #         def _(_) -> None:
    #             self.__pendings_ops.discard(f)
    #             for msg in msgs:
    #                 try:
    #                     self._on_message_processed(receiver_id, msg)
    #                 except Exception as e:
    #                     self.__logger.exception(
    #                         f"Exception in on_message_processed for actor {receiver_id}: {e}"
    #                     )

    #         return f

    @contextlib.asynccontextmanager
    async def _create_runtime_context(self):
        """Internal context manager that handles the actor system runtime lifecycle."""
        env_reset_token = self._current_env.set(self.__environment)
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
                        receiver_id: ActorId, receiver: InternalReceiver, msgs
                    ) -> aio.Future:
                        self._current_actor_id.set(receiver_id)
                        f = receiver(tg.create_task, *msgs)
                        return f

                    while True:
                        receiver_id, receiver, msgs = await self.__scheduled.get()
                        ctx = contextvars.copy_context()
                        f = ctx.run(do_step, receiver_id, receiver, msgs)
                        self.__pendings_ops.add(f)
                        self.__scheduled.task_done()

                # NOTE: Wrap with TaskGroup' tasks to ensure that exceptions are correctly propagated.
                async with running(tg.create_task(dispatch())):
                    yield
                    # Exhaustion happens here when exiting the context
                    await tg.create_task(wait_exhausted())
        finally:
            self._current_env.reset(env_reset_token)

    async def __aenter__(self):
        """Enter the async context manager and start processing."""
        if self.__running is not None:
            raise RuntimeError("Actor system is already running")
        self.__running = self._create_runtime_context()
        await self.__running.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the async context manager and perform exhaustion."""
        if self.__running is None:
            raise RuntimeError("Actor system is not running")
        try:
            return await self.__running.__aexit__(exc_type, exc_val, exc_tb)
        finally:
            self.__running = None

    @warnings.deprecated("Use the async context manager interface instead")
    async def step(self, *msgs: Msg) -> None:
        # NOTE: It's expected that inheritors will override this method and inject messages
        #       into approprate actors by calling super.step(...).
        del msgs
        async with self:
            pass  # Exhaustion happens automatically on context exit
