import asyncio as aio
import dataclasses
import pickle
import typing as t
import warnings
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from types import SimpleNamespace

from temporalio import workflow as wf
from temporalio.common import VersioningBehavior
from temporalio.exceptions import ApplicationError, ChildWorkflowError
from temporalio.workflow import NondeterminismError

from octar import ActorSystem
from octar.base import ActorId
from octar.core import ActorMessage, Request
from octar.environment import Environment
from octar.runtime import ActorState
from octar.temporal.core import TemporalWorkflowEnv

from .utils import BackoffParams, exponential_delays


# COORDINATOR/DRIVER SPLIT FOR NON-DETERMINISM RECOVERY
#
# Problem: Temporal doesn't allow catching non-determinism errors from within the workflow
# experiencing them during replay. Actor system code changes frequently (new activities,
# control flow changes, etc.), causing non-determinism errors that would crash workflows.
#
# Solution: Offload actor system execution to child workflows (drivers) so parent (coordinator)
# can catch and recover from their non-determinism errors.
#
# Coordinator (long-lived):
# - Receives events via user-defined signals
# - Spawns one driver per event
# - Catches ChildWorkflowError when driver fails due to non-determinism/unpickling
# - Retries by spawning fresh driver (which loads migrated state from DB)
# - Survives deployments via continue-as-new
#
# Driver (ephemeral, one per event):
# - Loads current state from DB (state migrations happen outside Temporal)
# - Processes one event through actor system (may involve many activities/timers)
# - Saves updated state to DB
# - Dies immediately after
# - If code changed: replay fails with non-determinism, coordinator catches and retries
#
# Key: We're working around Temporal's limitation by re-implementing child workflow retry logic
# at the coordinator level, with awareness of non-determinism errors. State in DB is source of
# truth and expected to be migrated independently of workflow history.
#
# Activities must be idempotent since failed drivers may be retried.
def _is_nondeterminism_error(e: ChildWorkflowError) -> bool:
    if e.cause is None:
        return False
    if not isinstance(e.cause, ApplicationError):
        return False
    # NOTE: Non-determinism error; https://github.com/temporalio/rules/blob/main/rules/TMPRL1100.md
    if "TMPRL1100" not in e.cause.message:
        return False
    return True


class _UnpicklingFailed(Exception):
    pass


# PICKLING STRATEGY FOR SCHEMA EVOLUTION
#
# Problem: Actor system code evolves frequently, causing non-determinism errors during Temporal replay.
# We want coordinator workflows to survive deployments even when event/actor system schemas change.
#
# Solution: Two-stage serialization with different responsibilities:
#
# 1. Temporal's marshalling (coordinator signal boundary):
#    - Deserializes signal payloads from primitives (JSON) to typed Python objects
#    - On coordinator replay after code change, uses CURRENT type definitions
#    - Handles compatible schema changes (added optional fields, renamed fields, etc.)
#    - Event object in coordinator scope reflects NEW structure after replay
#
# 2. Pickle (coordinator → driver boundary):
#    - Encodes fully-typed event object INCLUDING class metadata (__module__, __qualname__)
#    - Driver unpickles to reconstruct the exact type
#    - Fails if class metadata changed (renamed, moved modules, structural changes)
#
# Recovery mechanism:
# - Driver fails with _UnpicklingFailed when pickle metadata is incompatible
# - Coordinator catches this, still has the freshly-deserialized event in scope
# - Re-pickles event with CURRENT class metadata and retries
# - Eventually succeeds once new code is deployed and stable
#
# Why not just pass typed objects through Temporal params?
# - Would require defining same signal types on both coordinator AND driver workflows
# - Pickle lets us avoid this duplication while maintaining type safety
# - Schema evolution handled at coordinator boundary, pickle is just a pass-through
def _is_unpickling_error(e: ChildWorkflowError) -> bool:
    if e.cause is None:
        return False
    if not isinstance(e.cause, ApplicationError):
        return False
    if e.cause.type != _UnpicklingFailed.__name__:
        return False
    return True


# NEW CONTEXT MANAGER API FOR WORKFLOW DEFINITION
#
# Problem: Temporal doesn't allow classes defined in closures or as inner classes
# to be registered as workflows.
#
# Solution: Context manager that yields decorators for creating two separate
# but interwoven module-level workflow classes:
# 1. Coordinator workflow (created from user's mixin with signal methods)
# 2. Driver workflow (created from user's Driver implementation)
#
# Usage:
#     with workflow_defn_v1() as wf:
#         @wf.coordinator
#         class MyWorkflowSignals:
#             _events: aio.Queue[MyMessage]
#
#             @wf.signal
#             async def enqueue_event(self, event: MyMessage) -> None:
#                 await self._events.put(event)
#
#         @wf.driver
#         class MyDriver(Driver[P, S, MyMessage]):
#             ...
#
#     # Both MyWorkflowSignals and MyDriver are now module-level workflow classes
#     worker = Worker(workflows=[MyWorkflowSignals, MyDriver])


@warnings.deprecated("Too complex; use ActorSystemHostWorkflowV1 directly")
@dataclass(frozen=True)
class CoordinatorParamsV1[AP]:
    ac_params: AP
    backoff_params: BackoffParams = BackoffParams(
        max_retries=10,
        initial_delay_seconds=1.0,
        max_delay_seconds=300.0,  # 5 minutes
        multiplier=3.0,
    )


@warnings.deprecated("Too complex; use ActorSystemHostWorkflowV1 directly")
@dataclass(frozen=True)
class DriverParamsV1[AP]:
    event: bytes
    ac_params: AP


@warnings.deprecated("Too complex; use ActorSystemHostWorkflowV1 directly")
class BaseCoordinator[AP, M: ActorMessage]:
    def __init__(self, params: CoordinatorParamsV1[AP]) -> None:
        self._events = aio.Queue[M]()
        self._params = params

    @abstractmethod
    async def __call__(self, params: CoordinatorParamsV1[AP]) -> None:
        pass


@warnings.deprecated("Too complex; use ActorSystemHostWorkflowV1 directly")
class BaseDriver[AP, S, M: ActorMessage](ABC):
    @abstractmethod
    async def _create_actor_system(self, params: AP) -> ActorSystem[S, M]:
        pass

    @abstractmethod
    async def _save_state(self, state: S) -> None:
        pass

    @abstractmethod
    async def __call__(self, params: DriverParamsV1[AP]) -> None:
        pass


if t.TYPE_CHECKING:
    from typing import overload

    class _WorkflowDecorators[AP, M: ActorMessage]:
        @overload
        def driver[T: BaseDriver](
            self,
            cls: type[T],
            **defn_kwargs,
        ) -> type[T]: ...

        @overload
        def driver[T: BaseDriver](
            self,
            cls: None = None,
            **defn_kwargs,
        ) -> t.Callable[[type[T]], type[T]]: ...

        def driver[T: BaseDriver](
            self,
            cls: type[T] | None = None,
            **defn_kwargs,
        ) -> type[T] | t.Callable[[type[T]], type[T]]: ...

        @overload
        def coordinator[T: BaseCoordinator](
            self,
            cls: type[T],
            **defn_kwargs,
        ) -> type[T]: ...

        @overload
        def coordinator[T: BaseCoordinator](
            self,
            cls: None = None,
            **defn_kwargs,
        ) -> t.Callable[[type[T]], type[T]]: ...

        def coordinator[T: BaseCoordinator](
            self,
            cls: type[T] | None = None,
            **defn_kwargs,
        ) -> type[T] | t.Callable[[type[T]], type[T]]: ...


_versioning_key = "versioning_behavior"


@warnings.deprecated("Too complex; use ActorSystemHostWorkflowV1 directly")
@contextmanager
def workflow_defn_v1[AP, M: ActorMessage](
    ac_params_type: type[AP],
    msg_type: type[M],
) -> "t.Iterator[_WorkflowDecorators[AP, M]]":
    del msg_type

    def _make_metadata_patchers(base_cls: type):
        def patch_metadata(obj, *qualname_parts: str, no_name: bool = False):
            name = qualname_parts[-1] if qualname_parts else obj.__name__
            qualname_parts = (base_cls.__name__, *qualname_parts)
            if not no_name:
                qualname_parts += (name,)
            qualname = ".".join(qualname_parts)

            obj.__module__ = base_cls.__module__
            obj.__qualname__ = qualname
            obj.__name__ = name
            return obj

        def metadata_patched(*qualname_parts: str):
            return lambda obj: patch_metadata(obj, *qualname_parts)

        return patch_metadata, metadata_patched

    coordinator_cls, driver_host_cls = None, None

    def make_coordinator[T](cls: type[T] | None = None, **defn_kwargs):
        def decorator(cls: type[T]) -> type[T]:
            nonlocal coordinator_cls

            if coordinator_cls is not None:
                raise ValueError("coordinator decorator can only be called once")

            patch_metadata, metadata_patched = _make_metadata_patchers(cls)

            # HACK: Default temporal deserialization can't handle deserialization to generics properly,
            #       so we create a concrete subclass with ac_params_type force-filled in.
            @dataclass(frozen=True)
            @metadata_patched()
            class ParamsV1(CoordinatorParamsV1[AP]):
                ac_params: ac_params_type  # type: ignore

            @wf.init
            @metadata_patched()
            def __init__(self, params: ParamsV1):
                self._events = aio.Queue[M]()
                self._params = params

            @wf.run
            @metadata_patched()
            async def __call__(self, params: ParamsV1):
                async def step():
                    assert driver_host_cls is not None, "Driver must be defined before coordinator"

                    event, errors = await self._events.get(), []
                    async for _ in exponential_delays(params.backoff_params):
                        try:
                            await wf.execute_child_workflow(
                                driver_host_cls.__call__,
                                DriverParamsV1[AP](
                                    event=pickle.dumps(event),
                                    ac_params=self._params.ac_params,
                                ),
                            )
                        except ChildWorkflowError as e:
                            errors.append(e)
                            if _is_nondeterminism_error(e):
                                continue
                            if _is_unpickling_error(e):
                                continue
                            raise e
                        break
                    else:
                        raise ExceptionGroup("Failed to process event", errors)

                while not wf.info().is_continue_as_new_suggested():
                    await step()

                while not self._events.empty():
                    await step()

                wf.continue_as_new(params)

            # TODO: Apply to both coordinator and driver?
            def signal_methods():
                for name in dir(cls):
                    attr = getattr(cls, name, None)
                    if not callable(attr):
                        continue
                    if not hasattr(attr, "__temporal_signal_definition"):
                        continue
                    # FIXME: Also re-expose updates and queries?
                    yield (name, wf.signal(attr))  # type: ignore

                # NOTE: Coordinator is a long-lived workflow that must survive code changes,
                #       so we set it to auto-upgrade mode.

            _versioning_value = VersioningBehavior.AUTO_UPGRADE
            if _versioning_key in defn_kwargs and defn_kwargs[_versioning_key] != _versioning_value:
                warnings.warn("'versioning_behavior' is manually overridden - might cause issues")  # noqa: B028
                _versioning_value = defn_kwargs.pop(_versioning_key)

            coordinator_cls = wf.defn(
                **{
                    _versioning_key: _versioning_value,
                    **defn_kwargs,
                }
            )(
                patch_metadata(
                    type(
                        cls.__name__,
                        (cls,),
                        {
                            "__init__": __init__,
                            "__call__": __call__,
                            **dict(signal_methods()),
                        },
                    ),
                    no_name=True,
                )
            )
            return coordinator_cls

        if cls is not None:
            return decorator(cls)
        return decorator

    def make_driver[T](cls: type[T] | None = None, **defn_kwargs):
        def decorator(cls: type[T]) -> type[T]:
            nonlocal driver_host_cls
            if driver_host_cls is not None:
                raise ValueError("driver decorator can only be called once")

            patch_metadata, metadata_patched = _make_metadata_patchers(cls)

            # HACK: Default temporal deserialization can't handle deserialization to generics properly,
            #       so we create a concrete subclass with ac_params_type force-filled in.
            @dataclass(frozen=True)
            @metadata_patched()
            class ParamsV1(DriverParamsV1[AP]):
                ac_params: ac_params_type  # type: ignore

            @wf.run
            @metadata_patched()
            async def __call__(self, params: ParamsV1) -> None:
                try:
                    event = t.cast(M, pickle.loads(params.event))
                except Exception as e:
                    raise _UnpicklingFailed() from e
                actor_system = await self._create_actor_system(params.ac_params)
                await actor_system.step(event)
                await self._save_state(actor_system.state)

            # NOTE: Drivers are controlled by coordinators, so when update is needed, drivers must be able to
            #       finish doing what they were doing using the old code, then die gracefully, for the
            #       coordinator to spawn new drivers with the new code.
            _versioning_value = VersioningBehavior.PINNED
            if _versioning_key in defn_kwargs and defn_kwargs[_versioning_key] != _versioning_value:
                warnings.warn("'versioning_behavior' is manually overridden - might cause issues")  # noqa: B028
                _versioning_value = defn_kwargs.pop(_versioning_key)

            driver_host_cls = wf.defn(
                **{
                    "failure_exception_types": [
                        NondeterminismError,
                        _UnpicklingFailed,
                        *defn_kwargs.pop("failure_exception_types", []),
                    ],
                    _versioning_key: _versioning_value,
                    **defn_kwargs,
                },
            )(
                patch_metadata(
                    type(
                        cls.__name__,
                        (cls,),
                        {
                            "__call__": __call__,
                        },
                    ),
                    no_name=True,
                ),
            )
            return driver_host_cls

        if cls is not None:
            return decorator(cls)
        return decorator

    yield t.cast(
        "_WorkflowDecorators[AP, M]",
        SimpleNamespace(
            coordinator=make_coordinator,
            driver=make_driver,
        ),
    )

    if coordinator_cls is None or driver_host_cls is None:
        raise ValueError("Both coordinator and driver decorators must be called exactly once")


@dataclass(frozen=True)
class Params:
    pass


class _ActorSystemInput(t.NamedTuple):
    receiver: ActorId
    msg: ActorMessage
    response_fut: aio.Future | None


class ActorSystemHostWorkflowV1[S: ActorState, M: ActorMessage](ABC):
    def __init_subclass__(cls) -> None:
        # NOTE: Temporal requires @workflow.init and @workflow.run to be applied to methods defined directly
        # on the workflow class, not inherited. We need to patch metadata and assign to subclass.

        def patch_metadata(obj, name: str):
            obj.__module__ = cls.__module__
            obj.__qualname__ = f"{cls.__qualname__}.{name}"
            obj.__name__ = name
            return obj

        if "__init__" not in cls.__dict__:

            def init_proxy(self, params: Params) -> None:
                ActorSystemHostWorkflowV1.__init__(self, params)

            cls.__init__ = wf.init(patch_metadata(init_proxy, "__init__"))

        if "__call__" not in cls.__dict__:

            async def call_proxy(self, params: Params) -> None:
                await ActorSystemHostWorkflowV1.__call__(self, params)

            cls.__call__ = wf.run(patch_metadata(call_proxy, "__call__"))

    def __init__(self, params: Params) -> None:
        self.__params = params
        self.__env = TemporalWorkflowEnv(wf)
        self.__input = aio.Queue[_ActorSystemInput]()

    @abstractmethod
    async def _create_actor_system(self, env: Environment, params: Params) -> ActorSystem[S, M]:
        pass

    @abstractmethod
    async def _save_state(self, state: S) -> None:
        pass

    def _tell(self, receiver_id: ActorId, msg: M) -> None:
        self.__input.put_nowait(_ActorSystemInput(receiver_id, msg, None))

    def _ask[Response: ActorMessage](
        self, receiver_id: ActorId, msg: Request[Response]
    ) -> t.Awaitable[Response]:
        if not isinstance(msg, Request):
            raise TypeError("Message passed to 'ask' must be a Request")
        # NOTE: Those complications with futures being created here rather than using the actor system's
        #       ask machinery directly is because the Actor System is created asynchronously and might not exist when
        #       a request comes.
        f = aio.Future()
        self.__input.put_nowait(_ActorSystemInput(receiver_id, msg, f))
        return f

    async def _run_until_continue_as_new_suggested(self) -> None:
        actor_system = await self._create_actor_system(self.__env, self.__params)

        async def step(
            receiver_id: ActorId, msg: ActorMessage, response_fut: aio.Future | None
        ) -> None:
            async with actor_system:
                if response_fut is not None:
                    asker_id = msg._actor_asker_id
                    if asker_id is None:
                        asker_id = self.__env.create_actor_id()
                        msg = dataclasses.replace(msg, _actor_asker_id=asker_id)
                    actor_system.register_external(response_fut.set_result, asker_id, once=True)
                # NOTE: We exploit the fact that for actor system internals it doesn't matter through which interface we
                # are supplying requests - tell or ask - as long as the message has `Request` marker/mixin and
                # _asker_id is properly set, the target actor should see it as a proper response and `_answer`
                # accordingly. By using `tell` here we can spare the actor system the unnecessary future management and
                # receiver re-wrapping overhead.
                actor_system.tell(receiver_id, msg)
            await self._save_state(actor_system.state)

        cancellation = None
        while not wf.info().is_continue_as_new_suggested():
            step_task = aio.create_task(step(*(await self.__input.get())))
            try:
                await aio.shield(step_task)
            except aio.CancelledError as e:
                cancellation = e
                await step_task
                break

        # Draining the input queue to ensure all messages sent before continue-as-new suggestion are processed.
        while not self.__input.empty():
            await step(*(await self.__input.get()))

        if cancellation is not None:
            raise cancellation

    async def _continue_as_new(self, params: Params) -> None:
        wf.continue_as_new(params)

    async def __call__(self, params: Params) -> None:
        await self._run_until_continue_as_new_suggested()
        await self._continue_as_new(params)
