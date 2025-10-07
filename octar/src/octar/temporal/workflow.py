import asyncio as aio
import pickle
import typing as t
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import dataclass
from types import SimpleNamespace

from temporalio import workflow as wf
from temporalio.exceptions import ApplicationError, ChildWorkflowError
from temporalio.workflow import NondeterminismError

from octar import ActorSystem
from octar.core import ActorMessage

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


@dataclass(frozen=True)
class CoordinatorParamsV1[AP]:
    ac_params: AP
    backoff_params: BackoffParams = BackoffParams(
        max_retries=10,
        initial_delay_seconds=1.0,
        max_delay_seconds=300.0,  # 5 minutes
        multiplier=3.0,
    )


@dataclass(frozen=True)
class DriverParamsV1[AP]:
    event: bytes
    ac_params: AP


class BaseCoordinator[AP, M: ActorMessage]:
    def __init__(self, params: CoordinatorParamsV1[AP]) -> None:
        self._events = aio.Queue[M]()
        self._params = params

    @abstractmethod
    async def __call__(self, params: CoordinatorParamsV1[AP]) -> None:
        pass


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

            coordinator_cls = wf.defn(**defn_kwargs)(
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

            driver_host_cls = wf.defn(
                **{
                    "failure_exception_types": [
                        NondeterminismError,
                        _UnpicklingFailed,
                        *defn_kwargs.pop("failure_exception_types", []),
                    ],
                    **defn_kwargs,
                }
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
