import asyncio as aio
import dataclasses
import typing as t
from abc import ABC, abstractmethod
from dataclasses import dataclass

from temporalio import workflow as wf

from octar import ActorSystem
from octar.base import ActorId
from octar.core import ActorMessage, Request
from octar.environment import Environment
from octar.runtime import ActorState
from octar.temporal.core import TemporalWorkflowEnv


@dataclass(frozen=True)
class Params:
    pass


class _ActorSystemInput(t.NamedTuple):
    receiver_id: ActorId
    msgs: list[ActorMessage]
    resp_fs: dict[int, aio.Future[ActorMessage]]


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

    def _tell(self, receiver_id: ActorId, *msgs: M) -> None:
        self.__input.put_nowait(_ActorSystemInput(receiver_id, list(msgs), {}))

    def _ask[Response: ActorMessage](
        self, receiver_id: ActorId, msg: Request[Response]
    ) -> t.Awaitable[Response]:
        if not isinstance(msg, Request):
            raise TypeError("Message passed to 'ask' must be a Request")
        # NOTE: Those complications with futures being created here rather than using the actor system's
        #       ask machinery directly is because the Actor System is created asynchronously and might not exist when
        #       a request comes.
        f = aio.Future()
        self.__input.put_nowait(_ActorSystemInput(receiver_id, [msg], {0: f}))
        return f

    async def _run_until_continue_as_new_suggested(self) -> None:
        actor_system = await self._create_actor_system(self.__env, self.__params)

        async def step(inp: _ActorSystemInput) -> None:
            async with actor_system:
                while True:
                    receiver_id, msgs, resp_fs = inp
                    for idx, resp_f in resp_fs.items():
                        msg = msgs[idx]
                        asker_id = msg._actor_asker_id
                        if asker_id is None:
                            asker_id = self.__env.create_actor_id()
                            msg = dataclasses.replace(msg, _actor_asker_id=asker_id)
                        actor_system.register_external(resp_f.set_result, asker_id, once=True)
                        msgs[idx] = msg
                    # NOTE: We exploit the fact that for actor system internals it doesn't matter through which
                    # interface we are supplying requests - tell or ask - as long as the message has `Request`
                    # marker/mixin and _asker_id is properly set, the target actor should see it as a proper response
                    # and `_answer` accordingly. By using `tell` here we can spare the actor system the unnecessary
                    # future management and receiver re-wrapping overhead.
                    actor_system.tell(receiver_id, *msgs)
                    try:
                        inp = self.__input.get_nowait()
                    except aio.QueueEmpty:
                        break
            await self._save_state(actor_system.state)

        cancellation = None
        while not wf.info().is_continue_as_new_suggested():
            step_task = aio.create_task(step(await self.__input.get()))
            try:
                await aio.shield(step_task)
            except aio.CancelledError as e:
                cancellation = e
                await step_task
                break

        # Draining the input queue to ensure all messages sent before continue-as-new suggestion are processed.
        while not self.__input.empty():
            await step(await self.__input.get())

        if cancellation is not None:
            raise cancellation

    async def _continue_as_new(self, params: Params) -> None:
        wf.continue_as_new(params)

    async def __call__(self, params: Params) -> None:
        await self._run_until_continue_as_new_suggested()
        await self._continue_as_new(params)
