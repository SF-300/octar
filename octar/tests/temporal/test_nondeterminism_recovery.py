import asyncio as aio
from datetime import timedelta
from uuid import uuid4

import pytest

try:
    from temporalio import activity
    from temporalio import workflow as wf
    from temporalio.client import WorkflowFailureError
    from temporalio.exceptions import ApplicationError
    from temporalio.testing import WorkflowEnvironment
    from temporalio.worker import UnsandboxedWorkflowRunner, Worker
except ImportError:
    pytest.skip("temporalio not installed", allow_module_level=True)

from octar import ActorSystem
from octar.temporal.workflow import (
    BaseCoordinator,
    BaseDriver,
    CoordinatorParamsV1,
    workflow_defn_v1,
)

from ._utils import SimpleMessage, SimpleParams, SimpleState

# Test 2: Non-Determinism Recovery


class Activities:
    def __init__(self) -> None:
        self.ready_for_ejection = aio.Event()

    @activity.defn
    async def mark_ready_for_ejection(self) -> None:
        self.ready_for_ejection.set()

    @activity.defn
    async def process_v1(self, value: int) -> str:
        return f"v1_{value}"

    @activity.defn
    async def process_v2(self, value: int) -> str:
        return f"v2_{value}"


# Create V1 workflow
with workflow_defn_v1(SimpleParams, SimpleMessage) as wf_v1:

    @wf_v1.driver(name="TestDriver")
    class DriverV1(BaseDriver[SimpleParams, SimpleState, SimpleMessage]):
        async def _create_actor_system(self, params):
            class TestActorSystemV1(ActorSystem[SimpleState, SimpleMessage]):
                def __init__(self):
                    super().__init__()
                    self._state = SimpleState()

                @property
                def state(self) -> SimpleState:
                    return self._state

                async def step(self, *msgs: SimpleMessage) -> None:
                    await wf.execute_activity(
                        Activities.process_v1,
                        args=[msgs[0].value],
                        start_to_close_timeout=timedelta(
                            seconds=1,
                        ),
                    )

            return TestActorSystemV1()

        async def _save_state(self, state: SimpleState) -> None:
            await wf.execute_activity(
                Activities.mark_ready_for_ejection,
                args=[],
                start_to_close_timeout=timedelta(seconds=1),
            )
            await aio.sleep(timedelta(weeks=1).total_seconds())

    @wf_v1.coordinator(name="TestCoordinator", failure_exception_types=(Exception,))
    class CoordinatorV1(BaseCoordinator[SimpleParams, SimpleMessage]):
        @wf.signal
        async def enqueue_event(self, event: SimpleMessage) -> None:
            await self._events.put(event)


_expected_success_msg = "Success!!!"


with workflow_defn_v1(SimpleParams, SimpleMessage) as wf_v2:

    @wf_v2.driver(name="TestDriver")
    class DriverV2(BaseDriver[SimpleParams, SimpleState, SimpleMessage]):
        async def _create_actor_system(self, params):
            class TestActorSystemV2(ActorSystem[SimpleState, SimpleMessage]):
                def __init__(self):
                    super().__init__()
                    self._state = SimpleState()

                @property
                def state(self) -> SimpleState:
                    return self._state

                async def step(self, *msgs: SimpleMessage) -> None:
                    await wf.execute_activity(
                        Activities.process_v2,
                        args=[msgs[0].value],
                        start_to_close_timeout=timedelta(
                            seconds=1,
                        ),
                    )

            return TestActorSystemV2()

        async def _save_state(self, state: SimpleState) -> None:
            raise ApplicationError(_expected_success_msg, non_retryable=True)

    @wf_v2.coordinator(name="TestCoordinator", failure_exception_types=(Exception,))
    class CoordinatorV2(BaseCoordinator[SimpleParams, SimpleMessage]):
        @wf.signal
        async def enqueue_event(self, event: SimpleMessage) -> None:
            await self._events.put(event)


@pytest.mark.asyncio
async def test_nondeterminism_recovery(temporal_env: WorkflowEnvironment):
    """
    Test that coordinator recovers from non-determinism errors in driver.
    Simulates code deployment that changes activity behavior.
    """

    workflow_id = f"test-nondeterminism-{uuid4()}"
    task_queue = f"test-queue-{uuid4()}"

    # Phase 1: Start with V1
    print("\n📋 Phase 1: Starting with V1 workflows...")

    activities = Activities()

    worker1 = Worker(
        temporal_env.client,
        task_queue=task_queue,
        workflows=[CoordinatorV1, DriverV1],
        activities=[
            activities.mark_ready_for_ejection,
            activities.process_v1,
            activities.process_v2,
        ],
        workflow_runner=UnsandboxedWorkflowRunner(),
        debug_mode=True,
    )

    async with worker1:
        handle = await temporal_env.client.start_workflow(
            CoordinatorV1.__call__,
            args=[CoordinatorParamsV1[SimpleParams](ac_params=SimpleParams())],
            id=workflow_id,
            task_queue=task_queue,
        )

        # Send initial event and let it START processing (activity completes)
        # but keep driver sleeping
        await handle.signal(CoordinatorV1.enqueue_event, SimpleMessage(value=1, action="start"))
        await activities.ready_for_ejection.wait()

    print("🛑 Worker 1 stopped")

    # Phase 2: Start with V2 (simulates deployment)
    print("\n🔄 Phase 2: Starting with V2 workflows...")

    worker2 = Worker(
        temporal_env.client,
        task_queue=task_queue,
        workflows=[CoordinatorV2, DriverV2],
        activities=[
            activities.mark_ready_for_ejection,
            activities.process_v1,
            activities.process_v2,
        ],
        workflow_runner=UnsandboxedWorkflowRunner(),
        debug_mode=True,
    )

    async with worker2:
        handle = temporal_env.client.get_workflow_handle(workflow_id)

        # This should trigger replay with V2 code, causing non-determinism
        # Coordinator should catch and retry
        await handle.signal(CoordinatorV1.enqueue_event, SimpleMessage(value=2, action="process"))

        with pytest.raises(
            WorkflowFailureError,
            check=lambda e: e.cause.cause.message == _expected_success_msg,  # type: ignore
        ):
            await handle.result()

    print("✅ Non-determinism recovery test completed")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
