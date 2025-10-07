import asyncio as aio
from datetime import timedelta
from uuid import uuid4

import pytest

try:
    from temporalio import activity
    from temporalio import workflow as wf
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


# Activities for test verification
class _Activities:
    def __init__(self):
        self.saved_states: list[SimpleState] = []

    @activity.defn
    async def save_state(self, counter: int, messages: list[str]) -> None:
        self.saved_states.append(SimpleState(counter=counter, messages=tuple(messages)))


# Test workflow using new context manager pattern
with workflow_defn_v1(SimpleParams, SimpleMessage) as wf_defn:

    @wf_defn.driver
    class _Driver(BaseDriver[SimpleParams, SimpleState, SimpleMessage]):
        async def _create_actor_system(self, params):
            # In real usage, this would load from DB
            # For testing, we'll create a simple actor system
            state = SimpleState()

            # Create a minimal actor system (we'll need to implement a test version)
            # For now, let's use a mock that just updates state
            class TestActorSystem(ActorSystem[SimpleState, SimpleMessage]):
                def __init__(self, initial_state: SimpleState):
                    super().__init__()
                    self._state = initial_state

                @property
                def state(self) -> SimpleState:
                    return self._state

                async def step(self, *msgs: SimpleMessage) -> None:
                    event = msgs[0]
                    wf.logger.info(f"Processing event: {event.action} with value {event.value}")
                    self._state = SimpleState(
                        counter=self._state.counter + event.value,
                        messages=self._state.messages + (event.action,),
                    )

            return TestActorSystem(state)

        async def _save_state(self, state):
            # In real usage, this would save to DB
            # For testing, save via activity so we can verify
            await wf.execute_activity(
                "save_state",
                args=[state.counter, list(state.messages)],
                start_to_close_timeout=timedelta(seconds=5),
            )

    @wf_defn.coordinator
    class _Workflow(BaseCoordinator[SimpleParams, SimpleMessage]):
        @wf.signal
        async def enqueue_event(self, event: SimpleMessage) -> None:
            await self._events.put(event)


# Test 1: Happy Path - Basic Event Processing
@pytest.mark.asyncio
async def test_basic_event_processing(temporal_env: WorkflowEnvironment):
    """
    Test basic event processing through coordinator/driver pattern.
    Verifies that events are processed one-by-one through ephemeral drivers.
    """
    workflow_id = f"test-coordinator-{uuid4()}"
    task_queue = f"test-queue-{uuid4()}"

    # Create activities instance for state tracking
    activities = _Activities()

    async with Worker(
        temporal_env.client,
        task_queue=task_queue,
        workflows=[_Workflow, _Driver],
        activities=[activities.save_state],
        workflow_runner=UnsandboxedWorkflowRunner(),
        debug_mode=True,
    ):
        # Start coordinator
        handle = await temporal_env.client.start_workflow(
            _Workflow.__call__,
            args=[CoordinatorParamsV1[SimpleParams](ac_params=SimpleParams())],
            id=workflow_id,
            task_queue=task_queue,
        )

        # Send test events
        test_events = [
            SimpleMessage(value=1, action="start"),
            SimpleMessage(value=2, action="process"),
            SimpleMessage(value=3, action="finish"),
        ]

        for event in test_events:
            await handle.signal(_Workflow.enqueue_event, event)

        # Give time for processing
        await aio.sleep(2)

        # Verify state was saved correctly
        # Each driver is ephemeral and processes one event independently
        assert len(activities.saved_states) == 3, (
            f"Expected 3 saved states, got {len(activities.saved_states)}"
        )

        # Verify each event was processed correctly
        assert activities.saved_states[0].counter == 1
        assert activities.saved_states[0].messages == ("start",)

        assert activities.saved_states[1].counter == 2
        assert activities.saved_states[1].messages == ("process",)

        assert activities.saved_states[2].counter == 3
        assert activities.saved_states[2].messages == ("finish",)

        # Cancel to finish
        await handle.cancel()
        with pytest.raises(Exception):
            await handle.result()

        print(
            "✅ Basic event processing test completed - verified 3 events processed with correct state"
        )
