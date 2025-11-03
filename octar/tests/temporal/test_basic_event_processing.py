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

from dataclasses import dataclass

from octar import ActorSystem
from octar.base import ActorId
from octar.core import ActorMessage, Request
from octar.environment import Environment
from octar.temporal.workflow import ActorSystemHostWorkflowV1, Params

from ._utils import SimpleMessage, SimpleParams, SimpleState

# Known actor ID for routing messages
ENTRY_ACTOR_ID = ActorId("entry")


# Concrete Params subclass for our test workflow
@dataclass(frozen=True)
class SimpleWorkflowParams(Params):
    simple_params: SimpleParams = SimpleParams()


# Request/Response message types for ask test
@dataclass(frozen=True, kw_only=True)
class ComputeRequest(SimpleMessage, Request["ComputeResponse"]):
    """Request message that expects a ComputeResponse."""
    pass


@dataclass(frozen=True, kw_only=True)
class ComputeResponse(ActorMessage):
    """Response message with computed result."""
    result: int


# Activities for test verification
class _Activities:
    def __init__(self):
        self.saved_states: list[SimpleState] = []

    @activity.defn
    async def save_state(self, counter: int, messages: list[str]) -> None:
        self.saved_states.append(SimpleState(counter=counter, messages=tuple(messages)))


# Test actor system implementation
class TestActorSystem(ActorSystem[SimpleState, SimpleMessage]):
    """Minimal actor system for testing event processing."""

    def __init__(self, env: Environment):
        super().__init__(env)
        self._state = SimpleState()
        self._entry_actor_id = ENTRY_ACTOR_ID

        # Register external receiver for the entry actor
        def process_message(msg: SimpleMessage):
            """Process message and update state."""
            wf.logger.info(f"Processing event: {msg.action} with value {msg.value}")
            self._state = SimpleState(
                counter=self._state.counter + msg.value,
                messages=self._state.messages + (msg.action,),
            )

            # Handle request/response pattern
            if isinstance(msg, ComputeRequest):
                # Compute result and send response to asker
                result = self._state.counter * 10  # Example computation
                response = ComputeResponse(result=result, _actor_message_id=env.create_message_id())

                # Send response back to the asker
                asker_id = msg._actor_asker_id
                if asker_id:
                    self.tell(asker_id, response)

                return response  # Also return for external receiver pattern

        self.register_external(process_message, self._entry_actor_id)

    @property
    def state(self) -> SimpleState:
        """Return current aggregate state."""
        return self._state


# Test workflow using ActorSystemHostWorkflowV1
@wf.defn
class SimpleWorkflow(ActorSystemHostWorkflowV1[SimpleState, SimpleMessage]):
    """Test workflow that processes events through actor system."""

    @wf.signal
    async def enqueue_event(self, event: SimpleMessage) -> None:
        """Signal handler: fire-and-forget event delivery."""
        self._tell(ENTRY_ACTOR_ID, event)

    @wf.update
    async def compute_and_respond(self, request: ComputeRequest) -> ComputeResponse:
        """Update handler: request-response pattern using _ask()."""
        return await self._ask(ENTRY_ACTOR_ID, request)

    async def _create_actor_system(
        self, env: Environment, params: Params
    ) -> ActorSystem[SimpleState, SimpleMessage]:
        """Create test actor system with entry actor."""
        return TestActorSystem(env)

    async def _save_state(self, state: SimpleState) -> None:
        """Save state via activity for test verification."""
        await wf.execute_activity(
            "save_state",
            args=[state.counter, list(state.messages)],
            start_to_close_timeout=timedelta(seconds=5),
        )


# Test: Happy Path - Basic Event Processing
@pytest.mark.asyncio
async def test_basic_event_processing(temporal_env: WorkflowEnvironment):
    """
    Test basic event processing through ActorSystemHostWorkflowV1.
    Verifies that events are processed sequentially with correct state updates.
    """
    workflow_id = f"test-simple-{uuid4()}"
    task_queue = f"test-queue-{uuid4()}"

    # Create activities instance for state tracking
    activities = _Activities()

    async with Worker(
        temporal_env.client,
        task_queue=task_queue,
        workflows=[SimpleWorkflow],
        activities=[activities.save_state],
        workflow_runner=UnsandboxedWorkflowRunner(),
        debug_mode=True,
    ):
        # Start workflow
        handle = await temporal_env.client.start_workflow(
            SimpleWorkflow.__call__,
            args=[SimpleWorkflowParams()],
            id=workflow_id,
            task_queue=task_queue,
        )

        # Send test events (message IDs generated outside workflow context)
        from octar import DefaultEnv
        env = DefaultEnv()
        test_events = [
            SimpleMessage(value=1, action="start", _actor_message_id=env.create_message_id()),
            SimpleMessage(value=2, action="process", _actor_message_id=env.create_message_id()),
            SimpleMessage(value=3, action="finish", _actor_message_id=env.create_message_id()),
        ]

        for event in test_events:
            await handle.signal(SimpleWorkflow.enqueue_event, event)

        # Synchronize by cancelling - this ensures all queued signals are processed
        # The workflow drains the queue before actually stopping
        await handle.cancel()
        with pytest.raises(Exception):
            await handle.result()

        # Verify state was saved (note: batching optimization may reduce save count)
        # The workflow batches messages that arrive together, saving state once per batch
        assert len(activities.saved_states) >= 1, (
            f"Expected at least 1 saved state, got {len(activities.saved_states)}"
        )

        # Verify final state has all events processed correctly
        final_state = activities.saved_states[-1]
        assert final_state.counter == 6, f"Expected counter=6, got {final_state.counter}"
        assert final_state.messages == ("start", "process", "finish"), (
            f"Expected all messages, got {final_state.messages}"
        )

        print(
            "✅ Basic event processing test completed - verified 3 events processed with cumulative state"
        )


# Test 2: Request/Response Pattern with _ask()
@pytest.mark.asyncio
async def test_ask_response_pattern(temporal_env: WorkflowEnvironment):
    """
    Test request/response pattern through ActorSystemHostWorkflowV1._ask().
    Verifies that @wf.update handlers can await responses from actor system.
    """
    workflow_id = f"test-ask-{uuid4()}"
    task_queue = f"test-queue-{uuid4()}"

    # Create activities instance for state tracking
    activities = _Activities()

    async with Worker(
        temporal_env.client,
        task_queue=task_queue,
        workflows=[SimpleWorkflow],
        activities=[activities.save_state],
        workflow_runner=UnsandboxedWorkflowRunner(),
        debug_mode=True,
    ):
        # Start workflow
        handle = await temporal_env.client.start_workflow(
            SimpleWorkflow.__call__,
            args=[SimpleWorkflowParams()],
            id=workflow_id,
            task_queue=task_queue,
        )

        # Send a signal first to set counter to 5
        from octar import DefaultEnv

        env = DefaultEnv()
        setup_event = SimpleMessage(
            value=5, action="setup", _actor_message_id=env.create_message_id()
        )
        await handle.signal(SimpleWorkflow.enqueue_event, setup_event)

        # Send a request via update and await response
        # Update blocks until workflow processes and returns response
        request = ComputeRequest(
            value=0, action="compute", _actor_message_id=env.create_message_id()
        )
        response = await handle.execute_update(SimpleWorkflow.compute_and_respond, request)

        # Verify response (returned synchronously from update)
        assert isinstance(response, ComputeResponse)
        assert response.result == 50, f"Expected result 50 (5*10), got {response.result}"

        # Synchronize by cancelling to ensure signal is fully processed
        await handle.cancel()
        with pytest.raises(Exception):
            await handle.result()

        # Verify state was saved (setup + request)
        assert len(activities.saved_states) == 2, (
            f"Expected 2 saved states, got {len(activities.saved_states)}"
        )
        assert activities.saved_states[0].counter == 5
        assert activities.saved_states[1].counter == 5
        assert activities.saved_states[1].messages == ("setup", "compute")

        print("✅ Ask/response test completed - verified request/response pattern works")
