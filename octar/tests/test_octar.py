import asyncio as aio
import dataclasses
import typing as t
from dataclasses import dataclass

import pytest

from octar import Actor, ActorState, ActorSystem
from octar.base import Request
from octar.core import Registrator


# Concrete implementation of ActorSystem for testing
class TestActorSystem(ActorSystem[None, t.Any]):
    @property
    def state(self) -> None:
        return None


async def test_actor_state_and_simple_message():
    """Test actor initialization and simple message handling."""

    @dataclass(frozen=True)
    class CounterState(ActorState):
        value: int = 0

    class CounterActor(Actor):
        async def _step(self, state: CounterState, *events: int) -> CounterState:
            new_value = state.value
            for event in events:
                new_value += event
            return dataclasses.replace(state, value=new_value)

    # Create an actor system
    system = TestActorSystem()

    # Spawn an actor
    actor = system.spawn(CounterActor, CounterState())

    # Check initial state
    assert actor.state.value == 0

    # Send a message and process it
    actor.tell(1)
    await system.step()

    # Check updated state
    assert actor.state.value == 1

    # Send multiple messages
    actor.tell(2, 3)
    await system.step()

    # Check final state
    assert actor.state.value == 6


async def test_multiple_actors():
    """Test multiple actors working together in the system."""

    @dataclass(frozen=True)
    class ProducerState(ActorState):
        items: list[int] = dataclasses.field(default_factory=list)

    class ProducerActor(Actor):
        async def _step(self, state: ProducerState, *events: t.Any) -> ProducerState:
            # Produce numbers and append to state
            new_items = list(state.items)
            for event in events:
                if isinstance(event, int):
                    new_items.append(event)
            return dataclasses.replace(state, items=new_items)

    @dataclass(frozen=True)
    class ConsumerState(ActorState):
        total: int = 0

    class ConsumerActor(Actor):
        async def _step(self, state: ConsumerState, *events: int) -> ConsumerState:
            # Sum up all incoming numbers
            new_total = state.total
            for event in events:
                new_total += event
            return dataclasses.replace(state, total=new_total)

    # Create an actor system
    system = TestActorSystem()

    # Spawn actors
    producer = system.spawn(ProducerActor, ProducerState())
    consumer = system.spawn(ConsumerActor, ConsumerState())

    # Send messages to producer
    producer.tell(1, 2, 3)
    await system.step()

    # Check producer state
    assert producer.state.items == [1, 2, 3]

    # Forward producer state to consumer
    for num in producer.state.items:
        consumer.tell(num)
    await system.step()

    # Check consumer state
    assert consumer.state.total == 6


async def test_actor_request_response():
    """Test request-response pattern with actors."""

    # Define a state with requests dict
    @dataclass(frozen=True)
    class EchoState(ActorState):
        pending_requests: dict[str, Request[str]] = dataclasses.field(default_factory=dict)

    class EchoActor(Actor):
        async def _step(
            self,
            state: EchoState,
            *events: t.Union[tuple[str, str], Request[str], tuple[str, Request[str]]],
        ) -> EchoState:
            new_pending = dict(state.pending_requests)
            for event in events:
                if isinstance(event, Request):
                    # Just echo back "hello" for any request
                    event.set_result("hello")
                elif isinstance(event, tuple) and len(event) == 2:
                    if isinstance(event[1], Request):
                        # Register request with ID
                        request_id, request = event[0], event[1]
                        new_pending[request_id] = request
                    else:
                        # Handle message with ID
                        request_id, message = event[0], event[1]
                        if request_id in new_pending:
                            new_pending[request_id].set_result(f"Echo: {message}")
                            new_pending.pop(request_id)
            return dataclasses.replace(state, pending_requests=new_pending)

    # Create an actor system
    system = TestActorSystem()

    # Spawn an actor
    actor = system.spawn(EchoActor, EchoState())

    # Create a request and send it
    request = Request[str]()
    actor.ask(request)

    # Process the request
    await system.step()

    # Check the response
    result = await request
    assert result == "hello"

    # Test with request ID pattern
    request = Request[str]()

    # Register request with ID first
    actor.tell(("req1", request))
    await system.step()

    # Send message with ID
    actor.tell(("req1", "test message"))
    await system.step()

    # Check the response
    result = await request
    assert result == "Echo: test message"


async def test_nested_actor_request_response():
    """Test request-response pattern between actors within step functions."""

    # Define a responder actor state and step
    @dataclass(frozen=True)
    class ResponderState(ActorState):
        pass

    class ResponderActor(Actor):
        async def _step(
            self,
            state: ResponderState,
            *events: t.Union[Request[str], tuple[str, Request[str]]],
        ) -> ResponderState:
            for event in events:
                if isinstance(event, Request):
                    # Simple request
                    event.set_result("Simple response")
                elif isinstance(event, tuple) and len(event) == 2 and isinstance(event[1], Request):
                    # Message with request
                    message, req = event
                    req.set_result(f"Response to: {message}")
            return state

    # Define a requester actor state and step
    @dataclass(frozen=True)
    class RequesterState(ActorState):
        responses: list[str] = dataclasses.field(default_factory=list)
        nested_call_complete: bool = False

    class RequesterActor(Actor):
        def __init__(self, register_actor: Registrator, state: RequesterState, responder: Actor):
            super().__init__(register_actor, state)
            self.responder = responder

        async def _step(self, state: RequesterState, *events: t.Union[str, bool]) -> RequesterState:
            new_responses = list(state.responses)
            new_nested_complete = state.nested_call_complete

            for event in events:
                if isinstance(event, str):
                    if event == "make_simple_request":
                        # Make a simple request and wait for response
                        request = Request[str]()
                        response = await self.responder.ask(request)
                        new_responses.append(response)
                    else:
                        # Make a request with a message
                        request = Request[str]()
                        self.responder.tell((event, request))
                        # Wait for response within step
                        response = await request
                        new_responses.append(response)
                elif isinstance(event, bool) and event:
                    # Test nested calls
                    request1 = Request[str]()
                    response1 = await self.responder.ask(request1)

                    # After first response arrives, send second request
                    request2 = Request[str]()
                    self.responder.tell(("nested", request2))

                    # Wait for second response
                    response2 = await request2

                    new_responses.append(response1)
                    new_responses.append(response2)
                    new_nested_complete = True

            return dataclasses.replace(
                state, responses=new_responses, nested_call_complete=new_nested_complete
            )

    # Create an actor system
    system = TestActorSystem()

    # Spawn actors
    responder = system.spawn(ResponderActor, ResponderState())
    requester = system.spawn(RequesterActor, RequesterState(), responder)

    # Test simple request
    requester.tell("make_simple_request")
    await system.step()

    # Check requester state
    assert requester.state.responses == ["Simple response"]

    # Test request with message
    requester.tell("hello")
    await system.step()

    assert requester.state.responses == ["Simple response", "Response to: hello"]

    # Test nested requests
    requester.tell(True)  # Trigger nested requests
    await system.step()

    assert requester.state.nested_call_complete
    assert len(requester.state.responses) == 4
    assert requester.state.responses[2:] == ["Simple response", "Response to: nested"]


async def test_chain_of_actors_request_response():
    """Test a chain of actors passing requests through multiple layers."""

    @dataclass(frozen=True)
    class FinalResponderState(ActorState):
        pass

    class FinalResponderActor(Actor):
        async def _step(
            self, state: FinalResponderState, *events: Request[str]
        ) -> FinalResponderState:
            for event in events:
                if isinstance(event, Request):
                    event.set_result("Final response")
            return state

    @dataclass(frozen=True)
    class MiddleActorState(ActorState):
        pass

    class MiddleActor(Actor):
        def __init__(
            self,
            register_actor: Registrator,
            state: MiddleActorState,
            final_actor: Actor,
        ):
            super().__init__(register_actor, state)
            self.final_actor = final_actor

        async def _step(self, state: MiddleActorState, *events: Request[str]) -> MiddleActorState:
            for event in events:
                if isinstance(event, Request):
                    # Forward request to final actor
                    forward_request = Request[str]()
                    final_response = await self.final_actor.ask(forward_request)

                    # Send modified response back to original requester
                    event.set_result(f"Middle processed: {final_response}")
            return state

    @dataclass(frozen=True)
    class InitiatorState(ActorState):
        response: str = ""

    class InitiatorActor(Actor):
        def __init__(
            self,
            register_actor: Registrator,
            state: InitiatorState,
            middle_actor: Actor,
        ):
            super().__init__(register_actor, state)
            self.middle_actor = middle_actor

        async def _step(self, state: InitiatorState, *events: str) -> InitiatorState:
            new_response = state.response
            for event in events:
                if event == "start_chain":
                    # Send request to middle actor
                    request = Request[str]()
                    response = await self.middle_actor.ask(request)
                    new_response = response

            return dataclasses.replace(state, response=new_response)

    # Create system
    system = TestActorSystem()

    # Spawn actors in reverse order
    final_actor = system.spawn(FinalResponderActor, FinalResponderState())
    middle_actor = system.spawn(MiddleActor, MiddleActorState(), final_actor)
    initiator = system.spawn(InitiatorActor, InitiatorState(), middle_actor)

    # Start the chain
    initiator.tell("start_chain")
    await system.step()

    # Check the result
    assert initiator.state.response == "Middle processed: Final response"


async def test_high_throughput():
    """Test actor system performance with high message throughput."""

    @dataclass(frozen=True)
    class CounterState(ActorState):
        value: int = 0

    class CounterActor(Actor):
        async def _step(self, state: CounterState, *events: int) -> CounterState:
            new_value = state.value
            for event in events:
                new_value += event
            return dataclasses.replace(state, value=new_value)

    # Set up system and actor
    system = TestActorSystem()
    actor = system.spawn(CounterActor, CounterState())

    # Send a large number of messages
    message_count = 1000
    for i in range(message_count):
        actor.tell(1)

    # Process all messages
    await system.step()

    # Check final state
    assert actor.state.value == message_count


async def test_recursive_actor_step_execution():
    """Test system's ability to handle actors that schedule other actors during their step."""

    @dataclass(frozen=True)
    class ForwardingState(ActorState):
        target: Actor | None = None
        events: list[int] = dataclasses.field(default_factory=list)

    class ForwardingActor(Actor):
        async def _step(self, state: ForwardingState, *events: int) -> ForwardingState:
            # Record events
            new_events = list(state.events)
            new_events.extend(events)

            # Forward to target if set
            if state.target is not None:
                for event in events:
                    state.target.tell(event * 2)  # Double the value when forwarding

            return dataclasses.replace(state, events=new_events)

    # Set up system and actors
    system = TestActorSystem()

    # Create two actors - second one first so it can be referenced
    actor2 = system.spawn(ForwardingActor, ForwardingState())
    actor1 = system.spawn(ForwardingActor, ForwardingState(target=actor2))

    # Send message to actor1
    actor1.tell(1, 2)
    await system.step()

    # Check states - actor1 should have original messages
    assert actor1.state.events == [1, 2]
    # actor2 should have doubled messages
    assert actor2.state.events == [2, 4]


async def test_request_with_broken_response():
    """Test handling of requests where the responder breaks the protocol."""

    @dataclass(frozen=True)
    class BrokenResponderState(ActorState):
        pass

    class BrokenResponderActor(Actor):
        async def _step(
            self, state: BrokenResponderState, *events: Request[str]
        ) -> BrokenResponderState:
            # Doesn't set a result or exception on the request
            return state

    @dataclass(frozen=True)
    class RequesterState(ActorState):
        error: Exception | None = None

    class RequesterActor(Actor):
        def __init__(self, register_actor: Registrator, state: RequesterState, responder: Actor):
            super().__init__(register_actor, state)
            self.responder = responder

        async def _step(self, state: RequesterState, *events: str) -> RequesterState:
            new_error = state.error
            if "request" in events:
                request = Request[str]()

                try:
                    # Wait with a timeout to avoid test hanging
                    await aio.wait_for(self.responder.ask(request), 0.1)
                except Exception as e:
                    new_error = e

            return dataclasses.replace(state, error=new_error)

    # Set up actors
    system = TestActorSystem()
    responder = system.spawn(BrokenResponderActor, BrokenResponderState())
    requester = system.spawn(RequesterActor, RequesterState(), responder)

    # Send request
    requester.tell("request")
    await system.step()

    # Should have a timeout error
    assert requester.state.error is not None
    assert isinstance(requester.state.error, aio.TimeoutError)


@pytest.mark.asyncio
async def test_actor_step_exception_handling():
    """Test how exceptions in actor step functions are handled."""

    @dataclasses.dataclass(frozen=True)
    class FailingActorState(ActorState):
        value: int = 0

    class FailingActor(Actor):
        async def _step(self, state: FailingActorState, *events: int) -> FailingActorState:
            if any(event < 0 for event in events):
                raise ValueError("Negative values not allowed")
            return dataclasses.replace(state, value=state.value + sum(events))

    system = TestActorSystem()
    actor = system.spawn(FailingActor, FailingActorState())

    # Send valid messages
    actor.tell(1, 2, 3)
    await system.step()
    assert actor.state.value == 6

    # Send message that causes exception
    actor.tell(-1)

    with pytest.RaisesGroup(
        pytest.RaisesExc(ValueError, match="Negative values not allowed"),
    ):
        await system.step()

    # State should remain unchanged after exception
    assert actor.state.value == 6


@pytest.mark.asyncio
async def test_stash_unstash_happy_path_preserves_order_and_empties_stash():
    """Events stashed while paused are replayed in-order on resume; stash is emptied."""

    @dataclass(frozen=True)
    class S(ActorState):
        paused: bool = False
        processed: list[int] = dataclasses.field(default_factory=list)
        stashed_count: int = 0

    class StashingActor(Actor):
        def __init__(self, register: Registrator, state: S):
            super().__init__(register, state)

        async def _step(self, state: S, *events: t.Union[str, int]) -> S:
            new_paused = state.paused
            new_processed = list(state.processed)
            new_stashed_count = state.stashed_count

            for ev in events:
                if ev == "pause":
                    new_paused = True
                elif ev == "resume":
                    new_paused = False
                    self._stash.unstash_all()
                elif isinstance(ev, int):
                    if new_paused:
                        self._stash(ev)
                        new_stashed_count += 1
                    else:
                        new_processed.append(ev)

            return dataclasses.replace(
                state,
                paused=new_paused,
                processed=new_processed,
                stashed_count=new_stashed_count,
            )

    system = TestActorSystem()
    actor = system.spawn(StashingActor, S())

    actor.tell("pause")
    await system.step()
    assert actor.state.paused is True

    actor.tell(1, 2, 3)
    await system.step()
    assert actor.state.processed == []
    assert actor.state.stashed_count == 3  # Verify 3 items were stashed

    actor.tell("resume")
    await system.step()
    assert actor.state.paused is False
    assert actor.state.processed == [
        1,
        2,
        3,
    ]  # Verify stashed items were processed in order


@pytest.mark.asyncio
async def test_multiple_batches_stashed_then_single_resume_replays_all_in_order():
    """Several paused rounds accumulate in stash; one resume replays them FIFO."""

    @dataclass(frozen=True)
    class S(ActorState):
        paused: bool = False
        processed: list[int] = dataclasses.field(default_factory=list)
        stashed_count: int = 0

    class StashingActor(Actor):
        def __init__(self, register: Registrator, state: S):
            super().__init__(register, state)

        async def _step(self, state: S, *events: t.Union[str, int]) -> S:
            new_paused = state.paused
            new_processed = list(state.processed)
            new_stashed_count = state.stashed_count

            for ev in events:
                if ev == "pause":
                    new_paused = True
                elif ev == "resume":
                    new_paused = False
                    self._stash.unstash_all()
                elif isinstance(ev, int):
                    if new_paused:
                        self._stash(ev)
                        new_stashed_count += 1
                    else:
                        new_processed.append(ev)

            return dataclasses.replace(
                state,
                paused=new_paused,
                processed=new_processed,
                stashed_count=new_stashed_count,
            )

    system = TestActorSystem()
    actor = system.spawn(StashingActor, S())

    actor.tell("pause")
    await system.step()

    actor.tell(1, 2)
    await system.step()
    assert actor.state.stashed_count == 2  # Verify 2 items were stashed
    assert actor.state.processed == []

    actor.tell(3, 4)
    await system.step()
    assert actor.state.stashed_count == 4  # Verify 4 items total were stashed
    assert actor.state.processed == []

    actor.tell("resume")
    await system.step()
    assert actor.state.processed == [
        1,
        2,
        3,
        4,
    ]  # Verify all stashed items were processed in FIFO order
