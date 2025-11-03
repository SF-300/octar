import asyncio as aio
import dataclasses
from dataclasses import dataclass

import pytest

from octar import Actor, ActorMessage, ActorState, ActorSystem
from octar.base import ActorId, Request

# ==================== Message Definitions ====================


@dataclass(frozen=True, kw_only=True)
class IncrementMsg(ActorMessage):
    value: int


@dataclass(frozen=True, kw_only=True)
class ProduceMsg(ActorMessage):
    value: int


@dataclass(frozen=True, kw_only=True)
class ConsumeMsg(ActorMessage):
    value: int


@dataclass(frozen=True, kw_only=True)
class SimpleEchoResponse(ActorMessage):
    result: str


@dataclass(frozen=True, kw_only=True)
class SimpleEchoRequest(Request[SimpleEchoResponse], ActorMessage):
    pass


@dataclass(frozen=True, kw_only=True)
class EchoResponse(ActorMessage):
    result: str


@dataclass(frozen=True, kw_only=True)
class EchoRequest(Request[EchoResponse], ActorMessage):
    message: str


@dataclass(frozen=True, kw_only=True)
class MakeSimpleRequestMsg(ActorMessage):
    pass


@dataclass(frozen=True, kw_only=True)
class MakeNestedRequestMsg(ActorMessage):
    pass


@dataclass(frozen=True, kw_only=True)
class StartChainMsg(ActorMessage):
    pass


@dataclass(frozen=True, kw_only=True)
class ForwardMsg(ActorMessage):
    value: int


@dataclass(frozen=True, kw_only=True)
class TriggerRequestMsg(ActorMessage):
    pass


@dataclass(frozen=True, kw_only=True)
class PauseMsg(ActorMessage):
    pass


@dataclass(frozen=True, kw_only=True)
class ResumeMsg(ActorMessage):
    pass


@dataclass(frozen=True, kw_only=True)
class StashValueMsg(ActorMessage):
    value: int


# ==================== Test Actor System ====================


class TestActorSystem(ActorSystem[None, ActorMessage]):
    @property
    def state(self) -> None:
        return None


# ==================== Test 1: Basic Counter ====================


async def test_actor_state_and_simple_message():
    """Test actor initialization and simple message handling."""

    @dataclass(frozen=True, kw_only=True)
    class CounterState(ActorState[IncrementMsg]):
        value: int = 0

    class CounterActor(Actor[CounterState, IncrementMsg]):
        async def _step(self, state: CounterState, *events: IncrementMsg) -> CounterState:
            new_value = state.value
            for event in events:
                new_value += event.value
            return dataclasses.replace(state, value=new_value)

    # Create an actor system
    system = TestActorSystem()

    # Spawn an actor
    actor = system.spawn(CounterActor, CounterState())

    # Check initial state
    assert actor.state.value == 0

    # Send a message and process it
    async with system:
        system.tell(actor.actor_id, IncrementMsg(value=1))

    # Check updated state
    assert actor.state.value == 1

    # Send multiple messages
    async with system:
        system.tell(actor.actor_id, IncrementMsg(value=2), IncrementMsg(value=3))

    # Check final state
    assert actor.state.value == 6


# ==================== Test 2: Multiple Actors ====================


async def test_multiple_actors():
    """Test multiple actors working together in the system."""

    @dataclass(frozen=True, kw_only=True)
    class ProducerState(ActorState[ProduceMsg]):
        items: list[int] = dataclasses.field(default_factory=list)

    class ProducerActor(Actor[ProducerState, ProduceMsg]):
        async def _step(self, state: ProducerState, *events: ProduceMsg) -> ProducerState:
            new_items = list(state.items)
            for event in events:
                new_items.append(event.value)
            return dataclasses.replace(state, items=new_items)

    @dataclass(frozen=True, kw_only=True)
    class ConsumerState(ActorState[ConsumeMsg]):
        total: int = 0

    class ConsumerActor(Actor[ConsumerState, ConsumeMsg]):
        async def _step(self, state: ConsumerState, *events: ConsumeMsg) -> ConsumerState:
            new_total = state.total
            for event in events:
                new_total += event.value
            return dataclasses.replace(state, total=new_total)

    # Create an actor system
    system = TestActorSystem()

    # Spawn actors
    producer = system.spawn(ProducerActor, ProducerState())
    consumer = system.spawn(ConsumerActor, ConsumerState())

    # Send messages to producer
    async with system:
        system.tell(
            producer.actor_id,
            ProduceMsg(value=1),
            ProduceMsg(value=2),
            ProduceMsg(value=3),
        )

    # Check producer state
    assert producer.state.items == [1, 2, 3]

    # Forward producer state to consumer
    async with system:
        for num in producer.state.items:
            system.tell(consumer.actor_id, ConsumeMsg(value=num))

    # Check consumer state
    assert consumer.state.total == 6


# ==================== Test 3: Request-Response ====================


async def test_actor_request_response():
    """Test request-response pattern with actors."""

    @dataclass(frozen=True, kw_only=True)
    class EchoState(ActorState[SimpleEchoRequest | EchoRequest]):
        pass

    class EchoActor(Actor[EchoState, SimpleEchoRequest | EchoRequest]):
        async def _step(
            self,
            state: EchoState,
            *events: SimpleEchoRequest | EchoRequest,
        ) -> EchoState:
            for event in events:
                if isinstance(event, SimpleEchoRequest):
                    # Send response back to the transient receiver (the requester)
                    self._answer(event, SimpleEchoResponse(result="hello"))
                elif isinstance(event, EchoRequest):
                    # Send response back to the transient receiver
                    self._answer(event, EchoResponse(result=f"Echo: {event.message}"))
            return state

    # Create an actor system
    system = TestActorSystem()

    # Spawn an actor
    actor = system.spawn(EchoActor, EchoState())

    # Create a simple request and send it, then test with message in request
    async with system:
        response = await system.ask(actor.actor_id, SimpleEchoRequest())
        assert response.result == "hello"

        response = await system.ask(actor.actor_id, EchoRequest(message="test message"))
        assert response.result == "Echo: test message"


# ==================== Test 4: Nested Actor Request-Response ====================


async def test_nested_actor_request_response():
    """Test request-response pattern between actors within step functions."""
    print("TEST START")

    @dataclass(frozen=True, kw_only=True)
    class ResponderState(ActorState[SimpleEchoRequest | EchoRequest]):
        pass

    class ResponderActor(Actor[ResponderState, SimpleEchoRequest | EchoRequest]):
        async def _step(
            self,
            state: ResponderState,
            *events: SimpleEchoRequest | EchoRequest,
        ) -> ResponderState:
            print(f"Responder._step: got {len(events)} events")
            for event in events:
                print(f"Responder: event type = {type(event).__name__}")
                print(f"Responder: event._actor_sender_id = {event._actor_sender_id}")
                print(f"Responder: event._actor_asker_id = {event._actor_asker_id}")
                if isinstance(event, SimpleEchoRequest):
                    print("Responder: answering SimpleEchoRequest")
                    self._answer(event, SimpleEchoResponse(result="Simple response"))
                elif isinstance(event, EchoRequest):
                    print(f"Responder: answering EchoRequest with message: {event.message}")
                    response = EchoResponse(result=f"Response to: {event.message}")
                    print(
                        f"Responder: will send response to {event._actor_asker_id or event._actor_sender_id}"
                    )
                    self._answer(event, response)
            print("Responder._step: returning state")
            return state

    @dataclass(frozen=True, kw_only=True)
    class RequesterState(ActorState[MakeSimpleRequestMsg | EchoRequest | MakeNestedRequestMsg]):
        responses: list[str] = dataclasses.field(default_factory=list)
        nested_call_complete: bool = False
        # NOTE: Store responder actor_id to reconstruct ActorRef
        responder_id: ActorId

    class RequesterActor(
        Actor[RequesterState, MakeSimpleRequestMsg | EchoRequest | MakeNestedRequestMsg]
    ):
        async def _step(
            self,
            state: RequesterState,
            *events: MakeSimpleRequestMsg | EchoRequest | MakeNestedRequestMsg,
        ) -> RequesterState:
            print(f"Requester._step: got {len(events)} events")
            new_responses = list(state.responses)
            new_nested_complete = state.nested_call_complete

            for event in events:
                print(f"Requester: event type = {type(event).__name__}")
                print(f"Requester: event._actor_sender_id = {event._actor_sender_id}")
                print(f"Requester: event._actor_asker_id = {event._actor_asker_id}")
                print(f"Requester: self.actor_id = {self.actor_id}")
                if isinstance(event, MakeSimpleRequestMsg):
                    print("Requester: calling _ask for SimpleEchoRequest")
                    response = await self._ask(state.responder_id, SimpleEchoRequest())
                    print(f"Requester: got response: {response.result}")
                    new_responses.append(response.result)
                elif isinstance(event, EchoRequest):
                    print("Requester: forwarding EchoRequest")
                    print(
                        f"Requester: before _ask, event._actor_asker_id = {event._actor_asker_id}"
                    )
                    response = await self._ask(state.responder_id, event)
                    print(f"Requester: got response: {response.result}")
                    new_responses.append(response.result)
                elif isinstance(event, MakeNestedRequestMsg):
                    print("Requester: making nested requests")
                    response1 = await self._ask(state.responder_id, SimpleEchoRequest())
                    response2 = await self._ask(state.responder_id, EchoRequest(message="nested"))
                    new_responses.append(response1.result)
                    new_responses.append(response2.result)
                    new_nested_complete = True

            print(f"Requester._step: returning state with responses={new_responses}")
            return dataclasses.replace(
                state, responses=new_responses, nested_call_complete=new_nested_complete
            )

    # Create an actor system
    system = TestActorSystem()
    print("Created system")

    # Spawn actors
    responder = system.spawn(ResponderActor, ResponderState())
    print(f"Spawned responder: {responder.actor_id}")
    requester = system.spawn(RequesterActor, RequesterState(responder_id=responder.actor_id))
    print(f"Spawned requester: {requester.actor_id}")

    # Test simple request
    print("About to enter context 1")
    async with system:
        print("Inside context 1, telling requester")
        system.tell(requester.actor_id, MakeSimpleRequestMsg())
        print("Told requester, exiting context 1")
    print("Exited context 1")

    assert requester.state.responses == ["Simple response"]

    # Test request with message
    async with system:
        system.tell(requester.actor_id, EchoRequest(message="hello"))

    assert requester.state.responses == ["Simple response", "Response to: hello"]

    # Test nested requests
    async with system:
        system.tell(requester.actor_id, MakeNestedRequestMsg())

    assert requester.state.nested_call_complete
    assert len(requester.state.responses) == 4
    assert requester.state.responses[2:] == ["Simple response", "Response to: nested"]


# ==================== Test 5: Chain of Actors ====================


async def test_chain_of_actors_request_response():
    """Test a chain of actors passing requests through multiple layers."""

    @dataclass(frozen=True, kw_only=True)
    class FinalResponderState(ActorState[SimpleEchoRequest]):
        pass

    class FinalResponderActor(Actor[FinalResponderState, SimpleEchoRequest]):
        async def _step(
            self, state: FinalResponderState, *events: SimpleEchoRequest
        ) -> FinalResponderState:
            for event in events:
                self._answer(
                    event,
                    SimpleEchoResponse(result="Final response"),
                )
            return state

    @dataclass(frozen=True, kw_only=True)
    class MiddleActorState(ActorState[SimpleEchoRequest]):
        final_actor_id: ActorId

    class MiddleActor(Actor[MiddleActorState, SimpleEchoRequest]):
        async def _step(
            self, state: MiddleActorState, *events: SimpleEchoRequest
        ) -> MiddleActorState:
            for event in events:
                final_response = await self._ask(state.final_actor_id, SimpleEchoRequest())
                self._answer(
                    event,
                    SimpleEchoResponse(result=f"Middle processed: {final_response.result}"),
                )
            return state

    @dataclass(frozen=True, kw_only=True)
    class InitiatorState(ActorState[StartChainMsg]):
        response: str = ""
        middle_actor_id: ActorId

    class InitiatorActor(Actor[InitiatorState, StartChainMsg]):
        async def _step(self, state: InitiatorState, *events: StartChainMsg) -> InitiatorState:
            new_response = state.response

            for _event in events:
                response = await self._ask(state.middle_actor_id, SimpleEchoRequest())
                new_response = response.result

            return dataclasses.replace(state, response=new_response)

    # Create system
    system = TestActorSystem()

    # Spawn actors in reverse order
    final_actor = system.spawn(FinalResponderActor, FinalResponderState())
    middle_actor = system.spawn(MiddleActor, MiddleActorState(final_actor_id=final_actor.actor_id))
    initiator = system.spawn(InitiatorActor, InitiatorState(middle_actor_id=middle_actor.actor_id))

    # Start the chain
    async with system:
        system.tell(initiator.actor_id, StartChainMsg())

    # Check the result
    assert initiator.state.response == "Middle processed: Final response"


# ==================== Test 6: Recursive Actor Step Execution ====================


async def test_recursive_actor_step_execution():
    """Test system's ability to handle actors that schedule other actors during their step."""

    @dataclass(frozen=True, kw_only=True)
    class ForwardingState(ActorState[ForwardMsg]):
        target_id: ActorId | None = None
        events: list[int] = dataclasses.field(default_factory=list)

    class ForwardingActor(Actor[ForwardingState, ForwardMsg]):
        async def _step(self, state: ForwardingState, *events: ForwardMsg) -> ForwardingState:
            new_events = list(state.events)
            new_events.extend([e.value for e in events])

            if state.target_id is not None:
                for event in events:
                    self._tell(state.target_id, ForwardMsg(value=event.value * 2))

            return dataclasses.replace(state, events=new_events)

    # Set up system and actors
    system = TestActorSystem()

    # Create two actors - second one first so it can be referenced
    actor2 = system.spawn(ForwardingActor, ForwardingState())
    actor1 = system.spawn(ForwardingActor, ForwardingState(target_id=actor2.actor_id))

    # Send message to actor1
    async with system:
        system.tell(actor1.actor_id, ForwardMsg(value=1), ForwardMsg(value=2))

    # Check states - actor1 should have original messages
    assert actor1.state.events == [1, 2]
    # actor2 should have doubled messages
    assert actor2.state.events == [2, 4]


# ==================== Test 7: Request with Broken Response ====================


async def test_request_with_broken_response():
    """Test handling of requests where the responder breaks the protocol."""

    @dataclass(frozen=True, kw_only=True)
    class BrokenResponderState(ActorState[SimpleEchoRequest]):
        pass

    class BrokenResponderActor(Actor[BrokenResponderState, SimpleEchoRequest]):
        async def _step(
            self, state: BrokenResponderState, *events: SimpleEchoRequest
        ) -> BrokenResponderState:
            # Doesn't send a response
            return state

    @dataclass(frozen=True, kw_only=True)
    class RequesterState(ActorState[TriggerRequestMsg]):
        error: Exception | None = None
        responder_id: ActorId

    class RequesterActor(Actor[RequesterState, TriggerRequestMsg]):
        async def _step(self, state: RequesterState, *events: TriggerRequestMsg) -> RequesterState:
            new_error = state.error

            for _event in events:
                try:
                    # Wait with a timeout to avoid test hanging
                    await aio.wait_for(self._ask(state.responder_id, SimpleEchoRequest()), 0.1)
                except Exception as e:
                    new_error = e

            return dataclasses.replace(state, error=new_error)

    # Set up actors
    system = TestActorSystem()
    responder = system.spawn(BrokenResponderActor, BrokenResponderState())
    requester = system.spawn(RequesterActor, RequesterState(responder_id=responder.actor_id))

    # Send request
    async with system:
        system.tell(requester.actor_id, TriggerRequestMsg())

    # Should have a timeout error
    assert requester.state.error is not None
    assert isinstance(requester.state.error, aio.TimeoutError)


# ==================== Test 8: Actor Step Exception Handling ====================


@pytest.mark.asyncio
async def test_actor_step_exception_handling():
    """Test how exceptions in actor step functions are handled."""

    @dataclasses.dataclass(frozen=True, kw_only=True)
    class FailingActorState(ActorState[IncrementMsg]):
        value: int = 0

    class FailingActor(Actor[FailingActorState, IncrementMsg]):
        async def _step(self, state: FailingActorState, *events: IncrementMsg) -> FailingActorState:
            if any(event.value < 0 for event in events):
                raise ValueError("Negative values not allowed")
            return dataclasses.replace(state, value=state.value + sum(e.value for e in events))

    system = TestActorSystem()
    actor = system.spawn(FailingActor, FailingActorState())

    # Send valid messages
    async with system:
        system.tell(
            actor.actor_id,
            IncrementMsg(value=1),
            IncrementMsg(value=2),
            IncrementMsg(value=3),
        )
    assert actor.state.value == 6

    # Send message that causes exception
    with pytest.raises(BaseExceptionGroup) as exc_info:
        async with system:
            system.tell(actor.actor_id, IncrementMsg(value=-1))

    # Check that the exception group contains our ValueError
    assert any(
        isinstance(e, ValueError) and "Negative values not allowed" in str(e)
        for e in exc_info.value.exceptions
    )

    # State should remain unchanged after exception
    assert actor.state.value == 6


# ==================== Test 9: Stash Happy Path ====================


@pytest.mark.asyncio
async def test_stash_unstash_happy_path_preserves_order_and_empties_stash():
    """Events stashed while paused are replayed in-order on resume; stash is emptied."""

    @dataclass(frozen=True, kw_only=True)
    class S(ActorState[PauseMsg | ResumeMsg | StashValueMsg]):
        paused: bool = False
        processed: list[int] = dataclasses.field(default_factory=list)
        stashed_count: int = 0

    class StashingActor(Actor[S, PauseMsg | ResumeMsg | StashValueMsg]):
        async def _step(self, state: S, *events: PauseMsg | ResumeMsg | StashValueMsg) -> S:
            new_paused = state.paused
            new_processed = list(state.processed)
            new_stashed_count = state.stashed_count

            for ev in events:
                if isinstance(ev, PauseMsg):
                    new_paused = True
                elif isinstance(ev, ResumeMsg):
                    new_paused = False
                    self._stash.unstash_all()
                elif isinstance(ev, StashValueMsg):
                    if new_paused:
                        self._stash(ev)
                        new_stashed_count += 1
                    else:
                        new_processed.append(ev.value)

            return dataclasses.replace(
                state,
                paused=new_paused,
                processed=new_processed,
                stashed_count=new_stashed_count,
            )

    system = TestActorSystem()
    actor = system.spawn(StashingActor, S())

    async with system:
        system.tell(actor.actor_id, PauseMsg())
    assert actor.state.paused is True

    async with system:
        system.tell(
            actor.actor_id,
            StashValueMsg(value=1),
            StashValueMsg(value=2),
            StashValueMsg(value=3),
        )
    assert actor.state.processed == []
    assert actor.state.stashed_count == 3  # Verify 3 items were stashed

    async with system:
        system.tell(actor.actor_id, ResumeMsg())
    assert actor.state.paused is False
    assert actor.state.processed == [
        1,
        2,
        3,
    ]  # Verify stashed items were processed in order


# ==================== Test 10: Multiple Batches Stashed ====================


@pytest.mark.asyncio
async def test_multiple_batches_stashed_then_single_resume_replays_all_in_order():
    """Several paused rounds accumulate in stash; one resume replays them FIFO."""

    @dataclass(frozen=True, kw_only=True)
    class S(ActorState[PauseMsg | ResumeMsg | StashValueMsg]):
        paused: bool = False
        processed: list[int] = dataclasses.field(default_factory=list)
        stashed_count: int = 0

    class StashingActor(Actor[S, PauseMsg | ResumeMsg | StashValueMsg]):
        async def _step(self, state: S, *events: PauseMsg | ResumeMsg | StashValueMsg) -> S:
            new_paused = state.paused
            new_processed = list(state.processed)
            new_stashed_count = state.stashed_count

            for ev in events:
                if isinstance(ev, PauseMsg):
                    new_paused = True
                elif isinstance(ev, ResumeMsg):
                    new_paused = False
                    self._stash.unstash_all()
                elif isinstance(ev, StashValueMsg):
                    if new_paused:
                        self._stash(ev)
                        new_stashed_count += 1
                    else:
                        new_processed.append(ev.value)

            return dataclasses.replace(
                state,
                paused=new_paused,
                processed=new_processed,
                stashed_count=new_stashed_count,
            )

    system = TestActorSystem()
    actor = system.spawn(StashingActor, S())

    async with system:
        system.tell(actor.actor_id, PauseMsg())

    async with system:
        system.tell(actor.actor_id, StashValueMsg(value=1), StashValueMsg(value=2))
    assert actor.state.stashed_count == 2  # Verify 2 items were stashed
    assert actor.state.processed == []

    async with system:
        system.tell(actor.actor_id, StashValueMsg(value=3), StashValueMsg(value=4))
    assert actor.state.stashed_count == 4  # Verify 4 items total were stashed
    assert actor.state.processed == []

    async with system:
        system.tell(actor.actor_id, ResumeMsg())
    assert actor.state.processed == [
        1,
        2,
        3,
        4,
    ]  # Verify all stashed items were processed in FIFO order
