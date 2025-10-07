from dataclasses import dataclass

from octar import ActorMessage


@dataclass(frozen=True, kw_only=True)
class SimpleMessage(ActorMessage):
    value: int
    action: str


@dataclass(frozen=True)
class SimpleState:
    counter: int = 0
    messages: tuple[str, ...] = ()


@dataclass(frozen=True)
class SimpleParams:
    pass
