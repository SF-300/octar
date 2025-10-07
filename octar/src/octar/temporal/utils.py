import asyncio as aio
import typing as t
from dataclasses import dataclass


@dataclass(frozen=True)
class BackoffParams:
    max_retries: int
    initial_delay_seconds: float
    max_delay_seconds: float
    multiplier: float


async def exponential_delays(params: BackoffParams) -> t.AsyncIterator[int]:
    delay_sec = params.initial_delay_seconds
    max_delay_sec = params.max_delay_seconds

    for attempt in range(params.max_retries):
        yield attempt

        if attempt < params.max_retries - 1:
            await aio.sleep(delay_sec)
            delay_sec = min(delay_sec * params.multiplier, max_delay_sec)
