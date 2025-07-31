import asyncio as aio
import contextlib
import typing as t


@contextlib.asynccontextmanager
async def running(task: t.Awaitable) -> t.AsyncIterator[None]:
    fut = aio.ensure_future(task)
    try:
        yield
    finally:
        if not fut.cancel():
            return
        with contextlib.suppress(aio.CancelledError):
            await fut


def create_resolved_f[T](value: T) -> aio.Future[T]:
    loop = aio.get_running_loop()
    f = loop.create_future()
    f.set_result(value)
    return f
