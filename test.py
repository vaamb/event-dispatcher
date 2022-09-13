import asyncio

from src.dispatcher import AsyncBaseDispatcher

d = AsyncBaseDispatcher("test")


async def test_func(truc):
    print(truc)

d.fallback = test_func


async def emit():
    await d.emit("test", "fallback", "arg", room=None)


loop = asyncio.get_event_loop()
d.start(loop=loop)
loop.create_task(emit())

loop.run_forever()
