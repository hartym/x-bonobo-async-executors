import asyncio
import inspect
import logging


logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)


async def extract(*args):
    print('extract...', args)
    for i in range(10):
        await asyncio.sleep(1)
        yield i

async def transform(x):
    print('transform...', x)

def sync_transform(x):
    raise RuntimeError('woops')
    print('sync_transform...', x)

class token:
    pass

begin = token()
end = token()

q0 = asyncio.Queue()
q1 = asyncio.Queue()
q2 = asyncio.Queue()

async def consume(consumer, qin, qouts):
    print('<BEGIN>', consumer, qin, qouts)

    async def send_to_all_outputs(value):
        nonlocal qouts
        for qout in qouts:
            await qout.put(value)

    started = False
    runlevel = 0

    while not started or runlevel:
        value = await qin.get()

        # tokens are passed unchanged
        if isinstance(value, token):
            await send_to_all_outputs(value)

            if value is begin:
                started = True
                runlevel += 1
            elif value is end:
                runlevel -= 1

            continue

        # async generator
        elif inspect.isasyncgenfunction(consumer):
            async for pending in consumer(value):
                await send_to_all_outputs(pending)

        # async function
        elif inspect.iscoroutinefunction(consumer):
            pending = await consumer(value)
            await send_to_all_outputs(pending)

        # generator
        elif inspect.isgeneratorfunction(consumer):
            for pending in consumer(value):
                await send_to_all_outputs(pending)

        # function
        elif inspect.isfunction(consumer):
            try:
                fut = asyncio.get_event_loop().run_in_executor(None, consumer, value)
                pending = await fut
                await send_to_all_outputs(pending)
            except Exception as exc:
                logger.error('{} (in {}): {}'.format(type(exc).__name__, consumer.__name__, exc))

        else:
            raise NotImplementedError(repr(consumer))

    print('<END>', consumer, qin, qouts)

async def kickstart(queue):
    await queue.put(begin)
    await queue.put(())
    await queue.put(end)

tasks = list(map(asyncio.ensure_future, [
    consume(extract, q0, [q1, q2]),
    consume(transform, q1, []),
    consume(sync_transform, q2, []),
    kickstart(q0),
]))


asyncio.get_event_loop().run_until_complete(asyncio.wait(tasks))


