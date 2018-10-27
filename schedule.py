import asyncio
import inspect
import logging
import functools
import time

from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)

async def extract(*args):
    print('extract...', args)
    for i in range(1000):
        if i == 666:
            raise Exception('evil')
        yield i

async def transform(x):
    await asyncio.sleep(0.1)
    print('transform...', x)

def sync_transform(x):
    time.sleep(0.001)
    print('sync_transform...', x)

class token:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name

begin = token('begin')
end = token('end')

q0 = asyncio.Queue(maxsize=128)
q1 = asyncio.Queue(maxsize=128)
q2 = asyncio.Queue(maxsize=128)
q3 = asyncio.Queue(maxsize=128)

thread_pool_executor = ThreadPoolExecutor(max_workers=1)

def get_asyncgen_node_consumer(node):
    # async generator
    if inspect.isasyncgenfunction(node):
        return node

    # async function
    if inspect.iscoroutinefunction(node):
        @functools.wraps(node)
        async def _asyncgen_coroutine_consumer(*args, **kwargs):
            nonlocal node
            yield (await node(*args, **kwargs))
        return _asyncgen_coroutine_consumer

    # generator
    if inspect.isgeneratorfunction(node):
        @functools.wraps(node)
        async def _asyncgen_generator_consumer(*args, **kwargs):
            nonlocal node
            for x in node(*args, **kwargs):
                yield x
        return _asyncgen_generator_consumer

    # function
    if inspect.isfunction(node):
        @functools.wraps(node)
        async def _asyncgen_consumer(*args, **kwargs):
            nonlocal node
            yield await asyncio.get_event_loop().run_in_executor(thread_pool_executor, functools.partial(node, *args, **kwargs))
        return _asyncgen_consumer

    raise NotImplementedError(repr(node))

async def consume(node, qin, qouts):
    print('<BEGIN>', node, qin, qouts)

    async def send_to_all_outputs(value):
        nonlocal qouts
        for qout in qouts:
            await qout.put(value)

    asyncgen_consumer = get_asyncgen_node_consumer(node)
    print('...', asyncgen_consumer)

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
            qin.task_done()
            continue

        async for pending in asyncgen_consumer(value):
            await send_to_all_outputs(pending)

        qin.task_done()

    print('<END>', node, qin, qouts)

    """
        try:
            fut = asyncio.get_event_loop().run_in_executor(None, node, value)
            pending = await fut
            await send_to_all_outputs(pending)
        except Exception as exc:
            logger.error('{} (in {}): {}'.format(type(exc).__name__, node.__name__, exc))
    """

async def kickstart(queue):
    await queue.put(begin)
    await queue.put(())
    await queue.put(end)

tasks = list(map(asyncio.ensure_future, [
    consume(extract, q0, [q1, q2, q3]),
    consume(transform, q1, []),
    consume(sync_transform, q2, []),
    consume(sync_transform, q3, []),
    kickstart(q0),
]))

asyncio.get_event_loop().run_until_complete(
    asyncio.wait(tasks)
)

