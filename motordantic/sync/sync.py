import asyncio
import functools
from typing import Callable

__all__ = ('force_sync',)


def force_sync(fn: Callable, loop=None):
    '''
    turn an async function to sync function
    '''

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        res = fn(*args, **kwargs)
        if asyncio.iscoroutine(res):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
            return loop.run_until_complete(res)
        return res

    return wrapper


def add_sync_version(func):
    # assert asyncio.iscoroutine(func)

    def wrapper(*args, **kwds):
        return asyncio.new_event_loop().run_until_complete(func, *args, **kwds)

    func.sync = wrapper
    return func
