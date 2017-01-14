import asyncio

from tattle import logging

LOG = logging.get_logger(__name__)

__all__ = [
    'Timer'
]


class Timer(object):
    def __init__(self, callback, time, loop=None):
        self.func = callback
        self.time = time
        self._loop = loop or asyncio.get_event_loop()
        self._handle = None

    async def start(self):
        assert self._handle is None
        self._handle = self._loop.call_later(self.time, self._run)

    async def reset(self, time):
        self.stop()
        self.time = time
        self.start()

    async def stop(self):
        assert self._handle is not None
        self._handle.cancel()

    def _run(self):
        LOG.trace("Running timer callback: %s", self.func)
        res = self.func()
        if asyncio.coroutines.iscoroutine(res):
            self._loop.create_task(res)
