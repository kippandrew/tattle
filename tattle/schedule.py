import asyncio
import contextlib

from tattle import logging

LOG = logging.get_logger(__name__)


class ScheduledCallback(object):
    def __init__(self, callback, interval, loop=None):
        self.func = callback
        self.interval = interval
        self.started = False
        self._loop = loop or asyncio.get_event_loop()
        self._task = None

    async def start(self):
        if not self.started:
            self.started = True
            self._task = self._loop.create_task(self._run())

    async def stop(self):
        if self.started:
            self.started = False
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _run(self):
        while True:
            await asyncio.sleep(self.interval, loop=self._loop)
            LOG.trace("Running scheduled callback: %s", self.func)
            res = self.func()
            if asyncio.coroutines.iscoroutine(res):
                await res
