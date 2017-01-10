import asyncio
import contextlib

from tattle import logging

LOG = logging.get_logger(__name__)


class ScheduledCallback(object):
    def __init__(self, callback, time, loop=None):
        self.func = callback
        self.time = time / 1000.0
        self.is_started = False
        self._loop = loop or asyncio.get_event_loop()
        self._task = None

    async def start(self):
        if not self.is_started:
            self.is_started = True
            self._task = self._loop.create_task(self._run())

    async def stop(self):
        if self.is_started:
            self.is_started = False
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _run(self):
        while True:
            await asyncio.sleep(self.time)
            LOG.trace("Running scheduled callback: %s", self.func)
            await self.func()
