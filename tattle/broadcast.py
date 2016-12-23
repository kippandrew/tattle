import collections

from tattle import logging

LOG = logging.get_logger(__name__)


class BroadcastQueue():
    def __init__(self):
        self._queue = collections.deque()

    def push(self, message):
        LOG.debug("Queue message: %s", message)
        self._queue.append(message)

    def fetch(self, n=None):
        while True:
            try:
                yield self._queue.popleft()
            except IndexError:
                break
