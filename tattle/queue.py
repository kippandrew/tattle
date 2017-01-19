from tattle import logging

__all__ = [
    'BroadcastQueue',
]

LOG = logging.get_logger(__name__)


class _BroadcastQueueItem:
    def __init__(self, node, message, transmits=0):
        self.node = node
        self.message = message
        self.transmits = transmits


class BroadcastQueue:
    def __init__(self, max_size=0):
        self._max_size = max_size
        self._queue = list()

    def __len__(self):
        return len(self._queue)

    def push(self, node, message):
        self._push_item(_BroadcastQueueItem(node, message))

    def _push_item(self, item):

        # remove invalided messages
        self._queue = [i for i in self._queue if i.node != item.node]

        # add message
        self._queue.append(item)  # newer messages inserted at the end

        # prune queue if its get too large
        if 0 < self._max_size < len(self._queue):
            self.prune()

    def _next_item(self):
        if len(self._queue) < 1:
            return None
        return self._queue[len(self._queue) - 1]  # sorted oldest to newest, return newest

    def _pop_item(self, max_transmits):
        item = None

        while True:
            item = self._next_item()
            if item is None:
                break

            # don't pop a massage if it exceeds max transmits
            if item.transmits < max_transmits:
                item.transmits += 1
                break
            else:
                self._queue.pop()
                continue

        self.sort()
        return item

    def pop(self, max_transmits):
        item = self._pop_item(max_transmits)
        if item is None:
            return None
        return item.message

    def prune(self):
        raise NotImplementedError()

    def sort(self):
        # sort queue in place (oldest to newest)
        self._queue.sort(key=lambda i: i.transmits, reverse=True)

    def fetch(self, max_transmits, max_bytes, max_messages=0):
        """
        Pop messages up until max bytes
        :param max_transmits:
        :param max_bytes:
        :param max_messages:
        :return:
        """
        remaining = max_bytes
        messages = []

        for i in reversed(range(len(self._queue))):
            item = self._queue[i]

            # stop iterating if reached max messages
            if 0 < max_messages < len(messages):
                break

            # stop iterating if buffer is exhausted
            if len(item.message) > remaining:
                break

            messages.append(item.message)

            remaining -= len(item.message)

            # remove item if it exceeds max transmits
            item.transmits += 1
            if item.transmits >= max_transmits:
                del self._queue[i]

        # re-sort messages if necessary
        if len(messages) > 0:
            self.sort()

        return messages
