from tattle import logging
from tattle import messages

__all__ = [
    'MessageQueue',
    '_MessageQueueItem'
]

LOG = logging.get_logger(__name__)


class _MessageQueueItem(object):
    def __init__(self, node, message, transmits=0):
        self.node = node
        self.message = message
        self.transmits = transmits


class MessageQueue():
    def __init__(self, max_size=0):
        self._max_size = max_size
        self._queue = list()

    def __len__(self):
        return len(self._queue)

    def push(self, node, message):
        self._push_item(_MessageQueueItem(node, message, 0))

    def _push_item(self, item):

        # remove invalided messages
        self._queue = [i for i in self._queue if i.node != item.node]

        # add message
        self._queue.append(item)  # newer messages inserted at the end

    def _next_item(self):
        if len(self._queue) < 1:
            return None
        return self._queue[len(self._queue) - 1]  # sorted oldest to newest, return newest

    def pop(self, max_transmits, max_bytes=0):
        item = None

        while True:
            item = self._next_item()
            if item is None:
                break

            # don't pop a message if it exceeds max bytes
            if max_bytes > 0 and len(item.message) > max_bytes:
                break

            # don't pop a massage if it exceeds max transmits
            if item.transmits < max_transmits:
                item.transmits += 1
                break
            else:
                self._queue.pop()
                continue

        self.sort()

        if item is not None:
            return item.message
        else:
            return None

    def prune(self):
        pass

    def sort(self):
        # sort queue in place (oldest to newest)
        self._queue.sort(key=lambda i: i.transmits, reverse=True)

    def fetch(self, max_transmits, max_bytes):
        """
        Pop messages up until max bytes
        :param max_transmits:
        :param max_bytes:
        :return:
        """
        messages = []
        bytes_remaining = max_bytes

        while True:
            msg = self.pop(max_transmits, bytes_remaining)
            if msg is None:
                break
            messages.append(msg)
            bytes_remaining -= len(msg)

        return messages
