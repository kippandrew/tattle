import time

from tattle import logging

__all__ = [
    'NodeState',
    'NODE_STATUS_ALIVE',
    'NODE_STATUS_DEAD',
    'NODE_STATUS_SUSPECT'
]

NODE_STATUS_ALIVE = 'alive'
NODE_STATUS_SUSPECT = 'suspect'
NODE_STATUS_DEAD = 'dead'

LOG = logging.get_logger(__name__)


class NodeState(object):
    def __init__(self, name, address, port, protocol=None, sequence=None, status=NODE_STATUS_DEAD):
        self.name = name
        self.address = address
        self.port = port
        self.sequence = sequence
        self.protocol = protocol
        self._status = status
        self._status_change_timestamp = None

    def _get_status(self):
        return self._status

    def _set_status(self, value):
        if value != self._status:
            self._status = value
            self._status_change_timestamp = time.time()

    status = property(_get_status, _set_status)
