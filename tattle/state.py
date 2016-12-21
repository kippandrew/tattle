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
    def __init__(self, name, address, port, protocol=None, protocol_min=None, protocol_max=None, incarnation=None):
        self.name = name
        self.address = address
        self.port = port
        self.protocol_version = protocol
        self.protocol_version_min = protocol_min
        self.protocol_version_max = protocol_max
        self.incarnation = incarnation
        self.status = NODE_STATUS_DEAD
        self.status_change_timestamp = None
