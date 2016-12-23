import collections
import time

from tornado import gen
from tornado import locks

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


class NodeManager(collections.Sequence):
    def __init__(self, queue):
        self._nodes = collections.OrderedDict()
        self._nodes_lock = locks.Lock()
        self._local_node_name = None
        self._queue = queue

    def __getitem__(self, name):
        return self._nodes.__getitem__(name)

    def __iter__(self):
        return self._nodes.itervalues()

    def __len__(self):
        return len(self._nodes)

    @property
    def lock(self):
        return self._nodes_lock

    @property
    def local_node(self):
        return self._nodes[self._local_node_name]

    @gen.coroutine
    def set_local_node(self, local_node_name, local_node_address, local_node_port, local_node_sequence):
        """
        Set local node as alive
        """
        self._local_node_name = local_node_name

        # create NodeState for this node
        new_state = NodeState(local_node_name,
                              local_node_address,
                              local_node_port,
                              sequence=local_node_sequence)

        # signal node is alive
        yield self._on_node_alive(new_state)

    @gen.coroutine
    def merge(self, new_state):
        if new_state.status == NODE_STATUS_ALIVE:
            yield self._on_node_alive(new_state)
        elif new_state.status == NODE_STATUS_DEAD:
            # rather then declaring a node a dead immediately, mark it as suspect
            yield self._on_node_suspect(new_state)
        elif new_state.status == NODE_STATUS_SUSPECT:
            yield self._on_node_suspect(new_state)

    @gen.coroutine
    def _on_node_alive(self, new_state, bootstrap=True):

        # acquire node lock
        with (yield self._nodes_lock.acquire()):

            # It is possible that during a leave(), there is already an aliveMsg
            # in-queue to be processed but blocked by the locks above. If we let
            # that aliveMsg process, it'll cause us to re-join the cluster. This
            # ensures that we don't.
            # if self._leaving and new_state.name == self._local_node_name:
            #     return

            # check if this is a new node
            current_state = self._nodes.get(new_state.name)
            if current_state is None:
                LOG.info("Node discovered: %s", new_state.name)

                # add node to the node map also
                self._nodes[new_state.name] = new_state

                # set current state
                current_state = new_state

            # check node address
            if current_state.address != new_state.address or current_state.port != new_state.port:
                LOG.warn("Conflicting node address for %s (current=%s:%d new=%s:%d)",
                         new_state.name, current_state.address, current_state.port, new_state.address, new_state.port)
                return

            is_local_node = new_state.name == self._local_node_name

            # bail if the sequence number is older, and this is not about us
            if not is_local_node and new_state.sequence <= current_state.sequence:
                LOG.debug("Old sequence for node %s (current=%d new=%d)",
                          new_state.name, current_state.sequence, new_state.sequence)
                return

            # bail if the sequence number is strictly less and this is about us
            if is_local_node and new_state.sequence < current_state.sequence:
                LOG.warn("Old sequence for current node %s (current=%d new=%d)",
                         new_state.name, current_state.sequence, new_state.sequence)
                return

            # TODO: clear suspicion timer that may be in effect

            # If this about us we need to refute, otherwise broadcast
            if is_local_node and not bootstrap:
                # TODO: refute
                pass

            else:
                # TODO: queue broadcast message for gossip

                # Update the state and incarnation number
                current_state.sequence = new_state.sequence
                current_state.status = NODE_STATUS_ALIVE

            LOG.info("Node alive: %s", new_state.name)

    @gen.coroutine
    def _on_node_dead(self, node):

        with (yield self._nodes_lock.acquire()):
            pass

    @gen.coroutine
    def _on_node_suspect(self, node):

        with (yield self._nodes_lock.acquire()):
            pass


def kRandomNodes():
    pass
