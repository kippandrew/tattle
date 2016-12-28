import collections
import time

from tornado import gen
from tornado import locks

from tattle import logging
from tattle import messages
from tattle import utilities

__all__ = [
    'NodeState',
    'NODE_STATUS_ALIVE',
    'NODE_STATUS_DEAD',
    'NODE_STATUS_SUSPECT'
]

NODE_STATUS_ALIVE = 'ALIVE'
NODE_STATUS_SUSPECT = 'SUSPECT'
NODE_STATUS_DEAD = 'DEAD'

LOG = logging.get_logger(__name__)


class NodeState(object):
    def __init__(self, name, address, port):
        self.name = name
        self.address = address
        self.port = port
        self.incarnation = 0
        self.protocol_version = None
        self.protocol_max = None
        self.protocol_min = None
        self._status = NODE_STATUS_DEAD
        self._status_change_timestamp = None

    def _get_status(self):
        return self._status

    def _set_status(self, value):
        if value != self._status:
            self._status = value
            self._status_change_timestamp = time.time()

    status = property(_get_status, _set_status)

    def __repr__(self):
        return "<NodeState %s status:%s>" % (self.name, self.status)


class NodeManager(collections.Sequence):
    def __init__(self, queue):
        self._nodes = list()
        self._nodes_map = dict()
        self._nodes_lock = locks.Lock()
        self._local_node_name = None
        self._local_node_seq = utilities.Sequence()
        self._queue = queue

    def __getitem__(self, index):
        return self._nodes[index]

    def __iter__(self):
        return self._nodes.__iter__()

    def __len__(self):
        return len(self._nodes)

    @property
    def local_node(self):
        return self._nodes_map[self._local_node_name]

    @gen.coroutine
    def set_local_node(self, local_node_name, local_node_address, local_node_port, local_node_protocol=0):
        """
        Set local node as alive
        """
        self._local_node_name = local_node_name

        # create NodeState for this node
        new_state = NodeState(local_node_name,
                              local_node_address,
                              local_node_port)

        # set incarnation for the node
        new_state.incarnation = self._local_node_seq.increment()

        # signal node is alive
        yield self.on_node_alive(new_state, bootstrap=True)

    @gen.coroutine
    def merge(self, new_state):
        if new_state.status == NODE_STATUS_ALIVE:
            yield self.on_node_alive(new_state)
        elif new_state.status == NODE_STATUS_DEAD or new_state.status == NODE_STATUS_SUSPECT:
            # rather then declaring a node a dead immediately, mark it as suspect
            yield self.on_node_suspect(new_state)
        else:
            LOG.warn("Unknown node status: %s", new_state.status)
            return

    @gen.coroutine
    def on_node_alive(self, new_state, bootstrap=False):

        # acquire node lock
        with (yield self._nodes_lock.acquire()):

            # It is possible that during a leave(), there is already an aliveMsg
            # in-queue to be processed but blocked by the locks above. If we let
            # that aliveMsg process, it'll cause us to re-join the cluster. This
            # ensures that we don't.
            # if self._leaving and new_state.name == self._local_node_name:
            #     return

            # check if this is a new node
            current_state = self._nodes_map.get(new_state.name)
            if current_state is None:
                LOG.debug("Node discovered: %s", new_state.name)

                # copy new state to current state
                current_state = NodeState(new_state.name,
                                          new_state.address,
                                          new_state.port)

                # save current state
                self._nodes_map[new_state.name] = current_state
                self._nodes.append(current_state)

                # swap new node with a random node to ensure detection of failed node is bounded
                self._nodes = utilities.swap_random_nodes(self._nodes)

            LOG.trace("Node alive %s (current incarnation: %d, new incarnation: %d)",
                      current_state.name,
                      current_state.incarnation,
                      new_state.incarnation)

            assert current_state is not new_state  # make sure we've got a copy

            # check node address
            if current_state.address != new_state.address or current_state.port != new_state.port:
                LOG.warn("Conflicting node address for %s (current=%s:%d new=%s:%d)",
                         new_state.name, current_state.address, current_state.port, new_state.address, new_state.port)
                return

            is_local_node = new_state.name == self._local_node_name

            # bail if the incarnation number is older or the same at the current state, and this is not about us
            if not is_local_node and new_state.incarnation <= current_state.incarnation:
                LOG.trace("%s is older then current state: %d <= %d", new_state.name,
                          new_state.incarnation, current_state.incarnation)
                return

            # bail if the incarnation number is older then the current state, and this is about us
            if is_local_node and new_state.incarnation < current_state.incarnation:
                LOG.trace("%s is older then current state: %d < %d", new_state.name,
                          new_state.incarnation, current_state.incarnation)
                return

            # TODO: clear suspicion timer that may be in effect

            # If this about us we need to refute, otherwise broadcast
            if is_local_node and not bootstrap:
                LOG.trace("Node alive for local-node: %s", new_state.name)

                # TODO: check version
                if new_state.incarnation == current_state.incarnation:
                    return

                # TODO: refute
                raise NotImplementedError()

            else:

                # queue alive message for gossip
                alive_msg = messages.AliveMessage(node=new_state.name,
                                                  node_addr=messages.InternetAddress(new_state.address,
                                                                                     new_state.port),
                                                  incarnation=new_state.incarnation)
                self._queue.push(new_state.name, messages.MessageEncoder.encode(alive_msg))
                LOG.debug("Queued message: %s", alive_msg)

                # Update the state and incarnation number
                current_state.incarnation = new_state.incarnation
                current_state.status = NODE_STATUS_ALIVE

                LOG.info("Node alive: %s (incarnation %d)", new_state.name, new_state.incarnation)

    @gen.coroutine
    def on_node_dead(self, node):

        with (yield self._nodes_lock.acquire()):
            pass

    @gen.coroutine
    def on_node_suspect(self, node):

        with (yield self._nodes_lock.acquire()):
            pass
