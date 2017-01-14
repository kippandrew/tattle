import collections
import random
import time

import asyncio

from tattle import logging
from tattle import messages
from tattle import timer
from tattle import utilities

__all__ = [
    'NODE_STATUS_ALIVE',
    'NODE_STATUS_DEAD',
    'NODE_STATUS_SUSPECT'
]

NODE_STATUS_ALIVE = 'ALIVE'
NODE_STATUS_SUSPECT = 'SUSPECT'
NODE_STATUS_DEAD = 'DEAD'

LOG = logging.get_logger(__name__)


class Node(object):
    def __init__(self, name, host, port, incarnation=0, status=NODE_STATUS_DEAD):
        self.name = name
        self.host = host
        self.port = port
        self.incarnation = incarnation
        self._status = status
        self._status_change_timestamp = None

    def _get_status(self):
        return self._status

    def _set_status(self, value):
        if value != self._status:
            self._status = value
            self._status_change_timestamp = time.time()

    status = property(_get_status, _set_status)

    def __repr__(self):
        return "<Node %s status:%s>" % (self.name, self.status)


SuspectNode = collections.namedtuple('SuspectNode', ['timer', 'confirmations'])


class NodeManager(collections.Sequence):
    def __init__(self, queue):
        self._queue = queue
        self._nodes = list()
        self._nodes_map = dict()
        self._nodes_lock = asyncio.Lock()
        self._suspect_nodes = dict()
        self._suspect_nodes_lock = asyncio.Lock()
        self._local_node_name = None
        self._local_node_seq = utilities.Sequence()

    def __getitem__(self, index):
        return self._nodes[index]

    def __iter__(self):
        return self._nodes.__iter__()

    def __len__(self):
        return len(self._nodes)

    def _swap_random_nodes(self):
        random_index = random.randint(0, len(self._nodes) - 1)
        random_node = self._nodes[random_index]
        last_node = self._nodes[len(self._nodes) - 1]
        self._nodes[random_index] = last_node
        self._nodes[len(self._nodes) - 1] = random_node

    @property
    def local_node(self):
        return self._nodes_map[self._local_node_name]

    async def set_local_node(self, local_node_name, local_node_host, local_node_port):
        """
        Set local node as alive
        """
        assert self._local_node_name is None
        self._local_node_name = local_node_name

        # generate incarnation for the node
        incarnation = self._local_node_seq.increment()

        # signal node is alive
        await self.on_node_alive(local_node_name, incarnation, local_node_host, local_node_port, bootstrap=True)

    async def on_node_alive(self, name, incarnation, host, port, bootstrap=False):

        # acquire node lock
        with (await self._nodes_lock):

            # It is possible that during a leave(), there is already an aliveMsg
            # in-queue to be processed but blocked by the locks above. If we let
            # that aliveMsg process, it'll cause us to re-join the cluster. This
            # ensures that we don't.
            # if self._leaving and new_state.name == self._local_node_name:
            #     return

            # check if this is a new node
            current_node = self._nodes_map.get(name)
            if current_node is None:
                LOG.debug("Node discovered: %s", name)

                # create new Node
                current_node = Node(name, host, port)

                # save current state
                self._nodes_map[name] = current_node
                self._nodes.append(current_node)

                # swap new node with a random node to ensure detection of failed node is bounded
                self._swap_random_nodes()

            LOG.trace("Node alive %s (current incarnation: %d, new incarnation: %d)",
                      current_node.name,
                      current_node.incarnation,
                      incarnation)

            # check node address
            if current_node.host != host or current_node.port != port:
                LOG.warn("Conflicting node address for %s (current=%s:%d new=%s:%d)",
                         name, current_node.host, current_node.port, host, port)
                return

            is_local_node = name == self._local_node_name

            # bail if the incarnation number is older or the same at the current state, and this is not about us
            if not is_local_node and incarnation <= current_node.incarnation:
                LOG.trace("%s is older then current state: %d <= %d", name,
                          incarnation, current_node.incarnation)
                return

            # bail if the incarnation number is older then the current state, and this is about us
            if is_local_node and incarnation < current_node.incarnation:
                LOG.trace("%s is older then current state: %d < %d", name,
                          incarnation, current_node.incarnation)
                return

            # cancel suspect node
            if self._is_suspect_node(current_node):
                await self._cancel_suspect_node(current_node)

            # if this about the local node we need to refute, otherwise broadcast it
            if is_local_node and not bootstrap:
                LOG.trace("Node alive for local node: %s", name)
                if incarnation == current_node.incarnation:
                    return

                # TODO: refute
                raise NotImplementedError()

            else:

                # queue alive message for gossip
                alive = messages.AliveMessage(node=name, addr=messages.InternetAddress(host, port),
                                              incarnation=incarnation)
                self._queue.push(name, messages.MessageEncoder.encode(alive))
                LOG.trace("Queued message: %s", alive)

                # Update the state and incarnation number
                current_node.incarnation = incarnation
                current_node.status = NODE_STATUS_ALIVE

                LOG.info("Node alive: %s (incarnation %d)", name, incarnation)

    async def on_node_dead(self, name, incarnation):

        # acquire node lock
        with (await self._nodes_lock):
            pass

    async def on_node_suspect(self, name, incarnation):

        # acquire node lock
        with (await self._nodes_lock):

            # bail if this is a new node
            current_node = self._nodes_map.get(name)
            if current_node is None:
                LOG.warn("Unknown node: %s", name)
                return

            # bail if the incarnation number is older then the current state, and this is about us
            if incarnation < current_node.incarnation:
                LOG.trace("%s is older then current state: %d < %d", current_node.name, incarnation,
                          current_node.incarnation)
                return

            # # check if node is currently under suspicion
            # if self._is_suspect_node(name):
            #
            #     # if this is a suspicion confirmation is new broadcast it
            #     if self._confirm_suspect_node(name):
            #         # queue suspect message for gossip
            #         msg = messages.SuspectMessage(node=current_node.name, incarnation=incarnation)
            #         self._queue.push(current_node.name, messages.MessageEncoder.encode(msg))
            #         LOG.trace("Queued message: %s", msg)
            #
            #     return

            # if this is about the local node, we need to refute, otherwise broadcast it
            is_local_node = current_node.name == self._local_node_name
            if is_local_node:

                # TODO: refute
                raise NotImplementedError()

            else:

                # queue suspect message for gossip
                msg = messages.SuspectMessage(node=current_node.name, incarnation=current_node.incarnation)
                self._queue.push(current_node.name, messages.MessageEncoder.encode(msg))
                LOG.debug("Queued message: %s", msg)

            # Update the state and incarnation number
            current_node.incarnation = current_node.incarnation
            current_node.status = NODE_STATUS_ALIVE

            self._create_suspect_node(current_node.name)

    async def _create_suspect_node(self, node):

        # acquire suspects lock
        with (await self._suspect_nodes_lock):
            def _handle_suspect_node_timer():
                pass

            # add node to suspects
            self._suspect_nodes[node.name] = SuspectNode(timer.Timer(_handle_suspect_node_timer, 10), dict())

    async def _confirm_suspect_node(self, node):

        # acquire suspects lock
        with (await self._suspect_nodes_lock):
            suspect = self._suspect_nodes[node.name]

    async def _cancel_suspect_node(self, node):

        with (await self._suspect_nodes_lock):
            # stop suspect timer
            suspect = self._suspect_nodes[node.name]
            suspect.timer.stop()

            # remove node from suspects
            del self._suspect_nodes[node.name]

    def _is_suspect_node(self, node):
        return node.name in self._suspect_nodes
