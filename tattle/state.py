import asyncio
import collections
import random
import time

from tattle import logging
from tattle import messages
from tattle import timer
from tattle import utilities

__all__ = [
    'NodeManager',
    'NODE_STATUS_ALIVE',
    'NODE_STATUS_DEAD',
    'NODE_STATUS_SUSPECT',
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


class NodeManager(collections.Sequence, collections.Mapping):
    def __init__(self, queue, loop=None):
        self._queue = queue
        self._loop = loop or asyncio.get_event_loop()
        self._nodes = list()
        self._nodes_map = dict()
        self._nodes_lock = asyncio.Lock()
        self._suspect_nodes = dict()
        self._suspect_nodes_lock = asyncio.Lock()
        self._local_node_name = None
        self._local_node_seq = utilities.Sequence()

    def __getitem__(self, index):
        if isinstance(index, str):
            return self._nodes_map[index]
        else:
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
        """
        Handle a Node alive notification

        :param name:
        :param incarnation:
        :param host:
        :param port:
        :param bootstrap:
        :return:
        """

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

            # return if conflicting node address
            if current_node.host != host or current_node.port != port:
                LOG.error("Conflicting node address for %s (current=%s:%d new=%s:%d)",
                          name, current_node.host, current_node.port, host, port)
                return

            is_local_node = current_node is self.local_node

            # if this is not about us, return if the incarnation number is older or the same at the current state
            if not is_local_node and incarnation <= current_node.incarnation:
                LOG.trace("%s is older then current state: %d <= %d", name,
                          incarnation, current_node.incarnation)
                return

            # if this is about us, return if the incarnation number is older then the current state
            if is_local_node and incarnation < current_node.incarnation:
                LOG.trace("%s is older then current state: %d < %d", name,
                          incarnation, current_node.incarnation)
                return

            # if the node is suspect, alive message cancels the suspicion
            if current_node.status == NODE_STATUS_SUSPECT:
                await self._cancel_suspect_node(current_node)

            # update the current node status and incarnation number
            current_node.incarnation = incarnation
            current_node.status = NODE_STATUS_ALIVE

            # broadcast alive message
            self._broadcast_alive(current_node)

            LOG.info("Node alive: %s (incarnation %d)", name, incarnation)

    async def on_node_dead(self, name, incarnation):
        """
        Handle a Node dead notification

        :param name:
        :param incarnation:
        :return:
        """

        # acquire node lock
        with (await self._nodes_lock):

            # bail if this is a new node
            current_node = self._nodes_map.get(name)
            if current_node is None:
                LOG.warn("Ignoring unknown node: %s", name)
                return

            # return if node is dead
            if current_node.status == NODE_STATUS_DEAD:
                LOG.trace("Ignoring dead node: %s", name)
                return

            # return if the incarnation number is older then the current state
            if incarnation < current_node.incarnation:
                LOG.trace("%s is older then current state: %d < %d", current_node.name, incarnation,
                          current_node.incarnation)
                return

            # if this is about the local node, we need to refute. otherwise broadcast it
            if current_node is self.local_node:
                LOG.debug("Refuting DEAD message (incarnation=%d)", incarnation)
                self._refute()
                return

            # if the node is suspect, alive message cancels the suspicion
            if current_node.status == NODE_STATUS_SUSPECT:
                await self._cancel_suspect_node(current_node)

            # update the current node status and incarnation number
            current_node.incarnation = incarnation
            current_node.status = NODE_STATUS_DEAD

            # broadcast dead message
            self._broadcast_dead(current_node)

            LOG.error("Node dead: %s (incarnation %d)", name, incarnation)

    async def on_node_suspect(self, name, incarnation):
        """
        Handle a Node suspect notification

        :param name:
        :param incarnation:
        :return:
        """

        # acquire node lock
        with (await self._nodes_lock):

            # bail if this is a new node
            current_node = self._nodes_map.get(name)
            if current_node is None:
                LOG.warn("Ignoring unknown node: %s", name)
                return

            # return if node is dead
            if current_node.status == NODE_STATUS_DEAD:
                LOG.trace("Ignoring DEAD node: %s", name)
                return

            # return if the incarnation number is older then the current state
            if incarnation < current_node.incarnation:
                LOG.trace("%s is older then current state: %d < %d", current_node.name, incarnation,
                          current_node.incarnation)
                return

            # if this is about the local node, we need to refute. otherwise broadcast it
            if current_node is self.local_node:
                LOG.debug("Refuting SUSPECT message (incarnation=%d)", incarnation)
                self._refute()  # don't mark ourselves suspect
                return

            # check if node is currently under suspicion
            if current_node.status == NODE_STATUS_SUSPECT:
                # TODO: confirm suspect node
                pass

            # update the current node status and incarnation number
            current_node.incarnation = incarnation
            current_node.status = NODE_STATUS_SUSPECT

            # create suspect node
            await self._create_suspect_node(current_node)

            # broadcast suspect message
            self._broadcast_suspect(current_node)

            LOG.warn("Node suspect: %s (incarnation %d)", name, incarnation)

    async def _create_suspect_node(self, node):

        async def _handle_suspect_timer():
            LOG.debug("Suspect timer expired for %s", node.name)
            await self.on_node_dead(node.name, node.incarnation)

        with (await self._suspect_nodes_lock):

            # ignore if pending timer exists
            if node.name in self._suspect_nodes:
                return

            # create a Timer
            suspect_timer = timer.Timer(_handle_suspect_timer, 3, self._loop)
            suspect_timer.start()

            # add node to suspects
            self._suspect_nodes[node.name] = SuspectNode(suspect_timer, dict())

            LOG.debug("Created suspect node: %s", node.name)

    async def _cancel_suspect_node(self, node):
        with (await self._suspect_nodes_lock):
            # stop suspect Timer
            suspect = self._suspect_nodes[node.name]
            suspect.timer.stop()

            # remove node from suspects
            del self._suspect_nodes[node.name]

            LOG.debug("Canceled suspect node: %s", node.name)

    def _send_broadcast(self, node, msg):
        self._queue.push(node.name, messages.MessageEncoder.encode(msg))
        LOG.trace("Queued message: %s", msg)

    def _broadcast_alive(self, node):
        LOG.debug("Broadcasting ALIVE message for node: %s (incarnation=%d)", node.name, node.incarnation)
        alive = messages.AliveMessage(node=node.name,
                                      addr=messages.InternetAddress(node.host, node.port),
                                      incarnation=node.incarnation)
        self._send_broadcast(node, alive)

    def _broadcast_suspect(self, node):
        LOG.debug("Broadcasting SUSPECT message for node: %s (incarnation=%d)", node.name, node.incarnation)
        suspect = messages.SuspectMessage(node=node.name, incarnation=node.incarnation, sender=self.local_node.name)
        self._send_broadcast(node, suspect)

    def _broadcast_dead(self, node):
        LOG.debug("Broadcasting DEAD message for node: %s (incarnation=%d)", node.name, node.incarnation)
        dead = messages.DeadMessage(node=node.name, incarnation=node.incarnation, sender=self.local_node.name)
        self._send_broadcast(node, dead)

    def _refute(self):

        # increment local node incarnation
        self.local_node.incarnation = self._local_node_seq.increment()
        LOG.trace("Refuting message (new incarnation %d)", self.local_node.incarnation)

        # broadcast alive message
        self._broadcast_alive(self.local_node)
