import time

from tornado import ioloop
from tornado import gen

from tattle import broadcast
from tattle import logging
from tattle import message
from tattle import network
from tattle import state
from tattle import utils

__all__ = [
    'Cluster'
]

LOG = logging.get_logger(__name__)


class Cluster(object):
    def __init__(self, config, message_listener=None, message_broadcaster=None, custom_ioloop=None):
        """
        Create a new instance of the Cluster class
        :param config:
        :param message_listener:
        :param message_broadcaster:
        :param custom_ioloop:
        """
        self.nodes = list()
        self.nodes_map = dict()
        self.config = config
        self._sequence = utils.Sequence()
        self._incarnation = utils.Sequence()
        self._leaving = False
        self._io_loop = custom_ioloop or ioloop.IOLoop.current()

        # init message listener
        self._listener = message_listener or self._init_message_listener()
        self._listener.listen(self._handle_message)

        # init message broadcaster
        self._broadcast_queue = broadcast.Queue()
        self._broadcast = message_broadcaster or self._init_message_broadcaster()

        # setup scheduled callbacks
        self._probe_scheduler = ioloop.PeriodicCallback(self._do_probe,
                                                        self.config.probe_interval,
                                                        io_loop=self._io_loop)

        self._gossip_scheduler = ioloop.PeriodicCallback(self._do_gossip,
                                                         self.config.gossip_interval,
                                                         io_loop=self._io_loop)

        LOG.debug("Initialized Cluster")

    def _init_message_listener(self):

        # init network listeners
        self._udp_listener = network.UDPListener((self.config.bind_address, self.config.bind_port), self._io_loop)
        LOG.debug("Started UDPListener. Listening on udp %s:%d", self.config.bind_address, self.config.bind_port)

        self._tcp_listener = network.TCPListener((self.config.bind_address, self.config.bind_port), self._io_loop)
        LOG.debug("Started TCPListener. Listening on tcp %s:%d", self.config.bind_address, self.config.bind_port)

        # init message listener
        listener = network.MessageListener(self._udp_listener,
                                           self._tcp_listener,
                                           self._io_loop)
        return listener

    def _init_message_broadcaster(self):
        self._broadcast = broadcast.MessageBroadcaster()

    def _start_schedulers(self):
        self._probe_scheduler.start()
        LOG.debug("Started probe scheduler (interval=%dms)", self.config.probe_interval)

        self._gossip_scheduler.start()
        LOG.debug("Started gossip scheduler (interval=%dms)", self.config.gossip_interval)

    def _stop_schedulers(self):
        LOG.debug("Stopped probe scheduler")
        self._probe_scheduler.stop()

        LOG.debug("Stopped gossip scheduler")
        self._gossip_scheduler.stop()

    @gen.coroutine
    def run(self):
        """
        Create a cluster on this node.
        :return:
        """

        self._set_alive()

        self._start_schedulers()

    @gen.coroutine
    def join(self, *nodes):
        """
        Join a cluster.
        :param nodes:
        :return:
        """
        pass

    @gen.coroutine
    def leave(self):
        """
        Leave a cluster.
        :return:
        """
        pass

    @gen.coroutine
    def sync(self, node):
        """
        Sync this node with another node.
        :param node:
        :return:
        """
        pass

    @gen.coroutine
    def ping(self, node):
        pass

    @gen.coroutine
    def shutdown(self):
        """
        Shutdown this node. This will cause this node to appear dead to other nodes.
        :return: None
        """
        self._stop_schedulers()

        LOG.info("Shut down")

    @property
    def members(self):
        return self.nodes.__iter__()

    def _do_probe(self):
        pass

    def _probe_node(self, node):
        pass

    def _sync_node(self, node):
        pass

    def _do_gossip(self):
        pass

    def _handle_message(self):
        pass

    def _handle_alive_message(self):
        pass

    def _handle_dead_message(self):
        pass

    def _handle_suspect_message(self):
        pass

    def _on_node_alive(self, new_state, bootstrap=True):

        # It is possible that during a leave(), there is already an aliveMsg
        # in-queue to be processed but blocked by the locks above. If we let
        # that aliveMsg process, it'll cause us to re-join the cluster. This
        # ensures that we don't.
        if self._leaving and new_state.name == self.config.node_name:
            return

        # check if this is a new node
        current_state = self.nodes_map.get(new_state.name)
        if current_state is None:
            # if this is a new node add it to the node list
            self.nodes.append(new_state)

            # add node to the node map also
            self.nodes_map[new_state.name] = new_state

            # set current state
            current_state = new_state

        # check node address
        if current_state.address != new_state.address or current_state.port != new_state.port:
            LOG.warn("Conflicting node address for %s (current=%s:%d new=%s:%d)",
                     new_state.name, current_state.address, current_state.port, new_state.address, new_state.port)
            return

        is_local_node = new_state.name == self.config.node_name

        # bail if the incarnation number is older, and this is not about us
        if not is_local_node and new_state.incarnation <= current_state.incarnation:
            LOG.warn("Old incarnation for node %s (current=%d new=%d)",
                     new_state.name, current_state.incarnation, new_state.incarnation)
            return

        # bail if the incarnation number is strictly less and this is about us
        if is_local_node and new_state.incarnation < current_state.incarnation:
            LOG.warn("Old incarnation for current node %s (current=%d new=%d)",
                     new_state.name, current_state.incarnation, new_state.incarnation)
            return

        # TODO: clear out any suspicion timer that may be in effect.

        # If this about us we need to refute, otherwise broadcast
        if is_local_node and not bootstrap:
            # TODO: refute
            pass

        else:
            # queue broadcast message
            self._broadcast_queue.push(message.AliveMessage.create(new_state.name,
                                                                   new_state.incarnation,
                                                                   new_state.address,
                                                                   new_state.port))

            # Update the state and incarnation number
            current_state.incarnation = new_state.incarnation
            if current_state != state.NODE_STATUS_ALIVE:
                current_state.status = state.NODE_STATUS_ALIVE
                current_state.status_change_timestamp = time.time()

        # TODO: metrics
        LOG.debug("Node is alive: %s", new_state.name)

    def _on_node_dead(self, node):
        pass

    def _on_node_suspect(self, node):
        pass

    def _set_alive(self):
        """
        Set this node as alive
        """

        # get this node's name
        node_name = self.config.node_name

        # get this node's address
        if self.config.node_address is not None:
            node_address = self.config.node_address
        else:
            if self.config.bind_address == '0.0.0.0':
                raise NotImplementedError()
            node_address = self._udp_listener.local_address[0]

        # get this node's port
        if self.config.node_port is not None:
            node_port = self.config.node_port
        else:
            node_port = self._udp_listener.local_address[1]

        # create NodeState for this node
        current_node = state.NodeState(node_name,
                                       node_address,
                                       node_port,
                                       incarnation=self._incarnation.increment())

        # node is alive
        self._on_node_alive(current_node)
