from tornado import gen
from tornado import ioloop

from tattle import logging
from tattle import network
from tattle import state
from tattle import messages

LOG = logging.get_logger(__name__)


class Gossip(object):
    def __init__(self, queue, nodes, gossip_interval, gossip_nodes):
        self._queue = queue
        self._nodes = nodes
        self._gossip_interval = gossip_interval
        self._gossip_nodes = gossip_nodes
        self._schedule = ioloop.PeriodicCallback(self._do_gossip,
                                                 self._gossip_interval)

        self._udp_client = network.UDPClient()
    def start(self):
        self._schedule.start()
        LOG.debug("Started broadcast scheduler (interval=%dms)", self._gossip_interval)

    def stop(self):
        self._schedule.stop()

    @gen.coroutine
    def _do_gossip(self):

        def _filter_nodes(node):
            if node == self._nodes.local_node:
                return True
            return False

        selected_nodes = state.select_random_nodes(self._gossip_nodes, self._nodes, _filter_nodes)

        pending_messages = self._queue.fetch()

        for node in selected_nodes:

            conn = yield self._udp_client.connect(node.address, node.port)

            for m in pending_messages:

                LOG.debug("Gossip message to %s: %s", node.name, m)
                yield conn.send(messages.MessageEncoder.encode(m))

