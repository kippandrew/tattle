import collections
import struct

from tornado import ioloop
from tornado import gen
from tornado import iostream
from tornado import locks
from tornado import netutil

from tattle import broadcast
from tattle import logging
from tattle import messages
from tattle import network
from tattle import state
from tattle import utils

__all__ = [
    'Cluster'
]

LOG = logging.get_logger(__name__)


class Cluster(object):
    def __init__(self, config):
        """
        Create a new instance of the Cluster class
        :param config:
        :type config: tattle.config.Configuration
        """
        self.nodes = collections.OrderedDict()
        self._nodes_lock = locks.Lock()
        self.config = config
        self._ping_seq = utils.Sequence()
        self._node_seq = utils.Sequence()
        self._leaving = False

        # init resolver
        self._resolver = netutil.Resolver()

        # init listeners
        self._udp_listener = self._init_listener_udp()
        self._tcp_listener = self._init_listener_tcp()

        # init clients
        self._udp_client = self._init_client_udp()
        self._tcp_client = self._init_client_tcp()

        # setup scheduled callbacks
        self._probe_scheduler = ioloop.PeriodicCallback(self._do_probe,
                                                        self.config.probe_interval)

        self._gossip_scheduler = ioloop.PeriodicCallback(self._do_gossip,
                                                         self.config.gossip_interval)

        LOG.debug("Initialized Cluster")

    def _init_listener_udp(self):
        udp_listener = network.UDPListener()
        udp_listener.listen(self.config.bind_port, self.config.bind_address)
        udp_listener.start(self._handle_udp_data)
        LOG.debug("Started UDPListener. Listening on udp %s:%d", udp_listener.local_address, udp_listener.local_port)
        return udp_listener

    def _init_listener_tcp(self):
        tcp_listener = network.TCPListener()
        tcp_listener.listen(self.config.bind_port, self.config.bind_address)
        tcp_listener.start(self._handle_tcp_stream)
        LOG.debug("Started TCPListener. Listening on tcp %s:%d", tcp_listener.local_address, tcp_listener.local_port)
        return tcp_listener

    def _init_client_udp(self):
        udp_client = network.UDPClient()
        return udp_client

    def _init_client_tcp(self):
        tcp_client = network.TCPClient()
        return tcp_client

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

        yield self._set_local_node_alive()

        self._start_schedulers()

    @gen.coroutine
    def join(self, nodes):
        """
        Join a cluster.
        :param nodes: a list of node names or addresses
        :return:
        """

        # gather list of nodes to sync
        LOG.debug("Resolving %d nodes", len(nodes))
        sync_nodes = yield [self._resolve_node_address(n) for n in nodes]
        LOG.debug("Attempting to sync %d nodes", len(sync_nodes))

        # sync nodes
        results = yield [self._sync_node(node_addr) for node_addr in sync_nodes]
        successful_nodes, failed_nodes = utils.partition(lambda s: s is not None, results)
        LOG.debug("Successfully synced %d nodes (%d failed)", len(successful_nodes), len(failed_nodes))

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
        return self.nodes.viewvalues()

    @property
    def local_node_address(self):
        if self.config.node_address is not None:
            return self.config.node_address
        else:
            if self.config.bind_address is not None:
                if self.config.bind_address == '0.0.0.0':
                    # TODO: enumerate interfaces to get ip address
                    raise NotImplementedError()
                else:
                    return self.config.bind_address
            else:
                return self._udp_listener.local_address

    @property
    def local_node_port(self):
        if self.config.node_port is not None:
            return self.config.node_port
        else:
            if self.config.bind_port is not None:
                return self.config.bind_port
            else:
                return self._udp_listener.local_port

    @property
    def local_node_name(self):
        return "{name}:{port}".format(name=self.config.node_name, port=self.local_node_port)

    @property
    def local_node(self):
        return self.nodes[self.local_node_name]

    @gen.coroutine
    def _merge_remote_state(self, remote_state):
        LOG.debug("Merging remote state: %s", remote_state)

        new_state = state.NodeState(remote_state.name,
                                    remote_state.addr,
                                    remote_state.port,
                                    protocol=remote_state.protocol,
                                    sequence=remote_state.sequence,
                                    status=remote_state.status)

        yield self._merge_node(new_state)

    @gen.coroutine
    def _send_local_state(self, stream):
        local_state = []

        # FIXME: is lock necessary?
        with (yield self._nodes_lock.acquire()):
            # copy local state
            for node in self.nodes.values():
                local_state.append(messages.RemoteNodeState(node.name,
                                                            node.address,
                                                            node.port,
                                                            node.protocol,
                                                            node.sequence,
                                                            node.status))

        LOG.debug("Sending local state %s", local_state)

        # send message
        yield stream.write(self._encode_message(messages.SyncMessage(nodes=local_state)))

    @gen.coroutine
    def _sync_node(self, node_addr):
        stream = None
        try:

            # connect to node
            try:
                stream = yield network.TCPClient().connect(*node_addr)
            except Exception as ex:
                LOG.error("Error connecting to node %s: %s", node_addr, ex)
                return

            # send local state
            try:
                yield self._send_local_state(stream)
            except IOError:
                LOG.exception("Error sending remote state")
                return

            # read remote state
            try:
                remote_sync_message = yield self._read_and_decode_message(stream)
            except IOError:
                LOG.exception("Error receiving remote state")
                return

            # merge remote state
            for remote_state in remote_sync_message.nodes:
                yield self._merge_remote_state(remote_state)

        except:
            LOG.exception("Error syncing node")
            return

        finally:
            stream.close()

        raise gen.Return(node_addr)

    def _do_probe(self):
        pass

    def _probe_node(self, node_addr):
        pass

    def _do_gossip(self):
        pass

    @gen.coroutine
    def _resolve_node_address(self, node_addr):
        LOG.debug("Resolving node address %s", node_addr)

        # if node is a tuple, assume its (addr, port)
        if isinstance(node_addr, tuple):
            raise gen.Return(node_addr)

        # if node_addr is a string assume its host or host:port
        elif isinstance(node_addr, (str, unicode)):
            # get port from node
            if ':' in node_addr:
                host, _, port = node_addr.split(':')
            else:
                # use default port
                host, port = node_addr, self.config.node_port

            result = yield self._resolver.resolve(host, port)
            raise gen.Return(result)
        else:
            raise ValueError(node_addr, "Unknown node address format: %s", node_addr)

    @gen.coroutine
    def _read_message(self, stream):
        data = bytes()
        LOG.debug("Reading message header %d bytes", messages.HEADER_LENGTH)
        data += yield stream.read_bytes(messages.HEADER_LENGTH)

        length, flags, crc = struct.unpack(messages.HEADER_FORMAT, data[0:messages.HEADER_LENGTH])

        LOG.debug("Reading message %d bytes", length)
        data += yield stream.read_bytes(length - messages.HEADER_LENGTH)
        raise gen.Return(data)

    @gen.coroutine
    def _read_and_decode_message(self, stream):
        data = yield self._read_message(stream)
        message = self._decode_message(data)
        raise gen.Return(message)

    def _decode_message(self, data):
        try:
            msg = messages.MessageDecoder.decode(data)
            LOG.debug("Decoded message: %s", msg)
            return msg
        except messages.MessageDecodeError as e:
            LOG.error("Error decoding message: %s", e)
            return

    def _encode_message(self, msg):
        data = messages.MessageEncoder.encode(msg)
        LOG.debug("Encoded message: %s (%d bytes)", msg, len(data))
        return data

    @gen.coroutine
    def _handle_tcp_stream(self, stream, addr):
        try:

            # read until closed
            while True:

                try:
                    # read a message
                    data = yield self._read_message(stream)
                except iostream.StreamClosedError:
                    break
                except IOError:
                    LOG.exception("Error reading stream")
                    break

                # decode the message
                message = self._decode_message(data)
                if message is None:
                    continue

                # dispatch the message
                yield self._handle_tcp_message(message, addr, stream)

        except Exception:
            LOG.exception("Error handling TCP stream")
            return

    # noinspection PyTypeChecker
    @gen.coroutine
    def _handle_tcp_message(self, message, addr, stream):
        try:
            if isinstance(message, messages.SyncMessage):
                yield self._handle_sync_message(message, addr, stream)
            else:
                LOG.warn("Unknown message type: %r", message.__class__)
                return
        except Exception:
            LOG.exception("Error dispatching TCP message")
            return

    @gen.coroutine
    def _handle_sync_message(self, message, addr, stream):
        LOG.debug("Handling SYNC message: nodes=%s", message.nodes)

        # merge remote state
        for remote_state in message.nodes:
            yield self._merge_remote_state(remote_state)

        # reply with our state
        try:
            yield self._send_local_state(stream)
        except IOError:
            LOG.exception("Error sending remote state")
            return

    @gen.coroutine
    def _handle_udp_data(self, data, addr):

        # decode the message
        message = self._decode_message(data)
        if message is None:
            return

        # dispatch the message
        yield self._handle_udp_message(message, addr)

    # noinspection PyTypeChecker
    @gen.coroutine
    def _handle_udp_message(self, message, addr):
        try:
            if isinstance(message, messages.AliveMessage):
                yield self._handle_alive_message(message, addr)
            elif isinstance(message, messages.SuspectMessage):
                yield self._handle_suspect_message(message, addr)
            elif isinstance(message, messages.DeadMessage):
                yield self._handle_dead_message(message, addr)
            else:
                LOG.warn("Unknown message type: %r", message.__class__)
                return
        except:
            LOG.exception("Error handling UDP message")
            return

    @gen.coroutine
    def _handle_alive_message(self, message, addr):
        LOG.debug("Handling ALIVE message: node=%s", message.node)

    @gen.coroutine
    def _handle_suspect_message(self, message, addr):
        LOG.debug("Handling SUSPECT message: node=%s", message.node)

    @gen.coroutine
    def _handle_dead_message(self, message, addr):
        LOG.debug("Handling DEAD message: node=%s", message.node)

    @gen.coroutine
    def _merge_node(self, new_state):
        if new_state.status == state.NODE_STATUS_ALIVE:
            yield self._on_node_alive(new_state)
        elif new_state.status == state.NODE_STATUS_DEAD:
            # rather then declaring a node a dead immediately, mark it as suspect
            yield self._on_node_suspect(new_state)
        elif new_state.status == state.NODE_STATUS_SUSPECT:
            yield self._on_node_suspect(new_state)

    @gen.coroutine
    def _on_node_alive(self, new_state, bootstrap=True):

        # acquire node lock
        with (yield self._nodes_lock.acquire()):

            # It is possible that during a leave(), there is already an aliveMsg
            # in-queue to be processed but blocked by the locks above. If we let
            # that aliveMsg process, it'll cause us to re-join the cluster. This
            # ensures that we don't.
            if self._leaving and new_state.name == self.local_node_name:
                return

            # check if this is a new node
            current_state = self.nodes.get(new_state.name)
            if current_state is None:
                LOG.debug("New node discovered: %s", new_state.name)

                # add node to the node map also
                self.nodes[new_state.name] = new_state

                # set current state
                current_state = new_state

            # check node address
            if current_state.address != new_state.address or current_state.port != new_state.port:
                LOG.warn("Conflicting node address for %s (current=%s:%d new=%s:%d)",
                         new_state.name, current_state.address, current_state.port, new_state.address, new_state.port)
                return

            is_local_node = new_state.name == self.local_node_name

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

            # TODO: clear out any suspicion timer that may be in effect.

            # If this about us we need to refute, otherwise broadcast
            if is_local_node and not bootstrap:
                # TODO: refute
                pass

            else:
                # # queue broadcast message
                # self._broadcast_queue.push(message.AliveMessage.create(new_state.name,
                #                                                        new_state.incarnation,
                #                                                        new_state.address,
                #                                                        new_state.port))

                # Update the state and incarnation number
                current_state.sequence = new_state.sequence
                current_state.status = state.NODE_STATUS_ALIVE

            # TODO: metrics
            LOG.debug("Node is alive: %s", new_state.name)

    @gen.coroutine
    def _on_node_dead(self, node):

        with (yield self._nodes_lock.acquire()):
            pass

    @gen.coroutine
    def _on_node_suspect(self, node):

        with (yield self._nodes_lock.acquire()):
            pass

    @gen.coroutine
    def _set_local_node_alive(self):
        """
        Set this node as alive
        """

        # create NodeState for this node
        new_state = state.NodeState(self.local_node_name,
                                    self.local_node_address,
                                    self.local_node_port,
                                    sequence=self._node_seq.increment())

        # node is alive
        yield self._on_node_alive(new_state)
