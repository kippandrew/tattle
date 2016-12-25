import io
import struct

import six

from tornado import gen
from tornado import ioloop
from tornado import iostream
from tornado import netutil

from tattle import logging
from tattle import messages
from tattle import network
from tattle import state
from tattle import queue
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
        self.config = config
        self._ioloop = ioloop.IOLoop.current()
        # self._leaving = False
        self._ping_seq = utils.Sequence()
        self._node_seq = utils.Sequence()

        # init resolver
        self._resolver = netutil.Resolver()

        # init listeners
        self._udp_listener = self._init_listener_udp()
        self._tcp_listener = self._init_listener_tcp()

        # init queue
        self._queue = self._init_queue()

        # initialize node manager
        self._nodes = self._init_state()

        # init probe
        self._init_probe()

    def _init_listener_udp(self):
        udp_listener = network.UDPListener()
        udp_listener.listen(self.config.bind_port, self.config.bind_address)
        udp_listener.start(self._handle_udp_data)
        LOG.debug("Started UDPListener. Listening on udp %s:%d", udp_listener.local_address, udp_listener.local_port)
        return udp_listener

    def _init_listener_tcp(self):
        tcp_listener = network.TCPListener()
        tcp_listener.listen(self.config.bind_port, self.config.bind_address)
        tcp_listener.start(self._handle_tcp_connection)
        LOG.debug("Started TCPListener. Listening on tcp %s:%d", tcp_listener.local_address, tcp_listener.local_port)
        return tcp_listener

    def _init_client_udp(self):
        udp_client = network.UDPClient()
        return udp_client

    def _init_client_tcp(self):
        tcp_client = network.TCPClient()
        return tcp_client

    def _init_queue(self):
        q = queue.MessageQueue()
        return q

    def _init_state(self):
        nodes = state.NodeManager(self._queue)
        return nodes

    def _init_probe(self):
        self._probe_schedule = ioloop.PeriodicCallback(self._do_probe,
                                                       self.config.probe_interval)
        self._probe_index = 0
        self._probe_status = dict()

    def _do_probe(self):
        """
        Handle the probe_schedule periodic callback
        """
        node = None
        checked = 0

        # only check as many nodes as exist
        while checked < len(self._nodes):

            # handle wrap around
            if self._probe_index >= len(self._nodes):
                self._probe_index = 0
                continue

            node = self._nodes[self._probe_index]

            if node == self._nodes.local_node:
                skip = True  # skip local node
            elif node.status == state.NODE_STATUS_DEAD:
                skip = True  # skip dead nodes
            else:
                skip = False

            self._probe_index += 1

            # keep checking
            if skip:
                checked += 1
                continue

            break

        if node is not None:

            def _handle_future(f):
                error = f.exception()
                if error:
                    LOG.error("Error running probe", exc_info=f.exc_info())

            try:
                self._ioloop.add_future(self._probe_node(node), _handle_future)
            except Exception:
                LOG.exception("Error starting probe")

    @gen.coroutine
    def _probe_node(self, node):
        """
        Probe a node
        :param node: target node
        :type node: state.NodeState
        """
        LOG.debug("Probing node: %s", node.name)

        # send ping message
        ping = messages.PingMessage(self._ping_seq.increment(), node.name)

        self._send_message(node, ping)


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
        return self.config.node_name

    @gen.coroutine
    def start(self):
        """
        Start cluster on this node.
        """

        yield self._nodes.set_local_node(self.local_node_name,
                                         self.local_node_address,
                                         self.local_node_port)

        # start probe
        self._probe_schedule.start()

        LOG.info("Node started")

    @gen.coroutine
    def stop(self):
        """
        Shutdown this node. This will cause this node to appear dead to other nodes.
        """
        self._probe_schedule.stop()

        self._tcp_listener.stop()

        self._udp_listener.stop()
        LOG.info("Shut down")

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
        LOG.debug("Attempting to join %d nodes", len(sync_nodes))

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

    @property
    def members(self):
        return self._nodes

    @gen.coroutine
    def _merge_remote_state(self, remote_state):
        LOG.debug("Merging remote state: %s", remote_state)

        new_state = state.NodeState(remote_state.node,
                                    remote_state.address,
                                    remote_state.port,
                                    remote_state.protocol)
        new_state.incarnation = remote_state.incarnation
        new_state.status = remote_state.status
        yield self._nodes.merge(new_state)

    @gen.coroutine
    def _send_local_state(self, stream):

        # get local state
        local_state = []
        for node in self._nodes:
            local_state.append(messages.RemoteNodeState(node.name,
                                                        node.address,
                                                        node.port,
                                                        node.protocol,
                                                        node.incarnation,
                                                        node.status))

        LOG.debug("Sending local state %s", local_state)

        # send message
        yield stream.write(self._encode_message(messages.SyncMessage(nodes=local_state)))

    @gen.coroutine
    def _sync_node(self, node_addr):
        connection = None
        try:

            # connect to node
            try:
                connection = yield network.TCPClient().connect(*node_addr)
            except Exception as ex:
                LOG.error("Error connecting to node %s: %s", node_addr, ex)
                return

            # send local state
            try:
                yield self._send_local_state(connection)
            except IOError:
                LOG.exception("Error sending remote state")
                return

            # read remote state
            try:
                remote_sync_message = self._decode_message((yield self._read_tcp_message(connection)))
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
            connection.close()

        raise gen.Return(node_addr)

    @gen.coroutine
    def _resolve_node_address(self, node_addr):
        LOG.debug("Resolving node address %s", node_addr)

        # if node is a tuple, assume its (addr, port)
        if isinstance(node_addr, tuple):
            raise gen.Return(node_addr)

        # if node_addr is a string assume its host or host:port
        elif isinstance(node_addr, six.string_types):
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
    def _read_tcp_message(self, stream):
        """
        Read a message from a stream asynchronously
        :type stream: tornado.iostream.BaseIOStream
        :rtype: bytes
        """
        buf = bytes()
        LOG.debug("Reading message header %d bytes", messages.HEADER_LENGTH)
        buf += yield stream.read_bytes(messages.HEADER_LENGTH)
        if not buf:
            return
        length, _, _ = self._decode_message_header(buf)
        LOG.debug("Reading message %d bytes", length)
        buf += yield stream.read_bytes(length - messages.HEADER_LENGTH)
        raise gen.Return(buf)

    def _read_udp_message(self, stream):
        """
        Read a message from a stream synchronously
        :type stream: io.BufferedReader
        """
        buf = bytes()
        LOG.debug("Reading message header %d bytes", messages.HEADER_LENGTH)
        buf += stream.read(messages.HEADER_LENGTH)
        if not buf:
            return None
        length, _, _ = self._decode_message_header(buf)
        LOG.debug("Reading message %d bytes", length)
        buf += stream.read(length - messages.HEADER_LENGTH)
        return buf

    def _decode_message_header(self, raw):
        return struct.unpack(messages.HEADER_FORMAT, raw)

    def _decode_message(self, raw):
        try:
            msg = messages.MessageDecoder.decode(raw)
            LOG.debug("Decoded message: %s", msg)
            return msg
        except messages.MessageDecodeError as e:
            LOG.error("Error decoding message: %s", e)
            return

    def _encode_message(self, msg):
        data = messages.MessageEncoder.encode(msg)
        LOG.debug("Encoded message: %s (%d bytes)", msg, len(data))
        return data

    def _queue_message(self, msg):
        self._queue.push(msg.node, self._encode_message(msg))
        LOG.debug("Queued message: %s", msg)

    def _send_message(self, node, msg):
        LOG.debug("Sending %s to %s", msg, node.name)

        connection = network.UDPClient().connect(node.address, node.port)

        # encode message
        buf = bytes()
        buf += self._encode_message(msg)

        # gather gossip messages (already encoded)
        gossip = self._queue.fetch(utils.retransmitLimit(len(self._nodes), 3), 1024 - len(buf))
        LOG.debug("Sending %d gossip messages to %s", len(gossip), node.name)

        for g in gossip:
            buf += g

        # send message
        connection.send(buf)

    @gen.coroutine
    def _handle_tcp_connection(self, stream, client):
        try:
            # read until closed
            while True:

                try:
                    raw = yield self._read_tcp_message(stream)
                except iostream.StreamClosedError:
                    break
                except IOError:
                    LOG.exception("Error reading stream")
                    break

                if raw is None:
                    break

                # decode the message
                message = self._decode_message(raw)
                if message is None:
                    continue

                # dispatch the message
                yield self._handle_tcp_message(message, client, stream)

        except Exception:
            LOG.exception("Error handling TCP stream")
            return

    # noinspection PyTypeChecker
    @gen.coroutine
    def _handle_tcp_message(self, message, client, stream):
        LOG.debug("Handling TCP message from %s", client)
        try:
            if isinstance(message, messages.SyncMessage):
                yield self._handle_sync_message(message, stream)
            else:
                LOG.warn("Unknown message type: %r", message.__class__)
                return
        except Exception:
            LOG.exception("Error dispatching TCP message")
            return

    @gen.coroutine
    def _handle_sync_message(self, message, stream):
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
    def _handle_udp_data(self, data, client):
        try:

            # create a buffered reader
            reader = io.BufferedReader(io.BytesIO(data))

            # read until closed
            while True:

                try:
                    raw = self._read_udp_message(reader)
                except IOError:
                    LOG.exception("Error reading stream")
                    break

                if raw is None:
                    break

                # decode the message
                msg = self._decode_message(raw)
                if msg is None:
                    continue

                # dispatch the message
                yield self._handle_udp_message(msg, client)

        except Exception:
            LOG.exception("Error handling UDP data")
            return

    # noinspection PyTypeChecker
    @gen.coroutine
    def _handle_udp_message(self, message, client):
        LOG.debug("Handling UDP message from %s", client)
        try:
            if isinstance(message, messages.AliveMessage):
                yield self._handle_alive_message(message)
            elif isinstance(message, messages.SuspectMessage):
                yield self._handle_suspect_message(message)
            elif isinstance(message, messages.DeadMessage):
                yield self._handle_dead_message(message)
            elif isinstance(message, messages.PingMessage):
                yield self._handle_ping_message(message)
            elif isinstance(message, messages.PingRequestMessage):
                yield self._handle_ping_request_message(message)
            else:
                LOG.warn("Unknown message type: %r", message.__class__)
                return
        except:
            LOG.exception("Error dispatching UDP message")
            return

    @gen.coroutine
    def _handle_alive_message(self, message):
        LOG.debug("Handling ALIVE message: node=%s", message.node)

        new_state = state.NodeState(message.node,
                                    message.address,
                                    message.port,
                                    message.protocol)

        new_state.incarnation = message.incarnation

        yield self._nodes.on_node_alive(new_state)

    @gen.coroutine
    def _handle_suspect_message(self, message):
        LOG.debug("Handling SUSPECT message: node=%s", message.node)

    @gen.coroutine
    def _handle_dead_message(self, message):
        LOG.debug("Handling DEAD message: node=%s", message.node)

    @gen.coroutine
    def _handle_ping_message(self, message):
        LOG.debug("Handling PING message: node=%s", message.node)

    @gen.coroutine
    def _handle_ping_request_message(self, message):
        LOG.debug("Handling PING-REQ message: node=%s", message.node)
