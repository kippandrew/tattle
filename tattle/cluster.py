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
from tattle import utilities

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
        self._ping_seq = utilities.Sequence()
        self._node_seq = utilities.Sequence()

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
                LOG.exception("Error running probe")

    @gen.coroutine
    def _probe_node(self, target_node):
        """
        Probe a node
        :param target_node: node to probe
        :type target_node: state.NodeState
        """
        if target_node.name == self.local_node_name:
            return

        LOG.debug("Probing node %s", target_node.name)

        # send ping message
        ping = messages.PingMessage(self._ping_seq.increment(),
                                    target=target_node.name,
                                    sender=self.local_node_name,
                                    sender_addr=messages.InternetAddress(self.local_node_address,
                                                                         self.local_node_port))
        LOG.debug("Sending ping (seq=%d) to %s", ping.seq, target_node.name)
        yield self._send_udp_message(target_node.address, target_node.port, ping)

        # send indirect ping messages
        yield self._probe_node_indirect(target_node)

    @gen.coroutine
    def _probe_node_indirect(self, target_node):
        """
        Probe a node indirectly
        :param target_node: node to probe
        :type target_node: state.NodeState
        """

        def _filter_nodes(n):
            return n.name != target_node.name and n.name != self.local_node_name and n.status != state.NODE_STATUS_DEAD

        # send indirect ping to k nodes
        for indirect_node in utilities.select_random_nodes(self.config.indirect_probes, self._nodes, _filter_nodes):
            LOG.debug("Probing node %s indirectly via %s", target_node.name, indirect_node.name)

            # send ping request  message
            ping_req = messages.PingRequestMessage(self._ping_seq.increment(),
                                                   target=target_node.name,
                                                   target_addr=messages.InternetAddress(target_node.address,
                                                                                        target_node.port),
                                                   sender=self.local_node_name,
                                                   sender_addr=messages.InternetAddress(self.local_node_address,
                                                                                        self.local_node_port))
            LOG.debug("Sending indirect-ping (seq=%d) to %s", ping_req.seq, indirect_node.name)
            yield self._send_udp_message(indirect_node.address, indirect_node.port, ping_req)

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
        LOG.trace("Resolving %d nodes", len(nodes))
        sync_nodes = yield [self._resolve_node_address(n) for n in nodes]
        LOG.trace("Attempting to join %d nodes", len(sync_nodes))

        # sync nodes
        results = yield [self._sync_node(node_addr) for node_addr in sync_nodes]
        successful_nodes, failed_nodes = utilities.partition(lambda s: s is not None, results)
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
        return sorted(self._nodes, key=lambda n: n.name)

    @gen.coroutine
    def _merge_remote_state(self, remote_state):
        LOG.trace("Merging remote state: %s", remote_state)

        new_state = state.NodeState(remote_state.node,
                                    remote_state.addr.address,
                                    remote_state.addr.port)
        new_state.incarnation = remote_state.incarnation
        new_state.status = remote_state.status
        yield self._nodes.merge(new_state)

    @gen.coroutine
    def _send_local_state(self, stream):

        # get local state
        local_state = []
        for node in self._nodes:
            local_state.append(messages.RemoteNodeState(node=node.name,
                                                        node_addr=messages.InternetAddress(node.address, node.port),
                                                        incarnation=node.incarnation,
                                                        status=node.status))

        LOG.debug("Sending local state %s", local_state)

        # send message
        yield stream.write(self._encode_message(messages.SyncMessage(remote_state=local_state)))

    @gen.coroutine
    def _sync_node(self, node_addr):
        connection = None
        try:

            # connect to node
            LOG.debug("Connecting to node %s:%d", *node_addr)
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
        :return: buffer read or None
        :rtype: bytes
        """
        buf = bytes()
        buf += yield stream.read_bytes(messages.HEADER_LENGTH)
        if not buf:
            return
        length, _, _ = self._decode_message_header(buf)
        buf += yield stream.read_bytes(length - messages.HEADER_LENGTH)
        raise gen.Return(buf)

    def _read_udp_message(self, stream):
        """
        Read a message from a stream synchronously
        :type stream: io.BufferedReader
        :return: buffer read or None
        :rtype: bytes
        """
        buf = bytes()
        buf += stream.read(messages.HEADER_LENGTH)
        if not buf:
            return None
        length, _, _ = self._decode_message_header(buf)
        buf += stream.read(length - messages.HEADER_LENGTH)
        return buf

    def _decode_message_header(self, raw):
        return struct.unpack(messages.HEADER_FORMAT, raw)

    def _decode_message(self, raw):
        try:
            msg = messages.MessageDecoder.decode(raw)
            return msg
        except messages.MessageDecodeError as e:
            LOG.error("Error decoding message: %s", e)
            return

    def _encode_message(self, msg):
        data = messages.MessageEncoder.encode(msg)
        LOG.trace("Encoded message: %s (%d bytes)", msg, len(data))
        return data

    def _queue_message(self, msg):
        self._queue.push(msg.node, self._encode_message(msg))
        LOG.debug("Queued message: %s", msg)

    @gen.coroutine
    def _send_udp_message(self, sender_host, sender_port, msg):
        LOG.trace("Sending %s to %s:%d", msg, sender_host, sender_port)

        # connection = network.UDPClient().connect(node_address, node_port)

        # encode message
        buf = bytes()
        buf += self._encode_message(msg)

        # max_messages = len(self._nodes)
        max_transmits = utilities.calculate_transmit_limit(len(self._nodes), self.config.retransmit_multiplier)
        max_bytes = 1024 - len(buf)

        # gather gossip messages (already encoded)
        gossip = self._queue.fetch(max_transmits, max_bytes)
        if gossip:
            LOG.trace("Gossip message max-transmits: %d", max_transmits)

        for g in gossip:
            LOG.debug("Piggy-backing message %s to %s:%d", self._decode_message(g), sender_host, sender_port)
            buf += g

        # send message
        self._udp_listener.sendto(buf, sender_host, sender_port)

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
        LOG.trace("Handling TCP message from %s", client)
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
        LOG.trace("Handling SYNC message: nodes=%s", message.nodes)

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
    def _handle_udp_data(self, data, client_addr):
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
                yield self._handle_udp_message(msg, client_addr)

        except Exception:
            LOG.exception("Error handling UDP data")
            return

    # noinspection PyTypeChecker
    @gen.coroutine
    def _handle_udp_message(self, message, client_addr):
        LOG.trace("Handling UDP message from %s:%d", *client_addr)
        try:
            if isinstance(message, messages.AliveMessage):
                yield self._handle_alive_message(message, client_addr)
            elif isinstance(message, messages.SuspectMessage):
                yield self._handle_suspect_message(message, client_addr)
            elif isinstance(message, messages.DeadMessage):
                yield self._handle_dead_message(message, client_addr)
            elif isinstance(message, messages.PingMessage):
                yield self._handle_ping_message(message, client_addr)
            elif isinstance(message, messages.PingRequestMessage):
                yield self._handle_ping_request_message(message, client_addr)
            elif isinstance(message, messages.AckMessage):
                yield self._handle_ack_message(message, client_addr)
            else:
                LOG.warn("Unknown message type: %r", message.__class__)
                return
        except:
            LOG.exception("Error dispatching UDP message")
            return

    @gen.coroutine
    def _handle_alive_message(self, message, client_addr):
        LOG.trace("Handling ALIVE message: node=%s", message.node)

        new_state = state.NodeState(message.node,
                                    message.addr.address,
                                    message.addr.port)

        new_state.incarnation = message.incarnation

        yield self._nodes.on_node_alive(new_state)

    @gen.coroutine
    def _handle_suspect_message(self, message, client_addr):
        LOG.trace("Handling SUSPECT message: node=%s", message.node)

    @gen.coroutine
    def _handle_dead_message(self, message, client_addr):
        LOG.trace("Handling DEAD message: node=%s", message.node)

    @gen.coroutine
    def _handle_ping_message(self, msg, client_addr):
        """
        Handle a PingRequestMessage
        :type msg: messages.PingMessage
        :return None
        """
        LOG.trace("Handling PING message: target=%s", msg.node)

        # ensure target node is local node
        if msg.node != self.local_node_name:
            LOG.warn("Received ping message %d from %s for non-local node.", msg.seq, msg.sender_addr)
            return

        # send ack message
        ack = messages.AckMessage(msg.seq, sender=self.local_node_name)
        LOG.debug("Sending ACK (%d) to %s", msg.seq, msg.sender_addr)
        yield self._send_udp_message(msg.sender_addr.address, msg.sender_addr.port, ack)

    @gen.coroutine
    def _handle_ping_request_message(self, msg, client_addr):
        """
        Handle a PingRequestMessage
        :type msg: messages.PingRequestMessage
        :return None
        """
        LOG.trace("Handling PING-REQ message: target=%s", msg.node)

        ping = messages.PingMessage(msg.seq,
                                    target=msg.node,
                                    sender=msg.sender,
                                    sender_addr=msg.sender_addr)
        LOG.debug("Sending PING (%d) to %s", msg.seq, msg.node_addr)
        yield self._send_udp_message(msg.node_addr.address, msg.node_addr.port, ping)

    @gen.coroutine
    def _handle_ack_message(self, message, client_addr):
        LOG.trace("Handling ACK message: sender=%s", message.sender)
