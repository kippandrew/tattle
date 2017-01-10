import io
import struct
import asyncio

# from tornado import ioloop
from tornado import iostream

import asyncstream
from tattle import logging
from tattle import messages
from tattle import network
from tattle import schedule
from tattle import state
from tattle import queue
from tattle import utilities

__all__ = [
    'Cluster'
]

LOG = logging.get_logger(__name__)


class Cluster(object):
    def __init__(self, config, loop=None):
        """
        Create a new instance of the Cluster class
        :param config:
        :type config: tattle.config.Configuration
        """
        self.config = config
        self._loop = loop or asyncio.get_event_loop()

        # self._ioloop = ioloop.IOLoop.current()
        # self._leaving = False

        self._ping_seq = utilities.Sequence()
        self._node_seq = utilities.Sequence()

        # init resolver
        # self._resolver = netutil.Resolver()

        # init listeners
        self._udp_listener = self._init_listener_udp()
        self._tcp_listener = self._init_listener_tcp()

        # init queue
        self._queue = self._init_queue()

        # initialize node manager
        self._nodes = self._init_state()

        # init probe
        self._init_probe()

        # init sync
        self._init_sync()

    def _init_listener_udp(self):
        udp_listener = network.UDPListener(self.config.bind_address,
                                           self.config.bind_port,
                                           self._handle_udp_data,
                                           loop=self._loop)
        return udp_listener

    def _init_listener_tcp(self):
        tcp_listener = network.TCPListener(self.config.bind_address,
                                           self.config.bind_port,
                                           self._handle_tcp_connection,
                                           loop=self._loop)
        return tcp_listener

    def _init_queue(self):
        q = queue.MessageQueue()
        return q

    def _init_state(self):
        nodes = state.NodeManager(self._queue)
        return nodes

    def _init_probe(self):
        self._probe_schedule = schedule.ScheduledCallback(self._do_probe, self.config.probe_interval)
        self._probe_index = 0
        self._probe_status = dict()

    def _init_sync(self):
        self._sync_schedule = schedule.ScheduledCallback(self._do_sync, self.config.sync_interval)

    async def _do_probe(self):
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
                node = None
                continue

            break

        if node is None:
            return

        LOG.debug("Probing node: %s", node)

        try:
            await self._probe_node(node)
        except Exception:
            LOG.exception("Error running probe")

    async def _do_sync(self):
        """
        Handle the sync_schedule periodic callback
        """

        def _find_nodes(n):
            return n.name != self.local_node_name and n.status != state.NODE_STATUS_DEAD

        sync_node = next(iter(utilities.select_random_nodes(self.config.sync_nodes, self._nodes, _find_nodes)), None)
        if sync_node is None:
            return

        LOG.debug("Syncing node: %s", sync_node)

        try:
            await self._sync_node(sync_node.address, sync_node.port)
        except Exception:
            LOG.exception("Error running sync")

    async def _probe_node(self, target_node):
        """
        Probe a node
        :param target_node: node to probe
        :type target_node: state.NodeState
        """
        if target_node.name == self.local_node_name:
            return

        # send ping message
        ping = messages.PingMessage(self._ping_seq.increment(),
                                    target=target_node.name,
                                    sender=self.local_node_name,
                                    sender_addr=messages.InternetAddress(self.local_node_address,
                                                                         self.local_node_port))
        LOG.debug("Sending PING (seq=%d) to %s", ping.seq, target_node.name)
        await self._send_udp_message(target_node.address, target_node.port, ping)

        # send indirect ping messages
        await self._probe_node_indirect(target_node)

    async def _probe_node_indirect(self, target_node):
        """
        Probe a node indirectly
        :param target_node: node to probe
        :type target_node: state.NodeState
        """

        def _find_nodes(n):
            return n.name != target_node.name and n.name != self.local_node_name and n.status != state.NODE_STATUS_DEAD

        # send indirect ping to k nodes
        for indirect_node in utilities.select_random_nodes(self.config.probe_indirect_nodes, self._nodes, _find_nodes):
            LOG.debug("Probing node: %s indirectly via %s", target_node.name, indirect_node.name)

            # send ping request  message
            ping_req = messages.PingRequestMessage(self._ping_seq.increment(),
                                                   target=target_node.name,
                                                   target_addr=messages.InternetAddress(target_node.address,
                                                                                        target_node.port),
                                                   sender=self.local_node_name,
                                                   sender_addr=messages.InternetAddress(self.local_node_address,
                                                                                        self.local_node_port))
            LOG.debug("Sending PING-REQ (seq=%d) to %s", ping_req.seq, indirect_node.name)
            await self._send_udp_message(indirect_node.address, indirect_node.port, ping_req)

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

    async def start(self):
        """
        Start cluster on this node.
        """
        await self._udp_listener.start()
        LOG.debug("Started UDPListener. Listening on udp %s:%d",
                  self._udp_listener.local_address,
                  self._udp_listener.local_port)

        await self._tcp_listener.start()
        LOG.debug("Started TCPListener. Listening on tcp %s:%d",
                  self._tcp_listener.local_address,
                  self._tcp_listener.local_port)

        # setup local node
        await self._nodes.set_local_node(self.local_node_name,
                                         self.local_node_address,
                                         self.local_node_port)

        # schedule callbacks
        await self._probe_schedule.start()
        await self._sync_schedule.start()

        LOG.info("Node started")

    async def stop(self):
        """
        Shutdown this node. This will cause this node to appear dead to other nodes.
        """
        self._probe_schedule.stop()
        self._sync_schedule.stop()
        self._tcp_listener.stop()
        self._udp_listener.stop()

        LOG.info("Shut down")

    async def join(self, nodes):
        """
        Join a cluster.
        :param nodes: a list of node names or addresses
        :return:
        """

        # gather list of nodes to sync
        # LOG.trace("Resolving %d nodes", len(nodes))
        # sync_nodes = await [self._resolve_node_address(n) for n in nodes]
        # LOG.trace("Attempting to join %d nodes", len(sync_nodes))

        for node_addr in nodes:
            await self._sync_node(*node_addr)


        # # sync nodes
        # done, pending = await asyncio.wait([self._sync_node(*node_addr) for node_addr in nodes], loop=self._loop)
        # successful_nodes, failed_nodes = utilities.partition(lambda s: s is not None, done)
        # LOG.debug("Successfully synced %d nodes (%d failed)", len(successful_nodes), len(failed_nodes))

    async def leave(self):
        """
        Leave a cluster.
        :return:
        """
        pass

    async def sync(self, node):
        """
        Sync this node with another node.
        :param node:
        :return:
        """
        pass

    async def ping(self, node):
        pass

    @property
    def members(self):
        return sorted(self._nodes, key=lambda n: n.name)

    async def _merge_remote_state(self, remote_state):
        LOG.trace("Merging remote state: %s", remote_state)

        new_state = state.NodeState(remote_state.node,
                                    remote_state.addr.address,
                                    remote_state.addr.port)
        new_state.incarnation = remote_state.incarnation
        new_state.status = remote_state.status
        await self._nodes.merge(new_state)

    async def _send_local_state(self, stream):

        # get local state
        local_state = []
        for node in self._nodes:
            local_state.append(messages.RemoteNodeState(node=node.name,
                                                        node_addr=messages.InternetAddress(node.address, node.port),
                                                        incarnation=node.incarnation,
                                                        status=node.status))

        LOG.trace("Sending local state %s", local_state)

        # send message
        await stream.write_async(self._encode_message(messages.SyncMessage(remote_state=local_state)))

    async def _sync_node(self, node_address, node_port):
        """
        Sync with remote node
        :param node_address:
        :param node_port:
        :return:
        """
        connection = None
        try:

            # connect to node
            LOG.debug("Connecting to node %s:%d", node_address, node_port)
            try:
                connection = await asyncstream.Client(self._loop).connect(node_address, node_port)
            except Exception:
                LOG.exception("Error connecting to node %s:%d", node_address, node_port)
                return

            # send local state
            try:
                await self._send_local_state(connection)
            except IOError:
                LOG.exception("Error sending remote state")
                return

            # read remote state
            try:
                remote_sync_message = self._decode_message((await self._read_tcp_message(connection)))
            except IOError:
                LOG.exception("Error receiving remote state")
                return

            # merge remote state
            for remote_state in remote_sync_message.nodes:
                await self._merge_remote_state(remote_state)

        except:
            LOG.exception("Error syncing node")
            return

        finally:
            if connection is not None:
                connection.close()

        return True

    async def _read_tcp_message(self, stream):
        """
        Read a message from a stream asynchronously
        """
        buf = bytes()
        data = await stream.read_async(messages.HEADER_LENGTH)
        if not data:
            return
        buf += data
        length, _, _ = self._decode_message_header(buf)
        buf += await stream.read_async(length - messages.HEADER_LENGTH)
        return buf

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

    async def _send_udp_message(self, sender_host, sender_port, msg):
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

    async def _handle_tcp_connection(self, stream, client):
        try:
            # read until closed
            while True:

                try:
                    raw = await self._read_tcp_message(stream)
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
                await self._handle_tcp_message(message, client, stream)

        except Exception:
            LOG.exception("Error handling TCP stream")
            return

    # noinspection PyTypeChecker
    async def _handle_tcp_message(self, message, client, stream):
        LOG.trace("Handling TCP message from %s", client)
        try:
            if isinstance(message, messages.SyncMessage):
                await self._handle_sync_message(message, stream)
            else:
                LOG.warn("Unknown message type: %r", message.__class__)
                return
        except Exception:
            LOG.exception("Error dispatching TCP message")
            return

    async def _handle_sync_message(self, message, stream):
        LOG.trace("Handling SYNC message: nodes=%s", message.nodes)

        # merge remote state
        for remote_state in message.nodes:
            await self._merge_remote_state(remote_state)

        # reply with our state
        try:
            await self._send_local_state(stream)
        except IOError:
            LOG.exception("Error sending remote state")
            return

    async def _handle_udp_data(self, data, client_addr):
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
                await self._handle_udp_message(msg, client_addr)

        except Exception:
            LOG.exception("Error handling UDP data")
            return

    # noinspection PyTypeChecker
    async def _handle_udp_message(self, message, client_addr):
        LOG.trace("Handling UDP message from %s:%d", *client_addr)
        try:
            if isinstance(message, messages.AliveMessage):
                await self._handle_alive_message(message, client_addr)
            elif isinstance(message, messages.SuspectMessage):
                await self._handle_suspect_message(message, client_addr)
            elif isinstance(message, messages.DeadMessage):
                await self._handle_dead_message(message, client_addr)
            elif isinstance(message, messages.PingMessage):
                await self._handle_ping_message(message, client_addr)
            elif isinstance(message, messages.PingRequestMessage):
                await self._handle_ping_request_message(message, client_addr)
            elif isinstance(message, messages.AckMessage):
                await self._handle_ack_message(message, client_addr)
            else:
                LOG.warn("Unknown message type: %r", message.__class__)
                return
        except:
            LOG.exception("Error dispatching UDP message")
            return

    async def _handle_alive_message(self, message, client_addr):
        LOG.trace("Handling ALIVE message: node=%s", message.node)

        new_state = state.NodeState(message.node,
                                    message.addr.address,
                                    message.addr.port)

        new_state.incarnation = message.incarnation

        await self._nodes.on_node_alive(new_state)

    async def _handle_suspect_message(self, message, client_addr):
        LOG.trace("Handling SUSPECT message: node=%s", message.node)

    async def _handle_dead_message(self, message, client_addr):
        LOG.trace("Handling DEAD message: node=%s", message.node)

    async def _handle_ping_message(self, msg, client_addr):
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
        await self._send_udp_message(msg.sender_addr.address, msg.sender_addr.port, ack)

    async def _handle_ping_request_message(self, msg, client_addr):
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
        await self._send_udp_message(msg.node_addr.address, msg.node_addr.port, ping)

    async def _handle_ack_message(self, message, client_addr):
        LOG.trace("Handling ACK message: sender=%s", message.sender)
