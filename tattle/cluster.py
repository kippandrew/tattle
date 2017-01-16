import asyncio
import asyncstream
import collections
import io
import struct
import time

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

ProbeStatus = collections.namedtuple('ProbeStatus', ['future'])


class Cluster(object):
    def __init__(self, config, loop=None):
        """
        Create a new instance of the Cluster class
        :param config:
        :type config: tattle.config.Configuration
        """
        self.config = config

        self._loop = loop or asyncio.get_event_loop()

        self._leaving = False

        self._ping_seq = utilities.Sequence()
        self._node_seq = utilities.Sequence()

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
        return queue.MessageQueue()

    def _init_state(self):
        return state.NodeManager(self._queue, loop=self._loop)

    def _init_probe(self):
        self._probe_schedule = schedule.ScheduledCallback(self._do_probe, self.config.probe_interval, loop=self._loop)
        self._probe_index = 0
        self._probe_status = dict()

    def _init_sync(self):
        self._sync_schedule = schedule.ScheduledCallback(self._do_sync, self.config.sync_interval, loop=self._loop)

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
        await self._probe_schedule.stop()
        await self._sync_schedule.stop()
        await self._tcp_listener.stop()
        await self._udp_listener.stop()

        LOG.info("Node stopped")

    async def join(self, *other_nodes):
        """
        Join a cluster.
        :return:
        """

        # gather list of nodes to sync
        LOG.trace("Attempting to join nodes: %s", other_nodes)

        # sync nodes
        tasks = []
        for node_host, node_port in other_nodes:
            tasks.append(self._sync_node(node_host, node_port))

        # wait for syncs to complete
        results = await asyncio.gather(*tasks, loop=self._loop, return_exceptions=True)

        successful_nodes, failed_nodes = utilities.partition(lambda r: r is True, results)
        LOG.debug("Successfully synced %d nodes (%d failed)", len(successful_nodes), len(failed_nodes))

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
            # send ping messages
            self._loop.create_task(self._probe_node(node))
        except Exception:
            LOG.exception("Error running probe")

        try:
            # send indirect ping messages
            self._loop.create_task(self._probe_node_indirect(node, self.config.probe_indirect_nodes))
        except Exception:
            LOG.exception("Error running indirect probe")

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
            await self._sync_node(sync_node.host, sync_node.port)
        except Exception:
            LOG.exception("Error running sync")

    async def _probe_node(self, target_node):
        """
        Probe a node
        :param target_node: node to probe
        :type target_node: state.Node
        """
        if target_node.name == self.local_node_name:
            return

        # get a sequence number for the ping
        next_seq = self._ping_seq.increment()

        # start waiting for probe result
        waiter = self._wait_for_probe(next_seq)

        # send ping message
        ping = messages.PingMessage(next_seq,
                                    target=target_node.name,
                                    sender=self.local_node_name,
                                    sender_addr=messages.InternetAddress(self.local_node_address,
                                                                         self.local_node_port))
        LOG.debug("Sending PING (seq=%d) to %s", ping.seq, target_node.name)
        await self._send_udp_message(target_node.host, target_node.port, ping)

        # wait for probe result or timeout
        try:
            result = await waiter
        except asyncio.TimeoutError:
            await self._handle_probe_timeout(target_node)
        else:
            await self._handle_probe_result(target_node, result)

    async def _handle_probe_result(self, node, result):
        if result:
            LOG.debug("Probe successful for node: %s result=%s", node.name, result)

            # if node is suspect notify, then node is alive
            if node.status == state.NODE_STATUS_SUSPECT:
                LOG.warn("Suspect node is alive: %s", node.name)

                await self._nodes.on_node_alive(node.name, node.incarnation, node.host, node.port)

        else:
            LOG.debug("Probe failed for node: %s result=%s", node.name, result)
            raise NotImplementedError()

    async def _handle_probe_timeout(self, node):
        LOG.debug("Probe failed for node: %s", node.name)

        # notify node is suspect
        await self._nodes.on_node_suspect(node.name, node.incarnation)

    async def _wait_for_probe(self, seq):

        # start a timer
        start_time = time.time()

        # create a Future
        future = self._loop.create_future()

        # save future to be resolved when an ack is received
        self._probe_status[seq] = future

        try:
            # wait for timeout
            LOG.trace("Waiting for probe (seq=%d)", seq)
            result = await asyncio.wait_for(future, timeout=self.config.probe_timeout / 1000, loop=self._loop)
        except asyncio.TimeoutError:
            end_time = time.time()
            LOG.trace("Timeout waiting for probe (seq=%d) elapsed time: %0.2f", seq, end_time - start_time)
            raise  # re-raise TimeoutError
        finally:
            # remove pending probe status
            del self._probe_status[seq]

        end_time = time.time()
        LOG.trace("Successful probe (seq=%d) elapsed time: %0.2fs", seq, end_time - start_time)

        return result

    async def _probe_node_indirect(self, target_node, k):
        """
        Probe a node indirectly
        :param target_node: node to probe
        :type target_node: state.Node
        """

        def _find_nodes(n):
            return n.name != target_node.name and n.name != self.local_node_name and n.status == state.NODE_STATUS_ALIVE

        # send indirect ping to k nodes
        probes = []
        for indirect_node in utilities.select_random_nodes(k, self._nodes, _find_nodes):
            probes.append(self._probe_node_indirect_via(target_node, indirect_node))

        try:
            results = await asyncio.gather(*probes, loop=self._loop)
            # TODO: check results
        except:
            LOG.exception("Error probing nodes")

    async def _probe_node_indirect_via(self, target_node, indirect_node):
        LOG.debug("Probing node: %s indirectly via %s", target_node.name, indirect_node.name)

        # get a sequence number for the ping
        next_seq = self._ping_seq.increment()

        # start waiting for probe result
        waiter = self._wait_for_probe(next_seq)

        # send ping request message
        ping_req = messages.PingRequestMessage(next_seq,
                                               target=target_node.name,
                                               target_addr=messages.InternetAddress(target_node.host,
                                                                                    target_node.port),
                                               sender=self.local_node_name,
                                               sender_addr=messages.InternetAddress(self.local_node_address,
                                                                                    self.local_node_port))

        LOG.debug("Sending PING-REQ (seq=%d) to %s", ping_req.seq, indirect_node.name)
        await self._send_udp_message(indirect_node.host, indirect_node.port, ping_req)

        # wait for probe result or timeout
        try:
            result = await waiter
        except asyncio.TimeoutError:
            LOG.debug("Timeout waiting for indirect ACK %d", next_seq)
            await self._handle_probe_timeout(target_node)
        else:
            await self._handle_probe_result(target_node, result)

    async def _merge_remote_state(self, remote_state):
        LOG.trace("Merging remote state: %s", remote_state)

        # merge Node into state
        if remote_state.status == state.NODE_STATUS_ALIVE:
            await self._nodes.on_node_alive(remote_state.node,
                                            remote_state.incarnation,
                                            remote_state.addr.address,
                                            remote_state.addr.port)

        elif remote_state.status == state.NODE_STATUS_SUSPECT:
            await self._nodes.on_node_suspect(remote_state.node,
                                              remote_state.incarnation)

        elif remote_state.status == state.NODE_STATUS_DEAD:
            # rather then declaring a node a dead immediately, mark it as suspect
            await self._nodes.on_node_suspect(remote_state.node,
                                              remote_state.incarnation)

        else:
            LOG.warn("Unknown node status: %s", remote_state.status)
            return

    async def _send_local_state(self, stream):

        # get local state
        local_state = []
        for node in self._nodes:
            local_state.append(messages.RemoteNodeState(node=node.name,
                                                        node_addr=messages.InternetAddress(node.host, node.port),
                                                        incarnation=node.incarnation,
                                                        status=node.status))

        LOG.trace("Sending local state %s", local_state)

        # send message
        await stream.write_async(self._encode_message(messages.SyncMessage(remote_state=local_state)))

    async def _sync_node(self, node_host, node_port):
        """
        Sync with remote node
        :param node_host:
        :param node_port:
        :return:
        """
        connection = None
        try:

            # connect to node
            LOG.debug("Connecting to node %s:%d", node_host, node_port)
            try:
                connection = await asyncstream.Client(self._loop).connect(node_host, node_port)
            except Exception:
                LOG.exception("Error connecting to node %s:%d", node_host, node_port)
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

    async def _send_udp_message(self, host, port, msg):
        LOG.trace("Sending %s to %s:%d", msg, host, port)

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
            LOG.trace("Piggy-backing message %s to %s:%d", self._decode_message(g), host, port)
            buf += g

        # send message
        self._udp_listener.sendto(buf, host, port)

    async def _handle_tcp_connection(self, stream, client):
        try:
            # read until closed
            while True:

                try:
                    raw = await self._read_tcp_message(stream)
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

    async def _handle_tcp_message(self, message, client, stream):
        LOG.trace("Handling TCP message from %s", client)
        try:
            if isinstance(message, messages.SyncMessage):
                # noinspection PyTypeChecker
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

    async def _handle_udp_data(self, data, client):
        try:

            # create a buffered reader
            reader = io.BufferedReader(io.BytesIO(data))

            # read until closed
            while True:

                # read a message
                try:
                    raw = self._read_udp_message(reader)
                    if raw is None:
                        break
                except IOError:
                    LOG.exception("Error reading stream")
                    break

                # decode the message
                msg = self._decode_message(raw)
                if msg is None:
                    continue

                # dispatch the message
                await self._handle_udp_message(msg, client)

        except Exception:
            LOG.exception("Error handling UDP data")
            return

    async def _handle_udp_message(self, message, client):
        LOG.trace("Handling UDP message from %s:%d", *client)
        try:
            if isinstance(message, messages.AliveMessage):
                # noinspection PyTypeChecker
                await self._handle_alive_message(message, client)
            elif isinstance(message, messages.SuspectMessage):
                # noinspection PyTypeChecker
                await self._handle_suspect_message(message, client)
            elif isinstance(message, messages.DeadMessage):
                # noinspection PyTypeChecker
                await self._handle_dead_message(message, client)
            elif isinstance(message, messages.PingMessage):
                # noinspection PyTypeChecker
                await self._handle_ping_message(message, client)
            elif isinstance(message, messages.PingRequestMessage):
                # noinspection PyTypeChecker
                await self._handle_ping_request_message(message, client)
            elif isinstance(message, messages.AckMessage):
                # noinspection PyTypeChecker
                await self._handle_ack_message(message, client)
            elif isinstance(message, messages.NackMessage):
                # noinspection PyTypeChecker
                await self._handle_nack_message(message, client)
            else:
                LOG.warn("Unknown message type: %r", message.__class__)
                return
        except:
            LOG.exception("Error dispatching UDP message")
            return

    # noinspection PyUnusedLocal
    async def _handle_alive_message(self, msg, client):
        LOG.trace("Handling ALIVE message: node=%s", msg.node)

        await self._nodes.on_node_alive(msg.node, msg.incarnation, msg.addr.address, msg.addr.port)

    # noinspection PyUnusedLocal
    async def _handle_suspect_message(self, msg, client):
        LOG.trace("Handling SUSPECT message: node=%s", msg.node)

        await self._nodes.on_node_suspect(msg.node, msg.incarnation)

    # noinspection PyUnusedLocal
    async def _handle_dead_message(self, msg, client):
        LOG.trace("Handling DEAD message: node=%s", msg.node)

        await self._nodes.on_node_dead(msg.node, msg.incarnation)

    # noinspection PyUnusedLocal
    async def _handle_ping_message(self, msg, client):
        LOG.trace("Handling PING message: target=%s", msg.node)

        # ensure target node is local node
        if msg.node != self.local_node_name:
            LOG.warn("Received ping message %d from %s for non-local node.", msg.seq, msg.sender_addr)
            return

        # send ack message
        ack = messages.AckMessage(msg.seq, sender=self.local_node_name)
        LOG.debug("Sending ACK (%d) to %s", msg.seq, msg.sender_addr)
        await self._send_udp_message(msg.sender_addr.address, msg.sender_addr.port, ack)

    # noinspection PyUnusedLocal
    async def _handle_ping_request_message(self, msg, client):
        LOG.trace("Handling PING-REQ (%d): target=%s", msg.seq, msg.node)

        # get a sequence number for the ping
        next_seq = self._ping_seq.increment()

        # start waiting for probe result
        waiter = self._wait_for_probe(next_seq)

        # create PingMessage
        ping = messages.PingMessage(next_seq,
                                    target=msg.node,
                                    sender=self.local_node_name,
                                    sender_addr=messages.InternetAddress(self.local_node_address,
                                                                         self.local_node_port))

        LOG.debug("Sending PING (%d) to %s in response to PING-REQ (%d)", next_seq, msg.node_addr, msg.seq)
        await self._send_udp_message(msg.node_addr.address, msg.node_addr.port, ping)

        # wait for probe result or timeout
        try:
            result = await waiter
        except asyncio.TimeoutError:
            LOG.debug("Timeout waiting for ACK %d", next_seq)
            # await self._forward_indirect_probe_timeout(msg)
        else:
            await self._forward_indirect_probe_result(msg)

    # noinspection PyUnusedLocal
    async def _handle_ack_message(self, msg, client):
        LOG.trace("Handling ACK message (%d): sender=%s", msg.seq, msg.sender)

        # resolve pending probe
        ack_seq = msg.seq
        if ack_seq in self._probe_status:
            LOG.debug("Resolving probe (seq=%d) result=%s", msg.seq, True)
            self._probe_status[ack_seq].set_result(True)
        else:
            LOG.warn("Received ACK for unknown probe: %d from %s", msg.seq, msg.sender)

    # noinspection PyUnusedLocal
    async def _handle_nack_message(self, msg, client):
        LOG.trace("Handling NACK message (%d): sender=%s", msg.seq, msg.sender)

        # resolve pending probe
        ack_seq = msg.seq
        if ack_seq in self._probe_status:
            LOG.debug("Resolving probe (seq=%d) result=%s", msg.seq, False)
            self._probe_status[ack_seq].set_result(False)
        else:
            LOG.warn("Received NACK for unknown probe: %d from %s", msg.seq, msg.sender)

    async def _forward_indirect_probe_result(self, msg):
        # create AckMessage
        ack = messages.AckMessage(msg.seq, sender=self.local_node_name)
        LOG.debug("Forwarding ACK (%d) to %s", msg.seq, msg.node)
        await self._send_udp_message(msg.sender_addr.address, msg.sender_addr.port, ack)

    async def _forward_indirect_probe_timeout(self, msg):
        # create NackMessage
        ack = messages.NackMessage(msg.seq, sender=self.local_node_name)
        LOG.debug("Forwarding NACK (%d) to %s", msg.seq, msg.node)
        await self._send_udp_message(msg.sender_addr.address, msg.sender_addr.port, ack)
