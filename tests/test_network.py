import asyncio
import socket

import asynctest

from tattle import network


class AbstractNetworkTestCase(asynctest.TestCase):
    # noinspection PyAttributeOutsideInit
    def setUp(self):
        # setup super class
        super(AbstractNetworkTestCase, self).setUp()
        self._next_port = 55555

    def _get_available_local_address(self):
        port = self._next_port
        self._next_port += 1
        return (socket.gethostbyname('localhost'), port)

    def _create_udp_connection(self):
        peer = network.UDPConnection()
        self.addCleanup(lambda: peer.close())  # automatically close the socket after tests run
        return peer

    def _create_udp_listener(self, listen_address, listen_port, callback):
        listener = network.UDPListener(listen_address, listen_port, callback)
        self.addCleanup(lambda: listener.stop())  # automatically close the socket after tests run
        return listener

    def _create_tcp_listener(self, listen_address, listen_port, callback):
        listener = network.TCPListener(listen_address, listen_port, callback)
        self.addCleanup(lambda: listener.stop())  # automatically close the socket after tests run
        return listener


class UDPConnectionTestCase(AbstractNetworkTestCase):
    async def test_send_and_recv(self):
        # create a connection
        peer1_addr = self._get_available_local_address()
        peer1 = self._create_udp_connection()
        peer1.bind(peer1_addr)

        # create a connection
        peer2_addr = self._get_available_local_address()
        peer2 = self._create_udp_connection()
        peer2.bind(peer2_addr)

        # send message from peer1 to peer2
        await peer1.connect(peer2_addr)
        await peer1.send(b'Foo Bar')
        data = await peer2.recv(1024)
        self.assertEqual(data, b'Foo Bar')

    async def test_sendto_and_recvform(self):
        # create a connection
        peer1_addr = self._get_available_local_address()
        peer1 = self._create_udp_connection()
        peer1.bind(peer1_addr)

        # create a connection
        peer2_addr = self._get_available_local_address()
        peer2 = self._create_udp_connection()
        peer2.bind(peer2_addr)

        # send message from peer2 to peer1
        await peer2.sendto(b'Ding Dong', peer1_addr)
        data, addr = await peer1.recvfrom(1024)
        self.assertEqual(data, b'Ding Dong')
        self.assertEqual(addr, peer2_addr)


class AbstractListenerTestCase(AbstractNetworkTestCase):
    # noinspection PyAttributeOutsideInit
    def setUp(self):
        super(AbstractListenerTestCase, self).setUp()
        self.received = list()

    # noinspection PyPep8Naming
    def assertMessageReceived(self, msg, addr=None):
        for m, a in self.received:
            if m == msg:
                if addr is not None and a == addr:
                    return
                else:
                    return
        self.fail("Message not received: %s from %s %s" % (msg, addr, self.received))

    # noinspection PyPep8Naming
    def assertMessageNotReceived(self, msg, addr=None):
        found = False
        for m, a in self.received:
            if m == msg:
                if addr is not None and a == addr:
                    found = True
                    break
                else:
                    found = True
                    break
        if found:
            self.fail("Message was received: %s from %s" % (msg, addr))


class UDPListenerTestCase(AbstractListenerTestCase):
    def _handle_udp_data(self, data, addr):
        self.received.append((data, addr))

    async def test_receive_udp_message(self):
        # create a UDPListener
        listener_addr = self._get_available_local_address()
        listener = self._create_udp_listener(*listener_addr, self._handle_udp_data)
        await listener.start()

        # create a UDP connection
        peer1 = self._create_udp_connection()

        # send message to listener
        await peer1.sendto(b'foo', listener_addr)
        await peer1.sendto(b'bar', listener_addr)

        # create a UDP connection
        peer2 = self._create_udp_connection()

        # send message to listener
        await peer2.connect(listener_addr)
        await peer2.send(b'baz')

        await asyncio.sleep(.2)

        self.assertMessageReceived(b'foo')
        self.assertMessageReceived(b'bar')
        self.assertMessageReceived(b'baz')


class TCPListenerTestCase(AbstractListenerTestCase):
    async def _handle_tcp_stream(self, reader, writer, addr):
        data = await reader.read(255)
        self.received.append((data, addr))

    async def test_receive_tcp_message(self):
        # configure a TCPListener
        listener_addr = self._get_available_local_address()
        listener = self._create_tcp_listener(*listener_addr, self._handle_tcp_stream)
        await listener.start()

        # create a TCP connection
        reader, writer = await asyncio.open_connection(*listener_addr)

        # send message to listener
        writer.write(b'hello world')

        await asyncio.sleep(0.1)

        self.assertMessageReceived(b'hello world')
