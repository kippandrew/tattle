import socket

from tornado import gen
from tornado import testing

from tattle import network

from tests import fixture


class AbstractNetworkTestCase(fixture.AbstractTestCase):
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

    def _create_udp_listener(self, listen_address, listen_port):
        listener = network.UDPListener()
        listener.listen(listen_port, listen_address)
        self.addCleanup(lambda: listener.stop())  # automatically close the socket after tests run
        return listener

    def _create_tcp_listener(self, listen_address, listen_port):
        listener = network.TCPListener()
        listener.listen(listen_port, listen_address)
        self.addCleanup(lambda: listener.stop())  # automatically close the socket after tests run
        return listener


class UDPConnectionTestCase(AbstractNetworkTestCase):
    @testing.gen_test(timeout=1)
    def test_send_and_recv(self):
        # create a connection to peer2 bound to peer1
        peer1_addr = self._get_available_local_address()
        peer1 = self._create_udp_connection()
        peer1.bind(*peer1_addr)

        # create a connection to peer1 bound to peer2
        peer2_addr = self._get_available_local_address()
        peer2 = self._create_udp_connection()
        peer2.bind(*peer2_addr)

        # send message from peer1 to peer2
        peer1.sendto(b'Foo Bar', *peer2_addr)
        data, addr = yield peer2.recvfrom()
        self.assertEqual(data, b'Foo Bar')
        self.assertEqual(addr, peer1_addr)

        # send message from peer2 to peer1
        peer2.sendto(b'Ding Dong', *peer1_addr)
        data, addr = yield peer1.recvfrom()
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


class UDPClientTestCase(AbstractNetworkTestCase):
    @testing.gen_test
    def test_send_udp(self):
        # create UDPConnection
        peer1_addr = self._get_available_local_address()
        peer1 = self._create_udp_connection()
        peer1.bind(*peer1_addr)

        # create UDPClient
        conn = yield network.UDPClient().connect(*peer1_addr)
        conn.send(b'foo bar')
        received_data, addr = yield peer1.recvfrom()

        self.assertEqual(b'foo bar', received_data)


class UDPListenerTestCase(AbstractListenerTestCase):
    def _handle_udp_data(self, data, addr):
        self.received.append((data, addr))

    @testing.gen_test
    def test_receive_udp_message(self):
        # configure a UDPListener
        listener_addr = self._get_available_local_address()
        listener = self._create_udp_listener(*listener_addr)
        listener.start(self._handle_udp_data)

        # create a UDP connection
        peer1 = self._create_udp_connection()
        peer1_addr = (b'localhost', 12345)
        peer1.bind(*peer1_addr)

        # send message to listener
        peer1.sendto(b'ding dong', *listener_addr)

        yield gen.sleep(0.1)

        self.assertMessageReceived(b'ding dong', peer1_addr)


class TCPListenerTestCase(AbstractListenerTestCase):
    @gen.coroutine
    def _handle_tcp_stream(self, stream, addr):
        data = yield stream.read_bytes(11)
        self.received.append((data, addr))

    @testing.gen_test
    def test_receive_tcp_message(self):
        # configure a TCPListener
        listener_addr = self._get_available_local_address()
        listener = self._create_tcp_listener(*listener_addr)
        listener.start(self._handle_tcp_stream)

        # create a TCP connection
        stream = yield network.TCPClient().connect(*listener_addr)

        # send message to listener
        yield stream.write(b'hello world')

        yield gen.sleep(0.1)

        self.assertMessageReceived(b'hello world')
