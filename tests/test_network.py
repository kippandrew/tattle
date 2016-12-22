import socket
import unittest

from tornado import gen
from tornado import testing

from tattle import network

from tests import fixture


class MessageEqualityTestCase(unittest.TestCase):
    def test_message_equals(self):
        msg1 = network.PingMessage(1, 'test')
        msg2 = network.PingMessage(1, 'test')

        self.assertEqual(msg1, msg2)

    def test_message_not_equals(self):
        msg1 = network.PingMessage(1, 'test')
        msg2 = network.PingMessage(2, 'test')

        self.assertNotEqual(msg1, msg2)

    def test_message_hash(self):
        msg1 = network.PingMessage(1, 'test')
        msg2 = network.PingMessage(2, 'test')
        msg3 = network.PingMessage(2, 'test')

        self.assertEqual(len({msg1, msg2, msg3}), 2)


class MessageEncoderTestCase(unittest.TestCase):
    pass


class MessageDecoderTestCase(unittest.TestCase):
    def test_encode(self):
        orig = network.PingMessage(seq=1, node="test")
        buf = network.MessageEncoder.encode(orig)
        self.assertEqual(orig, network.MessageDecoder.decode(buf))


class AbstractNetworkTestCase(fixture.AbstractTestCase):
    def setUp(self):
        # setup super class
        super(AbstractNetworkTestCase, self).setUp()

        self._next_port = 55555

    def _get_available_local_address(self):
        port = self._next_port
        self._next_port += 1
        return (socket.gethostbyname('localhost'), port)

    def _create_udp_connection(self, bind_address='', bind_port=None):
        peer = network.UDPConnection()
        peer.bind(bind_address, bind_port)
        self.addCleanup(lambda: peer.close())  # automatically close the socket after tests run
        return peer

    def _create_udp_client(self):
        client = network.UDPClient()
        self.addCleanup(lambda: client.close())  # automatically close the client after tests run
        return client

    def _create_udp_listener(self, listen_address, listen_port):
        listener = network.UDPListener()
        listener.listen(listen_port, listen_address)
        self.addCleanup(lambda: listener.stop())  # automatically close the socket after tests run
        return listener


class UDPConnectionTestCase(AbstractNetworkTestCase):
    @testing.gen_test(timeout=1)
    def test_send_and_recv(self):
        # create a connection to peer2 bound to peer1
        peer1_addr = self._get_available_local_address()
        peer1 = self._create_udp_connection(*peer1_addr)

        # create a connection to peer1 bound to peer2
        peer2_addr = self._get_available_local_address()
        peer2 = self._create_udp_connection(*peer2_addr)

        # send message from peer1 to peer2
        peer1.sendto('Foo Bar', *peer2_addr)
        data, addr = yield peer2.recvfrom()
        self.assertEqual(data, 'Foo Bar')
        self.assertEqual(addr, peer1_addr)

        # send message from peer2 to peer1
        peer2.sendto('Ding Dong', *peer1_addr)
        data, addr = yield peer1.recvfrom()
        self.assertEqual(data, 'Ding Dong')
        self.assertEqual(addr, peer2_addr)


class AbstractListenerTestCase(AbstractNetworkTestCase):
    def setUp(self):
        super(AbstractListenerTestCase, self).setUp()
        self.received = list()

    def _handle_message(self, data, addr):
        self.received.append((data, addr))

    def assertMessageReceived(self, msg, addr=None):
        for m, a in self.received:
            if m == msg:
                if addr is not None and a == addr:
                    return
                else:
                    return
        self.fail("Message not received: %s from %s" % (msg, addr))

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
    def test_send_udp_message(self):
        test_message = network.PingMessage()

        peer1_addr = self._get_available_local_address()
        peer1 = self._create_udp_connection(*peer1_addr)

        client = self._create_udp_client()
        client.sendto(test_message, *peer1_addr)
        buf, addr = yield peer1.recvfrom()

        received_message = network.MessageDecoder.decode(buf)
        self.assertEqual(test_message, received_message)


class UDPListenerTestCase(AbstractListenerTestCase):
    @testing.gen_test
    def test_receive_udp_message(self):
        test_message = network.PingMessage()

        # configure a UDPListener
        listener_addr = self._get_available_local_address()
        listener = self._create_udp_listener(*listener_addr)
        listener.start(self._handle_message)

        peer1_addr = ('localhost', 12345)
        peer1 = self._create_udp_connection(*peer1_addr)
        peer1.connect(*listener_addr)
        peer1.send(network.MessageEncoder.encode(test_message))

        yield gen.sleep(0.1)

        self.assertMessageReceived(test_message, peer1_addr)


class TCPListenerTestCase(AbstractNetworkTestCase):
    pass
