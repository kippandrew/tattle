import socket
from datetime import timedelta

from tornado import gen
from tornado import testing

from tattle import message
from tattle import network

from tests import fixture
from tests import helpers


class AbstractNetworkTestCase(fixture.AbstractTestCase):
    def setUp(self):
        # setup super class
        super(AbstractNetworkTestCase, self).setUp()

        self._next_port = 55555

    def _get_listener_address(self):
        port = self._next_port
        self._next_port += 1
        return (socket.gethostbyname('localhost'), port)


class UDPListenerTestCase(AbstractNetworkTestCase):
    def _create_udp_listener(self, listener_addr):
        peer = network.UDPListener(listener_addr)
        self.addCleanup(lambda: peer.close())  # automatically close the socket after tests run
        return peer

    @testing.gen_test(timeout=1)
    def test_udp_listener(self):
        peer1_addr = self._get_listener_address()
        peer2_addr = self._get_listener_address()

        # create a connection to peer2 bound to peer1
        peer1 = self._create_udp_listener(peer1_addr)

        # create a connection to peer1 bound to peer2
        peer2 = self._create_udp_listener(peer2_addr)

        # send message from peer1 to peer2
        peer1.sendto('Foo Bar', peer2_addr)
        data, addr = yield peer2.recvfrom()
        self.assertEqual(data, 'Foo Bar')
        self.assertEqual(addr, peer1_addr)

        # send message from peer2 to peer1
        peer2.sendto('Ding Dong', peer1_addr)
        data, addr = yield peer1.recvfrom()
        self.assertEqual(data, 'Ding Dong')
        self.assertEqual(addr, peer2_addr)


class TCPListenerTestCase(AbstractNetworkTestCase):
    pass


class MessageListenerTestCase(AbstractNetworkTestCase):
    def setUp(self):
        super(MessageListenerTestCase, self).setUp()
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

    @testing.gen_test
    def test_receive_udp_message(self):

        msg = message.PingMessage()
        addr = ('localhost', 12345)

        fake_udp_listener = helpers.FakeUDPListener()
        fake_tcp_listener = helpers.FakeTCPListener()

        listener = network.MessageListener(fake_udp_listener, fake_tcp_listener)
        listener.listen(self._handle_message)

        fake_udp_listener.send(network.encode_message(msg), addr)

        yield gen.sleep(0.1)

        self.assertMessageReceived(msg, addr)

    @testing.gen_test
    def test_receive_udp_garbled_message(self):

        msg = 'deadbeef'
        addr = ('localhost', 12345)

        fake_udp_listener = helpers.FakeUDPListener()
        fake_tcp_listener = helpers.FakeTCPListener()

        listener = network.MessageListener(fake_udp_listener, fake_tcp_listener)
        listener.listen(self._handle_message)

        fake_udp_listener.send(msg, addr)

        yield gen.sleep(0.1)

        self.assertMessageNotReceived(msg, addr)
