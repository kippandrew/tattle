import socket

import tests

from tattle import network


class UDPListenerTestCase(tests.AbstractTestCase):
    # noinspection PyAttributeOutsideInit
    def setUp(self):
        # setup super class
        super(UDPListenerTestCase, self).setUp()

        self._next_port = 55555

    def _get_listener_address(self):
        port = self._next_port
        self._next_port += 1
        return (socket.gethostbyname('localhost'), port)

    def _create_listener(self, listener_addr):
        peer = network.UDPListener(listener_addr)
        self.addCleanup(lambda: peer.close())  # automatically close the socket after tests run
        return peer

    @tests.gen_test(timeout=1)
    def test_udp_listener(self):
        peer1_addr = self._get_listener_address()
        peer2_addr = self._get_listener_address()

        # create a connection to peer2 bound to peer1
        peer1 = self._create_listener(peer1_addr)

        # create a connection to peer1 bound to peer2
        peer2 = self._create_listener(peer2_addr)

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
