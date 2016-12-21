import socket
import struct
import time
import errno
import sys
import binascii

from tornado import concurrent
from tornado import gen
from tornado import ioloop
from tornado.tcpserver import TCPServer

from tattle import logging
from tattle import message

LOG = logging.get_logger(__name__)

# These errnos indicate that a non-blocking operation must be retried
# at a later time.  On most platforms they're the same value, but on
# some they differ.
_ERRNO_WOULDBLOCK = (errno.EWOULDBLOCK, errno.EAGAIN)
if hasattr(errno, "WSAEWOULDBLOCK"):
    _ERRNO_WOULDBLOCK += (errno.WSAEWOULDBLOCK,)

# These errnos indicate that a connection has been abruptly terminated.
# They should be caught and handled less noisily than other errors.
_ERRNO_CONNRESET = (errno.ECONNRESET, errno.ECONNABORTED, errno.EPIPE, errno.ETIMEDOUT)
if hasattr(errno, "WSAECONNRESET"):
    _ERRNO_CONNRESET += (errno.WSAECONNRESET, errno.WSAECONNABORTED, errno.WSAETIMEDOUT)


def encode_message(msg):
    raw = message.serialize(msg)
    calc = binascii.crc32(raw)
    crc = struct.pack('!l', calc)
    buf = crc + raw
    return buf


def decode_message(buf):
    crc, = struct.unpack('!l', buf[0:4])  # first 4 bytes in (network) big-endian
    buf = buf[4:]
    expected = binascii.crc32(buf)
    if crc != expected:
        raise message.MessageChecksumError("Message checksum mismatch: 0x%X != 0x%X" % (crc, expected))
    return message.deserialize(buf)


class TCPClient(object):
    pass


class TCPListener(TCPServer):
    def __init__(self, custom_ioloop=None):
        # initialize super class
        super(TCPListener, self).__init__(io_loop=custom_ioloop)

    def listen(self, port, address="", callback=None):
        pass

    def handle_stream(self, stream, address):
        pass


class UDPListener(object):
    def __init__(self, custom_ioloop=None):
        self._socket = self._create_socket()
        self._state = None
        self._read_callback = None
        self._read_bytes = None
        self._read_timeout = None
        self._listen_callback = None
        self._ioloop = custom_ioloop or ioloop.IOLoop.current()

    def _create_socket(self):
        udpsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udpsock.setblocking(False)
        return udpsock

    def listen(self, port, address="", callback=None):
        """
        Listen for data and invoke the callback when it arrives
        :param port:
        :param address:
        :param callback:
        :return:
        """
        assert self._listen_callback is None, "Already listening"
        self._listen_callback = callback
        self._socket.bind((address, port))

    def start(self):
        """
        Start the listener
        :return:
        """
        assert self._listen_callback is not None, "Not listening"

        def _process_packet(result):
            if result is None:
                return
            try:
                self._listen_callback(result)
            except Exception:
                LOG.exception("Error running callback")

            # wait for data
            self._ioloop.add_future(self.recvfrom(), callback)

        # wait for data
        self._ioloop.add_future(self.recvfrom(), self._listen_callback)

    def stop(self):
        """
        Stop the listener
        :return:
        """
        self._listen_callback = None
        self.close()

    def close(self):
        """
        Close connection
        :return:
        """
        self._ioloop.remove_handler(self._socket.fileno())
        self._socket.close()
        self._socket = None

    def sendto(self, data, addr):
        """
        Send data
        :param data:
        :param addr:
        :return:
        """
        return self._socket.sendto(data, addr)

    def recvfrom(self, max_buffer_size=4096, timeout=5, callback=None):
        """
        Receive data
        :param max_buffer_size:
        :return: Future
        """
        self._set_read_callback(callback)
        self._read_bytes = max_buffer_size
        if timeout > 0:
            self._read_timeout = self._ioloop.add_timeout(time.time() + timeout, self._handle_read_timeout)
        self._add_io_state(self._ioloop.READ)

    def _add_io_state(self, state):
        if self._state is None:
            self._state = ioloop.IOLoop.ERROR | state
            self._ioloop.add_handler(self._socket.fileno(), self._handle_events, self._state)
        elif not self._state & state:
            self._state = self._state | state
            self._ioloop.update_handler(self._socket.fileno(), self._state)

    def _set_read_callback(self, callback):
        assert self._read_callback is None, "Already reading"
        self._read_callback = callback

    def _run_read_callback(self, *args, **kwargs):
        assert self._read_callback is not None, "Not reading"
        callback = self._read_callback
        self._read_callback = None
        try:
            callback(*args, **kwargs)
        except Exception:
            LOG.exception("Error running read callback")

    def _handle_read_timeout(self):
        """
        handle_read_timeout is called when a timeout occurs reading data from the socket
        """
        if self._read_callback is not None:
            self._run_read_callback(None)
        LOG.error("Timeout reading data")

    def _handle_read_error(self, error):
        """
        handle_read_error is called when an error occurs reading data from the socket
        """
        if self._read_callback is not None:
            self._run_read_callback(None)
        LOG.error("Error reading data: %s", error)

    def _handle_read(self):
        """
        handle_read is called when the ioloop reports data is available on the socket (READ)
        """
        try:
            # clear timeout
            if self._read_timeout:
                self._ioloop.remove_timeout(self._read_timeout)

            if self._read_callback:
                try:
                    data, addr = self._socket.recvfrom(self._read_bytes)
                except socket.error as e:
                    if e.args[0] in _ERRNO_WOULDBLOCK:
                        return
                    else:
                        self._handle_read_error(e)
                        return
                if not data:
                    self.close()
                    return

                # run the read callback
                self._run_read_callback((data, addr))

        except Exception:
            LOG.exception("Error handling read")

        # self.recvfrom(self._listen_callback)

    def _handle_events(self, fd, events):
        if events & self._ioloop.READ:
            self._handle_read()
        if events & self._ioloop.ERROR:
            LOG.error('%s event error' % self)


class MessageListener(object):
    """
    The MessageListener class listens for messages via UDP/TCP and emits them via callback when they are received.
    """

    def __init__(self, udp_listener, tcp_listener, custom_ioloop=None):
        """
        Create new instance of the MessageListener class
        :param udp_listener
        :param tcp_listener
        :param custom_ioloop:
        """
        self._udp_listener = udp_listener
        self._tcp_listener = tcp_listener
        self._io_loop = custom_ioloop or ioloop.IOLoop.current()
        self._listen_callback = None
        self._listen_future = None
        self._closed = False

    def listen(self, callback):
        assert self._listen_future is None, "Already listening"

        self._closed = False
        self._listen_future = concurrent.TracebackFuture()
        self._listen_callback = callback

        self._listen_udp()
        self._listen_tcp()

        return self._listen_future

    def close(self):
        self._closed = True
        self._udp_listener.close()
        self._tcp_listener.close()

        self._listen_future.result()

    def _listen_udp(self):
        self._io_loop.add_future(self._udp_listener.recvfrom(4096, timeout=0), self._handle_udp_read_future)

    def _listen_tcp(self):
        pass

    def _handle_udp_read_future(self, future):
        try:
            error = future.exception()
            if error is not None:
                LOG.exception("Error reading UDP socket", exc_info=future.exc_info())
                return

            # get future result
            data, addr = future.result()

            # decode message
            try:
                msg = decode_message(data)
                LOG.debug("Decoded message: %s", msg)
            except message.MessageError as e:
                LOG.error("Error decoding message: %s", e)
                return

            # execute callback
            try:
                self._listen_callback(msg, addr)
            except Exception:
                LOG.exception("Error running callback")

        except Exception:
            LOG.exception("Error handling UDP read")

        # listen again
        self._listen_udp()

    def _handle_tcp_read_future(self, future):
        pass
