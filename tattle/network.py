import socket
import time
import errno
import sys

from tornado import concurrent
from tornado import ioloop

from tattle import logging

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


class TCPListener(object):
    def __init__(self, listen_address=None, custom_ioloop=None):
        pass


class UDPListener(object):
    def __init__(self, listen_address=None, custom_ioloop=None):
        self._socket = self._create_socket(listen_address)
        self._state = None
        self._read_future = None
        self._read_timeout = None
        self._write_future = None
        self._read_bytes = None
        self._ioloop = custom_ioloop or ioloop.IOLoop.current()

    def _create_socket(self, listen_address):
        udpsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udpsock.setblocking(False)
        if listen_address is not None:
            udpsock.bind(listen_address)
        return udpsock

    @property
    def local_address(self):
        return self._socket.getsockname()

    @property
    def peer_address(self):
        return self._socket.getpeername()

    def sendto(self, data, addr):
        """
        Send data
        :param data:
        :return:
        """
        return self._socket.sendto(data, addr)

    def recvfrom(self, max_bytes=4096, timeout=5):
        """
        Receive data
        :param timeout:
        :param max_bytes:
        :return: Future
        """
        future = self._set_read_future()
        self._read_bytes = max_bytes
        self._read_timeout = self._ioloop.add_timeout(time.time() + timeout, self._handle_read_timeout)
        self._add_io_state(self._ioloop.READ)
        return future

    def close(self):
        """
        Clone connection
        :return:
        """
        self._ioloop.remove_handler(self._socket.fileno())
        self._socket.close()
        self._socket = None

    def _add_io_state(self, state):
        if self._state is None:
            self._state = ioloop.IOLoop.ERROR | state
            self._ioloop.add_handler(self._socket.fileno(), self._handle_events, self._state)
        elif not self._state & state:
            self._state = self._state | state
            self._ioloop.update_handler(self._socket.fileno(), self._state)

    def _set_read_future(self):
        assert self._read_future is None, "Already reading"
        self._read_future = concurrent.TracebackFuture()
        return self._read_future

    def _resolve_read_future(self, result):
        assert self._read_future is not None, "Not running"
        future = self._read_future
        self._read_future = None
        future.set_result(result)

    def _handle_read_timeout(self):
        if self._read_future is not None:
            future = self._read_future
            self._read_future = None
            future.set_exception(ioloop.TimeoutError)

    def _handle_read_error(self):
        if self._read_future is not None:
            future = self._read_future
            self._read_future = None
            future.set_exc_info(sys.exc_info())
            # XXX close socket?

    def _handle_read(self):
        # clear timeout
        if self._read_timeout:
            self._ioloop.remove_timeout(self._read_timeout)

        if self._read_future:
            try:
                data, addr = self._socket.recvfrom(self._read_bytes)
            except socket.error as e:
                if e.args[0] in _ERRNO_WOULDBLOCK:
                    return
                else:
                    self._handle_read_error()
                    return
            if not data:
                self.close()
                return

            # resolve read future
            self._resolve_read_future((data, addr))

    def _handle_events(self, fd, events):
        if events & self._ioloop.READ:
            self._handle_read()
        if events & self._ioloop.ERROR:
            LOG.error('%s event error' % self)
