import errno
import socket
import sys
import time

from tornado import concurrent
from tornado import gen
from tornado import ioloop
from tornado import stack_context
from tornado import tcpclient
from tornado import tcpserver

from tattle import logging

__all__ = [
    'TCPListener',
    'TCPClient',
    'UDPConnection',
    'UDPListener',
]

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


class TCPClient(tcpclient.TCPClient):
    """
    TCPClient is a factory for creating TCP connections (IOStream)
    """
    pass


class TCPListener(tcpserver.TCPServer):
    """
    The TCPListener listens for messages over TCP
    """

    def __init__(self):
        super(TCPListener, self).__init__()
        self._stream_callback = None

    @property
    def local_address(self):
        for s in self._sockets.values():
            if s.family == socket.AF_INET:
                return s.getsockname()[0]

    @property
    def local_port(self):
        for s in self._sockets.values():
            if s.family == socket.AF_INET:
                return s.getsockname()[1]

    # noinspection PyMethodOverriding
    def start(self, message_callback):
        super(TCPListener, self).start()
        self._stream_callback = stack_context.wrap(message_callback)

    def stop(self):
        super(TCPListener, self).stop()
        self._stream_callback = None

    @gen.coroutine
    def handle_stream(self, stream, addr):
        LOG.debug("Handing incoming TCP connection from: %s", addr)

        # run callback
        try:
            if self._stream_callback is not None:
                yield self._stream_callback(stream, addr)
        except:
            LOG.exception("Error running callback")

        LOG.debug("Finished handling TCP connection from: %s", addr)


class UDPConnection(object):
    """
    The UDPConnection class is low-level interface for sending and receiving data asynchronously via UDP.
    """

    def __init__(self):
        self._socket = self._create_socket()
        self._state = None
        self._read_future = None
        self._write_future = None
        self._read_bytes = None
        self._read_timeout = None
        self._ioloop = ioloop.IOLoop.current()

    def _create_socket(self):
        udpsock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udpsock.setblocking(False)
        return udpsock

    @property
    def local_address(self):
        return self._socket.getsockname()[0]

    @property
    def local_port(self):
        return self._socket.getsockname()[1]

    def connect(self, address, port):
        """
        Connect to a given address (for use with send)
        :param address:
        :param port:
        :return:
        """
        self._socket.connect((address, port))

    def bind(self, address, port):
        """
        Bind the connection to given port and address
        :param address:
        :param port:
        :return:
        """
        self._socket.bind((address, port if port is not None else 0))

    def close(self):
        """
        Close connection
        :return: None
        """
        self._ioloop.remove_handler(self._socket.fileno())
        self._socket.close()
        self._socket = None

    @gen.coroutine
    def send(self, data):
        """
        Send data
        :param data:
        :return: Future
        """
        return self._socket.send(data)

    def sendto(self, data, address, port):
        """
        Send data
        :param data:
        :param address:
        :param port:
        :return: Future
        """
        return self._socket.sendto(data, (address, port))

    def recvfrom(self, max_buffer_size=4096, timeout=5):
        """
        Receive data
        :param timeout:
        :param max_buffer_size:
        :return: Future
        """
        future = self._set_read_future()
        self._read_bytes = max_buffer_size
        if timeout > 0:
            self._read_timeout = self._ioloop.add_timeout(time.time() + timeout, self._handle_read_timeout)
        self._add_io_state(self._ioloop.READ)
        return future

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

    def _resolve_read_future(self, result, exception=None, exc_info=None):
        assert self._read_future is not None, "Not reading"
        future = self._read_future
        self._read_future = None
        if exception is not None:
            future.set_exception(exception)
        elif exc_info is not None:
            future.set_exc_info(exc_info)
        else:
            future.set_result(result)

    def _handle_read_timeout(self):
        """
        _handle_read_timeout is called when a timeout occurs reading data from the socket
        """
        if self._read_future is not None:
            self._resolve_read_future(None, exception=ioloop.TimeoutError())

    def _handle_read_error(self):
        """
        _handle_read_error is called when an error occurs reading data from the socket
        """
        if self._read_future is not None:
            self._resolve_read_future(None, exc_info=sys.exc_info())

    def _handle_read(self):
        """
        _handle_read is called when the ioloop reports data is available on the socket
        """
        try:
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

                # run the read callback
                self._resolve_read_future((data, addr))

        except Exception:
            LOG.exception("Error handling read")

    # noinspection PyUnusedLocal
    def _handle_events(self, fd, events):
        """
        _handle_events is called when io state changes on the socket
        """
        if events & self._ioloop.READ:
            self._handle_read()
        if events & self._ioloop.ERROR:
            LOG.error('%s event error' % self)


class UDPClient(object):
    """
    The UDPClient is a factory for creating UDP connections
    """

    @gen.coroutine
    def connect(self, address, port):
        """
        Create a UDPConnection
        :param address:
        :param port:
        :return: Future
        """
        conn = UDPConnection()
        conn.connect(address, port)
        raise gen.Return(conn)


class UDPListener(object):
    """
    The UDPListener listens for messages via UDP
    """

    def __init__(self):
        self._connection = UDPConnection()
        self._ioloop = ioloop.IOLoop.current()
        self._data_callback = None

    @property
    def local_address(self):
        return self._connection.local_address

    @property
    def local_port(self):
        return self._connection.local_port

    def listen(self, port, address=""):
        """
        Listen for messages on a given port and address
        :param port:
        :param address:
        :return:
        """
        self._connection.bind(address, port)

    def start(self, data_callback):
        """
        Start the listener
        :return:
        """
        self._data_callback = stack_context.wrap(data_callback)

        # wait for data
        self._ioloop.add_future(self._connection.recvfrom(timeout=0), self._handle_data)

    def stop(self):
        """
        Stop the listener
        :return:
        """
        self._data_callback = None
        self._connection.close()

    def _handle_data(self, future):
        """
        _handle_data is called when data has been read
        """
        try:

            # handle future error
            error = future.exception()
            if error is not None:
                LOG.error(error, exc_info=future.exc_info())
                return

            # get future result
            data, addr = future.result()

            try:
                if self._data_callback is not None:
                    self._data_callback(data, addr)
            except:
                LOG.exception("Error running callback")

        except:
            LOG.exception("Error handling data")

        # wait for data
        self._ioloop.add_future(self._connection.recvfrom(timeout=0), self._handle_data)
