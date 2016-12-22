import binascii
import collections
import errno
import inspect
import socket
import struct
import sys
import time

import msgpack

from tornado import concurrent
from tornado import ioloop
from tornado import tcpclient
from tornado import tcpserver
from tattle import logging

__all__ = [
    'Message',
    'MessageDecoder',
    'MessageEncoder',
    'MessageError',
    'MessageDecodeError',
    'MessageEncodeError',
    'MessageChecksumError',
    'RefuteMessage',
    'PingMessage',
    'AckMessage',
    'NackMessage',
    'AliveMessage',
    'DeadMessage',
    'SuspectMessage',
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


class MessageError(Exception):
    pass


class MessageEncodeError(MessageError):
    pass


class MessageDecodeError(MessageError):
    pass


class MessageChecksumError(MessageDecodeError):
    pass


class _MessageBase(object):
    _fields_ = []

    def __init__(self, *args, **kwargs):

        # initialize fields
        fields = self.__class__.get_fields()
        for f in fields:
            self.__setattr__(f[0], None)

        # assign values from args
        for i, a in enumerate(args):
            key, cls = fields[i]
            if cls is not None:
                if not issubclass(a.__class__, cls):
                    raise TypeError("Field must be of type: %s" % cls.__name__)
            self.__setattr__(key, a)

        # assign values from kwargs
        names = [f[0] for f in fields]
        for k, a in kwargs.items():
            i = names.index(k)
            if i < 0:
                raise KeyError("Invalid field: %s" % k)
            key, cls = fields[i]
            if cls is not None:
                if not issubclass(a.__class__, cls):
                    raise TypeError("Field must be of type: %s" % cls.__name__)
            self.__setattr__(k, a)

    def __str__(self):
        d = collections.OrderedDict()
        for f in self.__class__.get_fields():
            attr = getattr(self, f[0])
            d[f[0]] = attr
        return "<%s %s>" % (self.__class__.__name__, dict(d))

    def __eq__(self, other):
        """Override the default equals behavior"""
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    def __ne__(self, other):
        """Define a non-equality test"""
        if isinstance(other, self.__class__):
            return not self.__eq__(other)
        return NotImplemented

    def __hash__(self):
        """Override the default hash behavior"""
        return hash(tuple(sorted(self.__dict__.items())))

    @classmethod
    def get_fields(cls):
        fields = []
        for base in reversed(inspect.getmro(cls)):
            if issubclass(base, _MessageBase):
                # noinspection PyProtectedMember
                for f in base._fields_:
                    if isinstance(f, tuple):
                        fields.append(f)
                    else:
                        fields.append((f, None))
        return fields


class Message(_MessageBase):
    def __init__(self, *args, **kwargs):
        super(Message, self).__init__(*args, **kwargs)


class MessageDecoder(object):
    @classmethod
    def _deserialize_internal(cls, data):
        # get class
        klass = getattr(sys.modules[__name__], data.pop(0))

        # get args
        args = data

        # get a list of fields
        fields = klass.get_fields()

        # deserialize any arguments first
        for field_name, field_type in fields:
            if field_type is not None:
                args.insert(0, cls._deserialize_internal(data))

        # get augments to pass to constructor
        args = [data.pop(0) for _ in range(len(fields))]

        # shenanigans to initialize Message without calling constructor
        obj = klass.__new__(klass, *args)
        _MessageBase.__init__(obj, *args)
        return obj

    @classmethod
    def deserialize(cls, raw):
        message = cls._deserialize_internal(msgpack.unpackb(raw, use_list=True))
        return message

    @classmethod
    def decode(cls, buf):
        # TODO: encryption
        # TODO: compression
        if len(buf) <= 8:
            raise MessageDecodeError("Message is too short")
        length, crc, = struct.unpack('!Il', buf[0:8])  # unpack header in network B/O
        buf = buf[8:]
        expected = binascii.crc32(buf)
        if crc != expected:
            raise MessageChecksumError("Message checksum mismatch: 0x%X != 0x%X" % (crc, expected))
        return cls.deserialize(buf)


class MessageEncoder(object):
    @classmethod
    def _serialize_internal(cls, msg):
        # insert the name of the class
        data = [msg.__class__.__name__]
        # get list of fields
        fields = msg.__class__.get_fields()
        for field_name, field_type in fields:
            attr = getattr(msg, field_name)
            if field_type is not None:
                data.extend(cls._serialize_internal(attr))
            else:
                data.append(attr)
        return data

    @classmethod
    def serialize_message(cls, msg):
        return msgpack.packb(cls._serialize_internal(msg))

    @classmethod
    def encode(cls, msg):
        # TODO: encryption
        # TODO: compression
        raw = cls.serialize_message(msg)
        crc = binascii.crc32(raw)
        length = len(raw) + 4 + 4  # 4 bytes length, 4 bytes for crc
        header = struct.pack('!Il', length, crc)  # pack header in network B/O
        return header + raw


class PingMessage(Message):
    _fields_ = [
        "seq",
        "node"
    ]


class AckMessage(Message):
    _fields_ = [
        "seq"
    ]


class NackMessage(Message):
    _fields_ = [
        "seq"
    ]


class SuspectMessage(Message):
    _fields_ = [
        "node",
        "incarnation",
        "from"
    ]


class DeadMessage(Message):
    _fields_ = [
        "node",
        "incarnation",
        "from"
    ]


class AliveMessage(Message):
    _fields_ = [
        "node",
        "incarnation",
        "addr",
        "port"
    ]


class RefuteMessage(Message):
    pass


class TCPClient(tcpclient.TCPClient):
    pass


class TCPListener(tcpserver.TCPServer):
    """
    The TCPListener listens for messages over TCP
    """

    def __init__(self):
        super(TCPListener, self).__init__()
        self._message_callback = None

    # noinspection PyMethodOverriding
    def start(self, message_callback):
        super(TCPListener, self).start()
        self._message_callback = message_callback

    def stop(self):
        super(TCPListener, self).stop()
        self._message_callback = None

    def handle_stream(self, stream, address):
        pass


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
        self._socket.bind((address, port))

    def close(self):
        """
        Close connection
        :return: None
        """
        self._ioloop.remove_handler(self._socket.fileno())
        self._socket.close()
        self._socket = None

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
    The UDPClient sends messages via UDP
    """
    def __init__(self):
        self._connection = UDPConnection()

    @property
    def local_address(self):
        return self._connection.local_address

    @property
    def local_port(self):
        return self._connection.local_port

    def connect(self, address, port):
        self._connection.connect(address, port)

    def send(self, message):
        self._connection.send(MessageEncoder.encode(message))

    def sendto(self, message, address, port):
        self._connection.sendto(MessageEncoder.encode(message), address, port)

    def close(self):
        self._connection.close()


class UDPListener(object):
    """
    The UDPListener listens for messages via UDP
    """
    def __init__(self):
        self._connection = UDPConnection()
        self._ioloop = ioloop.IOLoop.current()
        self._message_callback = None

    def listen(self, port, address=""):
        """
        Listen for messages on a given port and address
        :param port:
        :param address:
        :return:
        """
        self._connection.bind(address, port)

    def start(self, message_callback):
        """
        Start the listener
        :return:
        """
        self._message_callback = message_callback

        # wait for data
        self._ioloop.add_future(self._connection.recvfrom(timeout=0), self._handle_data)

    def stop(self):
        """
        Stop the listener
        :return:
        """
        self._message_callback = None
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

            # decode message
            try:
                msg = MessageDecoder.decode(data)
                LOG.debug("Decoded message: %s", msg)
            except MessageDecodeError as e:
                LOG.error("Error decoding message: %s", e)
                return

            try:
                if self._message_callback is not None:
                    self._message_callback(msg, addr)
            except:
                LOG.exception("Error running callback")

        except:
            LOG.exception("Error handling data")

        # wait for data
        self._ioloop.add_future(self._connection.recvfrom(timeout=0), self._handle_data)
