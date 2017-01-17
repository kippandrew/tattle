import binascii
import collections
import inspect
import struct
import sys

import msgpack

# import six

HEADER_LENGTH = 9  # 4 for length, 1 for flags, 4 crc
HEADER_FORMAT = '!IBL'


class MessageError(Exception):
    pass


class MessageEncodeError(MessageError):
    pass


class MessageDecodeError(MessageError):
    pass


class MessageChecksumError(MessageDecodeError):
    pass


class BaseMessage(object):
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
                # if field has a type defined it must of that type or None
                if a is not None and not issubclass(a.__class__, cls):
                    raise TypeError("Field %s must be of type: %s (is %s)" % (key, cls.__name__, a.__class__.__name__))
            self.__setattr__(key, a)

        # assign values from kwargs
        names = [f[0] for f in fields]
        for k, a in kwargs.items():
            i = names.index(k)
            if i < 0:
                raise KeyError("Invalid field: %s" % k)
            key, cls = fields[i]
            if cls is not None:
                # if field has a type defined it must of that type or None
                if a is not None and not issubclass(a.__class__, cls):
                    raise TypeError("Field %s must be of type: %s (is %s)" % (key, cls.__name__, a.__class__.__name__))
            self.__setattr__(k, a)

    def __repr__(self):
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
            if issubclass(base, BaseMessage):
                # noinspection PyProtectedMember
                for f in base._fields_:
                    if isinstance(f, tuple):
                        fields.append(f)
                    else:
                        fields.append((f, None))
        return fields


class Message(BaseMessage):
    def __init__(self, *args, **kwargs):
        super(Message, self).__init__(*args, **kwargs)


class MessageDecoder(object):
    @classmethod
    def _deserialize_internal(cls, data):
        # get message type
        message_type_name = data.pop(0)
        if message_type_name is None:
            return None
        message_type = getattr(sys.modules[__name__], message_type_name)

        message_args = []

        # deserialize all message fields
        message_fields = message_type.get_fields()
        for i in range(len(message_fields)):
            field_name, field_type = message_fields[i]
            if field_type is not None:

                # deserialize the field unless its None
                attr = data[0]
                if attr is None:
                    message_args.append(data.pop(0))
                else:
                    message_args.append(cls._deserialize_internal(data))

            else:
                attr = data.pop(0)
                if isinstance(attr, str) or isinstance(attr, bytes):
                    message_args.append(attr)
                elif isinstance(attr, collections.Sequence):
                    message_args.append([cls._deserialize_internal(i) for i in attr])
                else:
                    message_args.append(attr)

        # shenanigans to initialize Message without calling constructor
        message = message_type.__new__(message_type, *message_args)
        BaseMessage.__init__(message, *message_args)
        return message

    @classmethod
    def _deserialize(cls, raw):
        message = cls._deserialize_internal(msgpack.unpackb(raw, encoding='utf-8', use_list=True))
        return message

    @classmethod
    def decode(cls, buf):
        # TODO: encryption
        # TODO: compression
        if len(buf) <= HEADER_LENGTH:
            raise MessageDecodeError("Message is too short")
        length, flags, crc, = struct.unpack(HEADER_FORMAT, buf[0:HEADER_LENGTH])  # unpack header in network B/O
        buf = buf[HEADER_LENGTH:]
        expected = binascii.crc32(buf) & 0xffffffff  # https://docs.python.org/3/library/binascii.html#binascii.crc32
        if crc != expected:
            raise MessageChecksumError("Message checksum mismatch: 0x%X != 0x%X" % (crc, expected))
        return cls._deserialize(buf)


class MessageEncoder(object):
    @classmethod
    def _serialize_internal(cls, msg):
        # insert the name of the class
        data = [msg.__class__.__name__]

        # get list of fields
        fields = msg.__class__.get_fields()
        for field_name, field_type in fields:
            attr = getattr(msg, field_name)
            if field_type is not None and attr is not None:
                # if attr has a field type defined deserialize that field
                data.extend(cls._serialize_internal(attr))
            else:
                if isinstance(attr, str) or isinstance(attr, bytes):
                    data.append(attr)
                elif isinstance(attr, collections.Sequence):
                    data.append([cls._serialize_internal(i) for i in attr])
                elif isinstance(attr, collections.Mapping):
                    data.append({k: cls._serialize_internal(v) for k, v in attr.items()})
                else:
                    data.append(attr)
        return data

    @classmethod
    def _serialize(cls, msg):
        return msgpack.packb(cls._serialize_internal(msg), use_bin_type=True, encoding='utf-8')

    @classmethod
    def encode(cls, msg):
        # TODO: encryption
        # TODO: compression
        raw = cls._serialize(msg)
        crc = binascii.crc32(raw) & 0xffffffff  # https://docs.python.org/3/library/binascii.html#binascii.crc32
        flags = 0
        length = len(raw) + HEADER_LENGTH
        header = struct.pack(HEADER_FORMAT, length, flags, crc)  # pack header in network B/O
        return header + raw


class InternetAddress(BaseMessage):
    _fields_ = [
        "addr_v4",
        "addr_v6",
        "port"
    ]

    def __init__(self, addr_v4, port, addr_v6=None):
        super(InternetAddress, self).__init__(addr_v4, addr_v6, port)

    @property
    def address(self):
        if self.addr_v6 is not None:
            return self.addr_v6
        else:
            return self.addr_v4

    def __str__(self):
        return "<%s %s,%d>" % (self.__class__.__name__, self.address, self.port)


class PingMessage(Message):
    _fields_ = [
        "seq",
        "node",
        "sender",
        ("sender_addr", InternetAddress),
    ]

    def __init__(self, seq, target, sender=None, sender_addr=None):
        """
        Create new instance of the PingMessage class
        :param seq: sequence number
        :param target: target node name
        :param sender: sender node name
        :param sender_addr: sender node address
        """
        super(PingMessage, self).__init__(seq, target, sender, sender_addr)

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.node)


class PingRequestMessage(Message):
    _fields_ = [
        "seq",
        "node",
        ("node_addr", InternetAddress),
        "sender",
        ("sender_addr", InternetAddress),
    ]

    def __init__(self, seq, target, target_addr, sender=None, sender_addr=None):
        """
        Create new instance of the PingRequestMessage class
        :param seq: sequence number
        :param target: target node name
        :param target_addr: target node address
        :param sender: sender node name
        :param sender_addr: sender node address
        """
        super(PingRequestMessage, self).__init__(seq, target, target_addr, sender, sender_addr)

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.node)


class AckMessage(Message):
    _fields_ = [
        "seq",
        "sender"
    ]


class NackMessage(Message):
    _fields_ = [
        "seq",
        "sender"
    ]


class SuspectMessage(Message):
    _fields_ = [
        "node",  # node name
        "incarnation",
        "sender",
    ]

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.node)


class DeadMessage(Message):
    _fields_ = [
        "node",  # node name
        "incarnation",
        "sender"
    ]

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.node)


class AliveMessage(Message):
    _fields_ = [
        "node",  # node name
        ("addr", InternetAddress),
        "incarnation"
    ]

    def __init__(self, node, addr, incarnation):
        super(AliveMessage, self).__init__(node, addr, incarnation)

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.node)


class RemoteNodeState(BaseMessage):
    _fields_ = [
        "node",
        ("addr", InternetAddress),
        "incarnation",
        "status",
    ]

    def __init__(self, node, node_addr, incarnation, status):
        super(RemoteNodeState, self).__init__(node, node_addr, incarnation, status)


class SyncMessage(Message):
    _fields_ = [
        "nodes"
    ]

    def __init__(self, remote_state):
        super(SyncMessage, self).__init__(remote_state)
