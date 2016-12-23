import binascii
import collections
import inspect
import struct
import sys

import msgpack

HEADER_LENGTH = 9  # 4 for length, 1 for flags, 4 crc
HEADER_FORMAT = '!IBl'


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

        args = []
        for _ in range(len(fields)):
            attr = data.pop(0)
            if isinstance(attr, collections.Sequence) and not isinstance(attr, (str, unicode)):
                args.insert(0, [cls._deserialize_internal(i) for i in attr])
            elif isinstance(attr, collections.Mapping):
                args.insert(0, {k: cls._deserialize_internal(v) for k, v in attr.iteritems()})
            else:
                args.append(attr)

        # shenanigans to initialize Message without calling constructor
        obj = klass.__new__(klass, *args)
        BaseMessage.__init__(obj, *args)
        return obj

    @classmethod
    def _deserialize(cls, raw):
        message = cls._deserialize_internal(msgpack.unpackb(raw, use_list=True))
        return message

    @classmethod
    def decode(cls, buf):
        # TODO: encryption
        # TODO: compression
        if len(buf) <= HEADER_LENGTH:
            raise MessageDecodeError("Message is too short")
        length, flags, crc, = struct.unpack(HEADER_FORMAT, buf[0:HEADER_LENGTH])  # unpack header in network B/O
        buf = buf[HEADER_LENGTH:]
        expected = binascii.crc32(buf)
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
            if field_type is not None:
                data.extend(cls._serialize_internal(attr))
            else:
                if isinstance(attr, collections.Sequence) and not isinstance(attr, (str, unicode)):
                    data.append([cls._serialize_internal(i) for i in attr])
                elif isinstance(attr, collections.Mapping):
                    data.append({k: cls._serialize_internal(v) for k, v in attr.iteritems()})
                else:
                    data.append(attr)
        return data

    @classmethod
    def _serialize(cls, msg):
        return msgpack.packb(cls._serialize_internal(msg))

    @classmethod
    def encode(cls, msg):
        # TODO: encryption
        # TODO: compression
        raw = cls._serialize(msg)
        crc = binascii.crc32(raw)
        flags = 0
        length = len(raw) + HEADER_LENGTH
        header = struct.pack(HEADER_FORMAT, length, flags, crc)  # pack header in network B/O
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
        "node",  # node name
        "incarnation",
        "from"
    ]


class DeadMessage(Message):
    _fields_ = [
        "node",  # node name
        "incarnation",
        "from"
    ]


class AliveMessage(Message):
    _fields_ = [
        "node",  # node name
        "address",
        "port",
        "protocol",
        "incarnation"
    ]


class RefuteMessage(Message):
    pass


class RemoteNodeState(BaseMessage):
    _fields_ = [
        "node",
        "address",
        "port",
        "protocol",
        "incarnation",
        "status"
    ]


class SyncMessage(Message):
    _fields_ = [
        "nodes"
    ]

    def __init__(self, nodes):
        super(SyncMessage, self).__init__(nodes)
