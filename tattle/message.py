import collections
import inspect

import msgpack
import sys

from tattle import logging

LOG = logging.get_logger(__name__)


class MessageError(Exception):
    pass


class MessageChecksumError(MessageError):
    pass


class MessageBase(object):
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
            if issubclass(base, MessageBase):
                # noinspection PyProtectedMember
                for f in base._fields_:
                    if isinstance(f, tuple):
                        fields.append(f)
                    else:
                        fields.append((f, None))
        return fields


class Message(MessageBase):
    def __init__(self, *args, **kwargs):
        super(Message, self).__init__(*args, **kwargs)


class PingMessage(Message):
    _fields_ = [
        "seq",
        "node"
    ]

    @classmethod
    def create(cls, seq, node):
        """
        Create a PingMessage
        :type seq: int
        :type node: str
        """
        return cls(seq, node)


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

    @classmethod
    def create(cls, node, incarnation, addr, port):
        """
        Create an AliveMessage
        """
        return cls(node, incarnation, addr, port)


class RefuteMessage(Message):
    pass


def _deserialize_internal(data):
    # get class
    cls = getattr(sys.modules[__name__], data.pop(0))

    # get args
    args = data

    # get a list of fields
    fields = cls.get_fields()

    # deserialize any arguments first
    for field_name, field_type in fields:
        if field_type is not None:
            args.insert(0, _deserialize_internal(data))

    # get augments to pass to constructor
    args = [data.pop(0) for _ in range(len(fields))]

    # shenanigans to initialize Message without calling constructor
    obj = cls.__new__(cls, *args)
    MessageBase.__init__(obj, *args)
    return obj


def deserialize(raw):
    message = _deserialize_internal(msgpack.unpackb(raw, use_list=True))
    return message


def _serialize_internal(message):
    # insert the name of the class
    data = [message.__class__.__name__]
    # get list of fields
    fields = message.__class__.get_fields()
    for field_name, field_type in fields:
        attr = getattr(message, field_name)
        if field_type is not None:
            data.extend(_serialize_internal(attr))
        else:
            data.append(attr)
    return data


def serialize(message):
    return msgpack.packb(_serialize_internal(message))
