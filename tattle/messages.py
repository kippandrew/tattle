import binascii
import collections
import inspect
import struct
import sys

import msgpack

from tattle import crypto
from tattle import logging

__all__ = [
    'MESSAGE_HEADER_LENGTH',
    'MESSAGE_HEADER_FORMAT',
    'MESSAGE_FLAG_ENCRYPT',
    'Message',
    'MessageError',
    'MessageEncodeError',
    'MessageDecodeError',
    'MessageChecksumError',
    'MessageSerializer',
    'InternetAddress',
    'PingMessage',
    'PingRequestMessage',
    'AckMessage',
    'NackMessage',
    'SuspectMessage',
    'DeadMessage',
    'AliveMessage',
    'RemoteNodeState',
    'SyncMessage',
    'UserMessage',
]

LOG = logging.get_logger(__name__)

MESSAGE_HEADER_LENGTH = 7  # 2 for length, 1 for flags, 4 for CRC32
MESSAGE_HEADER_FORMAT = '!HBL'  # network byte order is B/E

MESSAGE_FLAG_ENCRYPT = 0x80


class MessageError(Exception):
    pass


class MessageEncodeError(MessageError):
    pass


class MessageDecodeError(MessageError):
    pass


class MessageChecksumError(MessageDecodeError):
    pass


class _BaseMessage(object):
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
            if issubclass(base, _BaseMessage):
                # noinspection PyProtectedMember
                for f in base._fields_:
                    if isinstance(f, tuple):
                        fields.append(f)
                    else:
                        fields.append((f, None))
        return fields


class Message(_BaseMessage):
    def __init__(self, *args, **kwargs):
        super(Message, self).__init__(*args, **kwargs)


class MessageSerializer(object):
    """
    Utility class for serializing and deserializing Messages
    """

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
        _BaseMessage.__init__(message, *message_args)
        return message

    @classmethod
    def _deserialize_message(cls, raw):
        return cls._deserialize_internal(msgpack.unpackb(raw, encoding='utf-8', use_list=True))

    @classmethod
    def _decrypt_message(cls, raw, keys):
        return crypto.decrypt_data(raw, keys)

    @classmethod
    def _verify_checksum(cls, raw, crc):
        expected = binascii.crc32(raw) & 0xffffffff  # https://docs.python.org/3/library/binascii.html#binascii.crc32
        if crc != expected:
            raise MessageChecksumError("Message checksum mismatch: 0x%X != 0x%X" % (crc, expected))

    @classmethod
    def decode(cls, buf, encryption=None) -> Message:
        """
        Decode a message from bytes

        :param buf:
        :param encryption: list of encryption keys
        :return: deserialized message
        """

        # unpack message header
        if len(buf) <= MESSAGE_HEADER_LENGTH:
            raise MessageDecodeError("Message is too short")
        (length, flags, crc) = struct.unpack(MESSAGE_HEADER_FORMAT, buf[0:MESSAGE_HEADER_LENGTH])

        # unpack message body
        raw = buf[MESSAGE_HEADER_LENGTH:]

        # verify message checksum
        cls._verify_checksum(raw, crc)

        # handle encryption
        if flags & MESSAGE_FLAG_ENCRYPT == MESSAGE_FLAG_ENCRYPT:
            raw = cls._decrypt_message(raw, keys=encryption)

        # return the deserialized message
        return cls._deserialize_message(raw)

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
    def _serialize_message(cls, msg):
        return msgpack.packb(cls._serialize_internal(msg), use_bin_type=True, encoding='utf-8')

    @classmethod
    def _encrypt_message(cls, raw, key):
        return crypto.encrypt_data(raw, key)

    @classmethod
    def _compute_checksum(cls, raw):
        crc = binascii.crc32(raw) & 0xffffffff  # https://docs.python.org/3/library/binascii.html#binascii.crc32
        return crc

    @classmethod
    def encode(cls, msg: Message, encryption=None) -> bytes:
        """
        Encode a message to bytes

        :param msg:
        :param encryption: encryption key
        :return:
        """

        # serialize the message
        raw = cls._serialize_message(msg)

        flags = 0

        # encrypt message
        if encryption is not None:
            flags |= MESSAGE_FLAG_ENCRYPT
            raw = cls._encrypt_message(raw, key=encryption)

        # calculate message checksum
        crc = cls._compute_checksum(raw)

        # calculate message length
        length = len(raw) + MESSAGE_HEADER_LENGTH

        # pack message header
        header = struct.pack(MESSAGE_HEADER_FORMAT, length, flags, crc)
        return header + raw


class InternetAddress(_BaseMessage):
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

    def __str__(self):
        return "<%s seq=%s>" % (self.__class__.__name__, self.seq)


class NackMessage(Message):
    _fields_ = [
        "seq",
        "sender"
    ]

    def __str__(self):
        return "<%s seq=%s>" % (self.__class__.__name__, self.seq)


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


class RemoteNodeState(_BaseMessage):
    _fields_ = [
        "node",
        ("addr", InternetAddress),
        "version",
        "incarnation",
        "status",
        "metadata",
    ]

    def __init__(self, node, addr, version, incarnation, status, metadata):
        super(RemoteNodeState, self).__init__(node, addr, version, incarnation, status, metadata)


class SyncMessage(Message):
    _fields_ = [
        "nodes"
    ]

    def __init__(self, remote_state):
        super(SyncMessage, self).__init__(remote_state)


class UserMessage(Message):
    _fields_ = [
        "data"
        "sender"
    ]

    def __init__(self, data, sender):
        """
        Create a new instance of the UserMessage class

        :param data: user message
        :param sender: sender node name
        """
        super(UserMessage, self).__init__(data, sender)
