import os
import unittest

from tattle import messages


class MessageEqualityTestCase(unittest.TestCase):
    def test_message_equals(self):
        msg1 = messages.PingMessage(1, 'test')
        msg2 = messages.PingMessage(1, 'test')

        self.assertEqual(msg1, msg2)

    def test_message_not_equals(self):
        msg1 = messages.PingMessage(1, 'test')
        msg2 = messages.PingMessage(2, 'test')

        self.assertNotEqual(msg1, msg2)

    def test_message_hash(self):
        msg1 = messages.PingMessage(1, 'test')
        msg2 = messages.PingMessage(2, 'test')
        msg3 = messages.PingMessage(2, 'test')

        self.assertEqual(len({msg1, msg2, msg3}), 2)


class MessagePackerTestCase(unittest.TestCase):
    def test_encode(self):
        orig = messages.PingMessage(1, "test")
        buf = messages.MessageSerializer.encode(orig)
        self.assertEqual(orig, messages.MessageSerializer.decode(buf))

    def test_encode_list(self):
        state = [messages.RemoteNodeState('test', messages.InternetAddress('127.0.0.0', 12345), 1, 1, 'alive', None)]
        orig = messages.SyncMessage(remote_state=state)
        buf = messages.MessageSerializer.encode(orig)
        new = messages.MessageSerializer.decode(buf)
        self.assertEqual(orig, new)

    def test_encode_complex(self):

        orig = messages.PingRequestMessage(1, "node", messages.InternetAddress('host', 123), "sender")
        buf = messages.MessageSerializer.encode(orig)
        self.assertEqual(orig, messages.MessageSerializer.decode(buf))

    def test_encode_encryption(self):

        key = os.urandom(16)

        orig = messages.PingMessage(1, "test")
        buf = messages.MessageSerializer.encode(orig, encryption=key)
        self.assertEqual(orig, messages.MessageSerializer.decode(buf, encryption=[key]))
