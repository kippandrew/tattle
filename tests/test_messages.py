import unittest

from tattle import messages
from tattle import state


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


class MessageEncoderTestCase(unittest.TestCase):
    pass


class MessageDecoderTestCase(unittest.TestCase):
    def test_encode(self):
        orig = messages.PingMessage(1, "test")
        buf = messages.MessageEncoder.encode(orig)
        self.assertEqual(orig, messages.MessageDecoder.decode(buf))

    def test_encode_list(self):
        orig = messages.SyncMessage(nodes=[messages.RemoteNodeState('test-node', '127.0.0.1', 12345)])
        buf = messages.MessageEncoder.encode(orig)
        new = messages.MessageDecoder.decode(buf)
        self.assertEqual(orig, new)

