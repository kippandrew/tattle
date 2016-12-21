import unittest

from tattle import message


class MessageEqualityTestCase(unittest.TestCase):
    def test_message_equals(self):
        msg1 = message.PingMessage.create(1, 'test')
        msg2 = message.PingMessage.create(1, 'test')

        self.assertEqual(msg1, msg2)

    def test_message_not_equals(self):
        msg1 = message.PingMessage.create(1, 'test')
        msg2 = message.PingMessage.create(2, 'test')

        self.assertNotEqual(msg1, msg2)

    def test_message_hash(self):
        msg1 = message.PingMessage.create(1, 'test')
        msg2 = message.PingMessage.create(2, 'test')
        msg3 = message.PingMessage.create(2, 'test')

        self.assertEqual(len({msg1, msg2, msg3}), 2)


class MessageSerializationTestCase(unittest.TestCase):
    def test_serialize_ping_message(self):
        original = message.PingMessage.create(1, 'test')

        raw1 = message.serialize(original)

        decoded = message.deserialize(raw1)

        self.assertEqual(decoded.seq, 1)
        self.assertEqual(decoded.node, 'test')

        raw2 = message.serialize(decoded)

        self.assertEqual(raw1, raw2)
