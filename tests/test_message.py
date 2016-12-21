import unittest

from tattle import message


class MessageSerializationTestCase(unittest.TestCase):

    def test_serialize_message(self):
        original = message.PingMessage.create(1, 'test')

        raw1 = message.serialize(original)

        decoded = message.deserialize(raw1)

        self.assertEqual(decoded.type, message.MESSAGE_TYPE_PING)

        raw2 = message.serialize(decoded)

        self.assertEqual(raw1, raw2)
