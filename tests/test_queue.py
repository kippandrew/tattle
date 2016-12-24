import unittest

from tattle import queue


class MessageQueueTestCase(unittest.TestCase):
    def test_queue(self):
        mq = queue.MessageQueue(10)

        mq.push('node1', b'node1 message')
        mq.push('node2', b'node2 message')

        self.assertEqual(len(mq), 2)

        mq.push('node1', b'node1 updated')
        mq.push('node3', b'node3 message')

        self.assertEqual(len(mq), 3)

        msg = mq.pop(3)
        self.assertEqual(msg, b'node3 message')

        msg = mq.pop(3)
        self.assertEqual(msg, b'node1 updated')

        msg = mq.pop(3)
        self.assertEqual(msg, b'node2 message')

        mq.pop(3) # pop node-3
        mq.pop(3) # pop node-1
        mq.pop(3) # pop node-2
        mq.pop(3) # pop node-3
        mq.pop(3) # pop node-1
        mq.pop(3) # pop node-2

        msg = mq.pop(3)
        self.assertIsNone(msg)
