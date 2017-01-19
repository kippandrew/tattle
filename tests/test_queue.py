import unittest

from tattle import queue


class BroadcastQueueTestCase(unittest.TestCase):
    def test_push(self):
        q = queue.BroadcastQueue()

        q.push('node1', b'node1 message')
        q.push('node2', b'node2 message')

        self.assertEqual(len(q), 2)

        q.push('node1', b'node1 updated')
        q.push('node3', b'node3 message')

        self.assertEqual(len(q), 3)

    def test_pop(self):
        q = queue.BroadcastQueue()
        q.push('node1', b'node1 message')
        q.push('node2', b'node2 message')
        q.push('node1', b'node1 updated')
        q.push('node3', b'node3 message')

        msg = q.pop(3)
        self.assertEqual(msg, b'node3 message')

        msg = q.pop(3)
        self.assertEqual(msg, b'node1 updated')

        msg = q.pop(3)
        self.assertEqual(msg, b'node2 message')

        q.pop(3)  # pop node-3
        q.pop(3)  # pop node-1
        q.pop(3)  # pop node-2
        q.pop(3)  # pop node-3
        q.pop(3)  # pop node-1
        q.pop(3)  # pop node-2

        msg = q.pop(3)
        self.assertIsNone(msg)

    def test_fetch(self):

        q = queue.BroadcastQueue()
        q.push('node1', b'node1 message')
        q.push('node2', b'node2 message')
        q.push('node1', b'node1 updated')
        q.push('node3', b'node3 message')

        messages = q.fetch(3, 30)
        self.assertEqual(messages, [b'node3 message', b'node1 updated'])

        messages = q.fetch(3, 40)
        self.assertEqual(messages, [b'node2 message', b'node3 message', b'node1 updated'])

        messages = q.fetch(3, 40)
        self.assertEqual(messages, [b'node2 message', b'node3 message', b'node1 updated'])

        messages = q.fetch(3, 40)
        self.assertEqual(messages, [b'node2 message'])
