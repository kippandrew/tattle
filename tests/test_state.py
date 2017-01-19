import asynctest
import unittest
import unittest.mock

from tattle import config
from tattle import state
from tattle import messages


# noinspection PyTypeChecker
class NodeManagerTestCase(asynctest.TestCase):
    async def setUp(self):
        super(NodeManagerTestCase, self).setUp()
        self.queue = unittest.mock.Mock()
        self.nodes = state.NodeManager(config.Configuration(), self.queue, self.loop)

        # create a local node
        await self.nodes.set_local_node('local-node', '127.0.0.1', 7800)

        # create other nodes
        for n in range(4):
            await self.nodes.on_node_alive('node-{}'.format(n + 1), 1, '127.0.0.1', 7800 + n + 1)

        self.queue.reset_mock()

    async def test_alive_local_node(self):
        await self.nodes.on_node_alive('local-node', 1, '127.0.0.1', 7801)
        self.queue.push.assert_not_called()

    async def test_alive_node(self):
        await self.nodes.on_node_alive('node-1', 2, '127.0.0.1', 7801)

        expected_node = self.nodes.get('node-1')
        self.assertIsNotNone(expected_node)
        self.assertEqual(expected_node.incarnation, 2)
        self.assertEqual(expected_node.status, state.NODE_STATUS_ALIVE)

        expected_message = messages.AliveMessage('node-1', messages.InternetAddress('127.0.0.1', 7801), 2)

        self.queue.push.assert_called_with('node-1', messages.MessageSerializer.encode(expected_message))

    async def test_suspect_node(self):
        await self.nodes.on_node_suspect('node-1', 1)

        expected_node = self.nodes.get('node-1')
        self.assertIsNotNone(expected_node)
        self.assertEqual(expected_node.status, state.NODE_STATUS_SUSPECT)

        expected_message = messages.SuspectMessage('node-1', 1, 'local-node')
        self.queue.push.assert_called_with('node-1', messages.MessageSerializer.encode(expected_message))

    async def test_suspect_node_refute(self):
        await self.nodes.on_node_suspect('local-node', 1)

        local_node = self.nodes.local_node
        self.assertEqual(local_node.incarnation, 2)
        self.assertEqual(local_node.status, state.NODE_STATUS_ALIVE)

        # suspecting the local node should be refuted (sand Alive)
        expected_message = messages.AliveMessage('local-node', messages.InternetAddress('127.0.0.1', 7800), 2)
        self.queue.push.assert_called_with('local-node', messages.MessageSerializer.encode(expected_message))

    async def test_dead_node(self):
        await self.nodes.on_node_dead('node-1', 1)

        expected_node = self.nodes.get('node-1')
        self.assertIsNotNone(expected_node)
        self.assertEqual(expected_node.status, state.NODE_STATUS_DEAD)

        expected_message = messages.DeadMessage('node-1', 1, 'local-node')

        self.queue.push.assert_called_with('node-1', messages.MessageSerializer.encode(expected_message))

    async def test_alive_cancel_suspect(self):
        await self.nodes.on_node_suspect('node-1', 1)
        self.assertIn('node-1', self.nodes._suspect_nodes)

        await self.nodes.on_node_alive('node-1', 2, '127.0.0.1', 7801)
        self.assertNotIn('node-1', self.nodes._suspect_nodes)
