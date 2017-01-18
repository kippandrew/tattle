import functools

from tornado import gen
from tornado import stack_context
from tornado import testing

import tattle.logging


class AbstractIntegrationTest(testing.AsyncTestCase):
    def setUp(self):
        super(AbstractIntegrationTest, self).setUp()
        self.contexts = {}
        self.last_node = 1

    def _create_node_context(self, node_name):
        ctx = stack_context.StackContext(tattle.logging.LogContext(node_name))
        self.contexts[node_name] = ctx
        return ctx

    def _run_with_context(self, ctx, func, *args, **kwargs):
        return stack_context.run_with_stack_context(ctx, functools.partial(func, *args, **kwargs))

    def run_with_node_context(self, node, func, *args, **kwargs):
        return self._run_with_context(self.contexts[node.config.node_name], func, *args, **kwargs)

    def configure_node(self):
        cfg = tattle.Configuration()
        cfg.node_name = 'node-%d' % self.last_node
        cfg.bind_port = 7900 + self.last_node
        self.last_node += 1
        return cfg

    @gen.coroutine
    def create_node(self, config):
        node = tattle.Cluster(config)
        yield node.start()
        raise gen.Return(node)

    @gen.coroutine
    def start_node(self, config=None):
        config = config or self.configure_node()
        ctx = self._create_node_context(config.node_name)
        node = yield self._run_with_context(ctx, self.create_node, config)
        self.addCleanup(lambda: node.stop())
        raise gen.Return(node)

    @gen.coroutine
    def join_node(self, node, *other_nodes):
        yield node.join([(n.local_node_address, n.local_node_port) for n in other_nodes])

    @gen.coroutine
    def stop_node(self, node):
        yield node.stop()

    def assertNodesConverged(self, nodes):
        if any(len(n.members) < len(nodes) for n in nodes):
            self.fail("Failed to converge nodes:\n%s" + '\n'.join([repr(n.members) for n in nodes]))
