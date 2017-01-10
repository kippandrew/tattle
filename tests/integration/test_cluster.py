import datetime
import random

from tornado import concurrent
from tornado import gen
from tornado import ioloop
from tornado import testing

from tattle import logging
from tests import integration

logging.init_logger(logging.TRACE)


class ClusterIntegrationTest(integration.AbstractIntegrationTest):

    def _wait_until_converged(self, nodes):
        future = concurrent.TracebackFuture()

        def _check_converged():
            if any(len(n.members) < len(nodes) for n in nodes):
                ioloop.IOLoop.current().add_timeout(datetime.timedelta(milliseconds=500), _check_converged)
                return
            future.set_result(True)

        _check_converged()
        return future

    @gen.coroutine
    def _execute_cluster_converge_test(self, n):
        nodes = []
        timeout = 5
        try:

            # create n nodes
            for i in range(n):
                node = yield self.start_node()
                nodes.append(node)

                # join node to a random other node
                yield self.join_node(node, random.choice(nodes))

                yield gen.sleep(.5)

            yield gen.with_timeout(datetime.timedelta(seconds=timeout), self._wait_until_converged(nodes))
        except gen.TimeoutError:
            self.fail("Failed to converge in {} seconds".format(timeout))

        self.assertNodesConverged(nodes)

    @testing.gen_test(timeout=10)
    def test_join_3(self):
        yield self._execute_cluster_converge_test(3)

    @testing.gen_test(timeout=10)
    def test_join_4(self):
        yield self._execute_cluster_converge_test(4)

    @testing.gen_test(timeout=10)
    def test_join_6(self):
        yield self._execute_cluster_converge_test(6)

    @testing.gen_test(timeout=10)
    def test_join_8(self):
        yield self._execute_cluster_converge_test(8)

    @testing.gen_test(timeout=10)
    def test_join_9(self):
        yield self._execute_cluster_converge_test(8)

