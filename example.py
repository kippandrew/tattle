import asyncio
import random
import threading
import sys

import tattle
import tattle.logging

last_node = 1

all_nodes = set()
all_threads = dict()


def configure_node():
    global last_node
    cfg = tattle.DefaultConfiguration()
    cfg.node_name = 'node-%d' % last_node
    cfg.bind_port = 7900 + last_node
    last_node += 1
    return cfg


def start_node():
    config = configure_node()
    loop = asyncio.new_event_loop()
    node = tattle.Cluster(config, loop=loop)
    thread = NodeThread(node, join_nodes=[random.choice(list(all_nodes))] if all_nodes else None, loop=loop)
    thread.start()
    all_nodes.add(node)
    all_threads[node] = thread
    return node, thread


class NodeThread(threading.Thread):
    def __init__(self, node, join_nodes=None, loop=None):
        self.node = node
        self.join_nodes = join_nodes or []
        self.loop = loop
        self.future = loop.create_future()
        super().__init__(name=node.local_node_name, daemon=True)

    async def _start_node(self):
        await self.node.start()
        if self.join_nodes is not None:
            await self.node.join(*[(n.local_node_address, n.local_node_port) for n in self.join_nodes])

    async def _stop_node(self):
        await self.node.stop()

    def run(self):
        try:
            self.loop.run_until_complete(self._start_node())

            try:
                self.loop.run_until_complete(self.future)
            except asyncio.CancelledError:
                pass

            self.loop.run_until_complete(self._stop_node())
        finally:
            self.loop.stop()

    def stop(self):
        self.loop.call_soon(lambda: self.future.cancel())


def wait_until_converged(expected_nodes=None):
    if expected_nodes is None:
        expected_nodes = all_nodes

    future = asyncio.Future()

    status = lambda n: [(m.name, m.status) for m in n.members]

    def _check_converged():
        for e in expected_nodes:
            if any(status(n) != status(e) for n in expected_nodes):
                asyncio.get_event_loop().call_later(0.1, _check_converged)
                return
        future.set_result(True)

    _check_converged()
    return future


def dump_nodes():
    for n in all_nodes:
        print(n.local_node_name, n.members)


def stop_nodes():
    for n in all_nodes:
        all_threads[n].stop()


async def run():
    node1, node1_thread = start_node()
    node2, node2_thread = start_node()
    node3, node3_thread = start_node()
    node4, node4_thread = start_node()
    node5, node5_thread = start_node()

    timeout = 5
    try:
        await asyncio.wait_for(wait_until_converged(), timeout)
    except asyncio.TimeoutError:
        print("Failed to converge after {} seconds".format(timeout), file=sys.stderr)
        # sys.exit(1)
    finally:
        dump_nodes()

    # stop node3
    node3_thread.stop()
    await asyncio.sleep(5)

    timeout = 5
    try:
        # expect all nodes to agree
        await asyncio.wait_for(wait_until_converged(all_nodes - {node3}), timeout)
    except asyncio.TimeoutError:
        print("Failed to converge after {} seconds".format(timeout), file=sys.stderr)
        # sys.exit(1)
    finally:
        dump_nodes()

    # stop_nodes()


# init logging
logger = tattle.logging.init_logger(tattle.logging.TRACE)

# lets go!
asyncio.get_event_loop().run_until_complete(run())
