import asyncio
import functools
import threading
import sys

import tattle
import tattle.logging

last_node = 1

nodes = []


def configure_node():
    global last_node
    cfg = tattle.DefaultConfiguration()
    cfg.node_name = 'node-%d' % last_node
    cfg.bind_port = 7900 + last_node
    last_node += 1
    return cfg


def start_node(members=None):
    config = configure_node()
    # node = ClusterThread(config, members=members, loop=asyncio.new_event_loop())
    # node.start()
    # return node
    loop = asyncio.new_event_loop()
    node = tattle.Cluster(config, loop=loop)
    nodes.append(node)
    thread = NodeThread(node, loop, members)
    thread.start()
    return node, thread


class NodeThread(threading.Thread):
    def __init__(self, node, loop, members=None):
        self.node = node
        self.loop = loop
        self.members = members
        self.future = loop.create_future()
        super().__init__(name=node.local_node_name, daemon=True)

    async def _start_node(self):
        await self.node.start()
        if self.members is not None:
            await self.node.join(self.members)

    async def _stop_node(self):
        await self.node.stop()

    def run(self):
        self.loop.run_until_complete(self._start_node())

        try:
            self.loop.run_until_complete(self.future)
        except asyncio.CancelledError:
            print("Stopping node: %s" % self.name)

        self.loop.run_until_complete(self._stop_node())

    def stop(self):
        self.loop.call_soon(lambda: self.future.cancel())


def wait_until_converged(nodes):
    future = asyncio.Future()

    def _check_converged():
        if any(len(n.members) < len(nodes) for n in nodes):
            asyncio.get_event_loop().call_later(0.2, _check_converged)
            return
        future.set_result(True)

    _check_converged()
    return future


async def shutdown():
    asyncio.get_event_loop().stop()


async def run():
    node1, node1_thread = start_node()
    node2, node2_thread = start_node([(node1.local_node_address, node1.local_node_port)])
    node3, node3_thread = start_node([(node1.local_node_address, node1.local_node_port)])

    timeout = 30
    try:
        await asyncio.wait_for(wait_until_converged([node1, node2, node3]), timeout)
    except asyncio.TimeoutError:
        print("Failed to converge after {} seconds".format(timeout), file=sys.stderr)

    finally:
        print(node1.config.node_name, node1.members)
        print(node2.config.node_name, node2.members)
        print(node3.config.node_name, node3.members)

    # await asyncio.sleep(1)

    node3_thread.stop()
    # node3.stop()

    # return await shutdown()


# init logging
logger = tattle.logging.init_logger(tattle.logging.TRACE)

# lets go!
asyncio.ensure_future(run())
asyncio.get_event_loop().run_forever()
