import asyncio
import random
import threading
import sys

import requests

import tattle
import tattle.logging

last_node = 1

all_nodes = set()
all_threads = dict()


class NodeThread(threading.Thread):
    def __init__(self, node, loop=None):
        self.node = node
        self.loop = loop
        self.api = None
        self.server = None
        self.handler = None
        self.future = loop.create_future()
        super().__init__(name=node.local_node_name, daemon=True)

    def _start_api(self):
        self.api = tattle.APIServer(self.node, loop=self.loop)
        handler, server = tattle.start_server(self.api, self.node.config.api_port, self.node.config.api_address)
        self.handler = handler
        self.server = server

    def _stop_api(self):
        tattle.stop_server(self.api, self.server, self.handler)

    async def _start_node(self):
        await self.node.start()

    async def _stop_node(self):
        await self.node.stop()

    def run(self):
        try:
            self._start_api()

            self.loop.run_until_complete(self._start_node())

            try:
                self.loop.run_until_complete(self.future)
            except asyncio.CancelledError:
                pass

            self.loop.run_until_complete(self._stop_node())

        finally:
            self._stop_api()
            self.loop.stop()

    def die(self):
        self.loop.call_soon(lambda: self.future.cancel())


class NodeClient:
    def __init__(self, host='127.0.0.1', port=7800):
        self.host = host
        self.port = port

    def _url(self, path):
        return "http://{host}:{port}{path}".format(host=self.host, port=self.port, path=path)

    def join(self, *nodes):
        return requests.post(self._url('/cluster/join'), json=[{'host': n[0], 'port': n[1]} for n in nodes])

    def leave(self):
        return requests.post(self._url('/cluster/leave'))

    def members(self):
        return requests.get(self._url('/cluster/members/'))

    def stop(self):
        return requests.post(self._url('/cluster/stop'))

    def start(self):
        return requests.post(self._url('/cluster/start'))


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


def configure_node():
    global last_node
    cfg = tattle.DefaultConfiguration()
    cfg.node_name = 'node-%d' % last_node
    cfg.bind_port = 7900 + last_node
    cfg.api_port = 7800 + last_node
    last_node += 1
    return cfg


def start_node():
    config = configure_node()
    loop = asyncio.new_event_loop()
    node = tattle.Cluster(config, loop=loop)
    thread = NodeThread(node, loop=loop)
    thread.start()

    other_node = random.choice(list(all_nodes)) if all_nodes else None

    all_nodes.add(node)
    all_threads[node] = thread
    client = NodeClient(port=config.api_port)

    if other_node:
        client.join((other_node.local_node_address, other_node.local_node_port))

    return client


def dump_nodes():
    for n in all_nodes:
        print(n.local_node_name, n.members)


def stop_nodes():
    for n in all_nodes:
        all_threads[n].die()


async def run():
    node1 = start_node()
    node2 = start_node()
    node3 = start_node()
    node4 = start_node()
    node5 = start_node()

    timeout = 5
    try:
        await asyncio.wait_for(wait_until_converged(), timeout)
    except asyncio.TimeoutError:
        print("Failed to converge after {} seconds".format(timeout), file=sys.stderr)
        return
    finally:
        dump_nodes()

    # stop node3
    node3.stop()
    await asyncio.sleep(5)

    timeout = 5
    try:
        await asyncio.wait_for(wait_until_converged(), timeout)
    except asyncio.TimeoutError:
        print("Failed to converge after {} seconds".format(timeout), file=sys.stderr)
    finally:
        dump_nodes()


# init logging
logger = tattle.logging.init_logger(tattle.logging.DEBUG)

# lets go!
asyncio.get_event_loop().run_until_complete(run())
