import sys
import asyncio

import tattle
import tattle.logging

last_node = 1


def configure_node():
    global last_node
    cfg = tattle.DefaultConfiguration()
    cfg.node_name = 'node-%d' % last_node
    cfg.bind_port = 7900 + last_node
    last_node += 1
    return cfg


async def start_node():
    config = configure_node()
    node = tattle.Cluster(config)
    await node.start()
    return node


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
    node1 = await start_node()
    node2 = await start_node()
    node3 = await start_node()
    # node4 = await start_node()
    # node5 = await start_node()
    # node6 = await start_node()
    # node7 = await start_node()

    await node1.join([(node2.local_node_address, node2.local_node_port)])
    await asyncio.sleep(1)

    await node3.join([(node2.local_node_address, node2.local_node_port)])
    await asyncio.sleep(1)

    timeout = 30
    try:
        await asyncio.wait_for(wait_until_converged([node1, node2, node3]), timeout)
    except asyncio.TimeoutError:
        print("Failed to converge after {} seconds".format(timeout), file=sys.stderr)

    finally:
        print(node1.config.node_name, node1.members)
        print(node2.config.node_name, node2.members)
        print(node3.config.node_name, node3.members)

        return await shutdown()


# init logging
logger = tattle.logging.init_logger(tattle.logging.TRACE)

# lets go!
asyncio.ensure_future(run())
asyncio.get_event_loop().run_forever()
