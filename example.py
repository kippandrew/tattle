import asyncio
import datetime
import functools

# from tornado import concurrent
# from tornado import gen
# from tornado import ioloop
from tornado import stack_context

import tattle
import tattle.logging

last_node = 1
contexts = dict()


def create_node_context(node_name):
    ctx = stack_context.StackContext(tattle.logging.LogContext(node_name))
    contexts[node_name] = ctx
    return ctx


def run_with_node_context(node, func, *args, **kwargs):
    return run_with_context(contexts[node.config.node_name], func, *args, **kwargs)


def run_with_context(ctx, func, *args, **kwargs):
    return stack_context.run_with_stack_context(ctx, functools.partial(func, *args, **kwargs))


def configure_node():
    global last_node
    cfg = tattle.DefaultConfiguration()
    cfg.node_name = 'node-%d' % last_node
    cfg.bind_port = 7900 + last_node
    last_node += 1
    return cfg


async def create_node(config):
    node = tattle.Cluster(config)
    await node.start()
    return node


async def join_node(node, other_nodes):
    await node.join([(n.local_node_address, n.local_node_port) for n in other_nodes])


async def start_node():
    node_config = configure_node()
    node_ctx = create_node_context(node_config.node_name)
    node = await run_with_context(node_ctx, create_node, node_config)
    return node


# def wait_until_converged(nodes):
#     future = concurrent.TracebackFuture()
#
#     def _check_converged():
#         if any(len(n.members) < len(nodes) for n in nodes):
#             ioloop.IOLoop.current().add_timeout(datetime.timedelta(milliseconds=200), _check_converged)
#             return
#         future.set_result(True)
#
#     _check_converged()
#     return future


async def run():
    node1 = await start_node()
    node2 = await start_node()
    node3 = await start_node()
    node4 = await start_node()
    node5 = await start_node()
    node6 = await start_node()
    node7 = await start_node()

    await run_with_node_context(node1, join_node, node1, [node2])
    await asyncio.sleep(1)

    await run_with_node_context(node3, join_node, node3, [node2])
    await asyncio.sleep(1)

    # await run_with_node_context(node4, join_node, node4, [node1])
    # await run_with_node_context(node5, join_node, node5, [node4])
    # await gen.sleep(1)
    #
    # await run_with_node_context(node6, join_node, node6, [node5])
    # await gen.sleep(1)
    #
    # await run_with_node_context(node7, join_node, node7, [node1])
    # await gen.sleep(1)

    # timeout = 30
    # try:
    #     await gen.with_timeout(datetime.timedelta(seconds=timeout), wait_until_converged([node1,
    #                                                                                       node2,
    #                                                                                       node3,
    #                                                                                       node4,
    #                                                                                       node5,
    #                                                                                       node6,
    #                                                                                       node7]))
    # except gen.TimeoutError:
    #     ioloop.IOLoop.current().stop()
    #
    #     print("Failed to converge after {} seconds".format(timeout), file=sys.stderr)
    #     print(node1.config.node_name, node1.members, file=sys.stderr)
    #     print(node2.config.node_name, node2.members, file=sys.stderr)
    #     print(node3.config.node_name, node3.members, file=sys.stderr)
    #     print(node4.config.node_name, node4.members, file=sys.stderr)
    #     print(node5.config.node_name, node5.members, file=sys.stderr)
    #     print(node6.config.node_name, node6.members, file=sys.stderr)
    #     print(node7.config.node_name, node7.members, file=sys.stderr)
    #
    # else:
    #     ioloop.IOLoop.current().stop()
    #
    #     print(node1.config.node_name, node1.members)
    #     print(node2.config.node_name, node2.members)
    #     print(node3.config.node_name, node3.members)
    #     print(node4.config.node_name, node4.members)
    #     print(node5.config.node_name, node5.members)
    #     print(node6.config.node_name, node6.members)
    #     print(node7.config.node_name, node7.members)


# init logging
logger = tattle.logging.init_logger(tattle.logging.TRACE)

# run cluster
# run()

asyncio.ensure_future(run())
asyncio.get_event_loop().run_forever()

# ioloop.IOLoop.configure('tornado.platform.asyncio.AsyncIOLoop')
# ioloop.IOLoop.instance().start()
