import functools
import logging

from tornado import gen
from tornado import ioloop
from tornado import stack_context

import tattle
import tattle.logging

last_node = 1
contexts = dict()


def create_node_context(node_name):
    ctx = stack_context.StackContext(tattle.logging.LogPrefixContext(node_name))
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


@gen.coroutine
def create_node(config):
    node = tattle.Cluster(config)
    yield node.run()
    raise gen.Return(node)


@gen.coroutine
def join_node(node, other_nodes):
    yield node.join([(n.local_node_address, n.local_node_port) for n in other_nodes])


@gen.coroutine
def run_node():
    node_config = configure_node()
    node_ctx = create_node_context(node_config.node_name)
    node = yield run_with_context(node_ctx, create_node, node_config)
    raise gen.Return(node)


@gen.coroutine
def run():
    node1 = yield run_node()
    node2 = yield run_node()
    node3 = yield run_node()

    run_with_node_context(node1, join_node, node1, [node2, node3])


# init logging
logger = tattle.logging.init_logger(logging.DEBUG)

# run cluster
run()

ioloop.IOLoop.instance().start()
