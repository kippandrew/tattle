import itertools

import math
import random

import six


class Sequence(object):
    def __init__(self, start=1, step=1):
        self._seq = itertools.count(start, step)

    def increment(self):
        return six.next(self._seq)


def partition(fun, iterable):
    trues = list()
    falses = list()
    for i in iterable:
        if fun(i):
            trues.append(i)
        else:
            falses.append(i)
    return trues, falses


def calculate_transmit_limit(n, m):
    scale = math.ceil(math.log10(n + 1))
    return scale * m


def swap_random_nodes(nodes):
    random_index = random.randint(0, len(nodes) - 1)
    random_node = nodes[random_index]
    last_node = nodes[len(nodes) - 1]
    nodes[random_index] = last_node
    nodes[len(nodes) - 1] = random_node
    return nodes


def select_random_nodes(k, nodes, predicate=None):
    selected = []

    k = min(k, len(nodes))
    c = 0

    while len(selected) < k and c <= (3 * len(nodes)):
        c += 1
        node = random.choice(nodes)
        if node in selected:
            continue

        if predicate is not None:
            if not predicate(node):
                continue

        selected.append(node)

    return selected
