import itertools

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
