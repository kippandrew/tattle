import itertools


class Sequence(object):
    def __init__(self, start=1, step=1):
        self._seq = itertools.count(start, step)

    def increment(self):
        return next(self._seq)
