import itertools

class Sequence(object):

    def __init__(self, start=0, step=1):
        self._seq = itertools.count(start, step)

    def increment(self):
        return self._seq.next()
