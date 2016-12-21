from tornado import locks
from tornado import gen


class FakeUDPListener(object):
    def __init__(self):
        self.condition = locks.Condition()
        self.message = None

    @gen.coroutine
    def recvfrom(self, max_bytes, timeout):
        yield self.condition.wait()
        print("done", self.message)
        raise gen.Return(self.message)

    def send(self, msg, addr=None):
        self.message = (msg, addr)
        self.condition.notify()
        print("notify", self.message)


class FakeTCPListener(object):
    pass
