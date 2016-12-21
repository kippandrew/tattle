from tattle import logging

LOG = logging.get_logger(__name__)


class Queue(object):
    def push(self, message, callback=None):
        pass


class MessageBroadcaster(object):
    pass
