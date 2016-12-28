import socket


def _default_node_name():
    return socket.gethostname()


class Configuration(object):
    def __init__(self):
        self.node_name = None
        self.node_address = None
        self.node_port = None
        self.bind_address = None
        self.bind_port = None
        self.api_address = None
        self.api_port = None
        self.probe_interval = None
        self.probe_timeout = None
        self.probe_indirect_nodes = 3
        self.sync_interval = None
        self.sync_nodes = 1
        self.retransmit_multiplier = None
        self.suspicion_multiplier = None


class DefaultConfiguration(Configuration):
    def __init__(self):
        super(DefaultConfiguration, self).__init__()

        self.bind_address = '127.0.0.1'
        self.bind_port = 7900
        self.api_address = '127.0.0.1'
        self.api_port = 7800
        self.retransmit_multiplier = 3
        self.probe_interval = 100
        self.probe_timeout = 200
        self.probe_indirect_nodes = 1
        self.sync_interval = 10000

        if self.node_name is None:
            self.node_name = _default_node_name()


def init_config():
    return DefaultConfiguration()
