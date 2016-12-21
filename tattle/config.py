import socket


def get_default_node_name():
    return socket.gethostname()


class Configuration(object):
    node_name = None
    node_address = None
    node_port = None
    bind_address = None
    bind_port = None
    api_address = None
    api_port = None
    probe_interval = None
    probe_timeout = None
    gossip_interval = None
    gossip_nodes = None
    sync_interval = None

    def __init__(self):
        if self.node_name is None:
            self.node_name = get_default_node_name()


class DefaultConfiguration(Configuration):
    bind_address = '127.0.0.1'
    bind_port = 7878
    api_address = '127.0.0.1'
    api_port = 7800
    probe_interval = 1000
    probe_timeout = 200
    gossip_interval = 100
    gossip_nodes = 3
    sync_interval = 15000


def init_config():
    return DefaultConfiguration()

