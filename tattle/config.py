import socket


def _default_node_name():
    return socket.gethostname()


class Configuration(object):
    def __init__(self, **settings):
        """
        Initialize instance of Configuration class
        """

        self.node_name = _default_node_name()
        """
        Name of node. Should be unique within the cluster.
        """

        self.bind_address = '0.0.0.0'
        """
        Node listening address.
        """

        self.bind_port = 7900
        """
        Node listening port.
        """

        self.node_address = None
        """
        Address that will advertised to other nodes. Useful for NAT.
        """

        self.node_port = None
        """
        Port that will advertised to other nodes. Useful for NAT.
        """

        self.api_address = '127.0.0.1'
        """
        API listening address.
        """

        self.api_port = 7800
        """
        API listening port.
        """

        self.retransmit_multi = 3
        """
        Multiplier for the number of retransmissions of a gossip message.
        The number of retransmits is calculated as retransmit_multi * log(N+1), where N is the number of nodes
        in the cluster.
        """

        self.probe_interval = 0.1
        """
        Probe interval in seconds.
        """

        self.probe_timeout = 0.5
        """
        Probe timeout in seconds.
        """

        self.probe_indirect_nodes = 3
        """
        Number of nodes to send indirect probes.
        """

        self.sync_interval = 10
        """
        Sync interval in seconds.
        """

        self.sync_nodes = 1
        """
        Number of nodes to sync.
        """

        self.suspicion_min_timeout_multi = 5
        self.suspicion_max_timeout_multi = 6

        self.__dict__.update(**settings)


def init_config():
    return Configuration()
