Tattle
------

Tattle is a Python 3 implementation of the Scalable Weakly-consistent Infection-style Process Group Membership
(SWIM) gossip protocol for managing cluster membership in a distributed Python application.


Quick Start::

    import asyncio
    import tattle

    config = tattle.DefaultConfiguration()
    node = tattle.Cluster(config)

    asyncio.ensure_future(node.start())

