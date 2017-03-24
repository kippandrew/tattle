Tattle
------

.. image:: https://travis-ci.org/kippandrew/tattle.svg?branch=master
   :target: https://travis-ci.org/kippandrew/tattle
   :alt: Travis CI

.. image:: https://codeclimate.com/github/kippandrew/tattle/badges/gpa.svg
   :target: https://codeclimate.com/github/kippandrew/tattle
   :alt: Code Climate

.. image:: https://codeclimate.com/github/kippandrew/tattle/badges/coverage.svg
   :target: https://codeclimate.com/github/kippandrew/tattle/coverage
   :alt: Test Coverage

Tattle is a Python 3.5+ implementation of the Scalable Weakly-consistent Infection-style Process Group Membership
(SWIM) gossip protocol for managing cluster membership in a distributed Python application. The tattle library is
designed to help developers create distributed applications in Python by providing a framework for managing
cluster membership, disseminating member status, and detecting member failures. In addition to managing
cluster membership tattle can be used to disseminate arbitrary messages throughout the cluster.

Tattle can be used as both a library or as standalone process. When using tattle as a standalone process,
a REST-ful API is provided to manage the cluster.

Example Code (node.py)::

    #!/usr/bin/env python3.5
    import sys
    import asyncio
    import tattle

    async def start_node():
        node tattle.Cluster(tattle.Configuration(bind_port=port)).start()
        await node.start()

        if join is not None:
            await node.join(tattle.parse_address(join))
        return node

    port = sys.argv[1]
    join = sys.argv[2] if len(sys.argv) > 2 else None

    asyncio.ensure_future(start_node(int(port), join)

Running the Example::

    python node.py 7901 &
    python node.py 7902 localhost:7901 &
    python node.py 7903 localhost:7901 &
