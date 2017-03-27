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

`Python <http://www.python.org/>`_

Tattle is a Python 3.5+ implementation of the `Scalable Weakly-consistent Infection-style Process Group Membership <docs/swim.pdf>`_
(SWIM) gossip protocol for managing cluster membership in a distributed Python application. The tattle library is
designed to help developers create distributed applications in Python by providing a framework for managing
cluster membership, disseminating member status, and detecting member failures. In addition to managing
cluster membership tattle can be used to disseminate arbitrary messages throughout the cluster.

Usage (node.py)::

    import sys
    import asyncio
    import tattle

    async def __main__():

        # parse arguments
        port = sys.argv[1]
        join = sys.argv[2] if len(sys.argv) > 2 else None

        # start node
        node = await tattle.Cluster(tattle.Configuration(bind_port=port)).start()

        # join cluster
        if join is not None:
            await node.join(tattle.parse_address(join))

    asyncio.ensure_future(__main__())

Running the Example::

    python3 node.py 7901 &
    python3 node.py 7902 localhost:7901 &
    python3 node.py 7903 localhost:7901 &
