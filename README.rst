.. image:: https://travis-ci.org/kippandrew/tattle.svg?branch=master
   :target: https://travis-ci.org/kippandrew/tattle
   :alt: Travis CI

.. image:: https://codeclimate.com/github/kippandrew/tattle/badges/gpa.svg
   :target: https://codeclimate.com/github/kippandrew/tattle
   :alt: Code Climate

.. image:: https://codeclimate.com/github/kippandrew/tattle/badges/coverage.svg
   :target: https://codeclimate.com/github/kippandrew/tattle/coverage
   :alt: Test Coverage

Tattle
------

Tattle is a Python 3.5+ implementation of the `Scalable Weakly-consistent Infection-style Process Group Membership <docs/swim.pdf>`_
(SWIM) gossip protocol for managing cluster membership in a distributed Python application. The tattle library is
designed to help developers create distributed applications in Python by providing a framework for managing
cluster membership, disseminating member status, and detecting member failures. In addition to managing
cluster membership tattle can be used to disseminate arbitrary messages throughout the cluster.

.. code-block:: python
    :caption: node.py

    import sys
    import asyncio
    import tattle


    async def run_node():
        # create node
        cluster = await tattle.Cluster(config).start()

        # join cluster
        if join is not None:
            await cluster.join(tattle.parse_address(join))


    # parse arguments
    port = sys.argv[1] if len(sys.argv) > 1 else 7900
    join = sys.argv[2] if len(sys.argv) > 2 else None

    config = tattle.Configuration(name='node-%d' % port, bind_port=port)

    asyncio.get_event_loop().run_until_complete(run_node())
    asyncio.get_event_loop().run_forever()

Running the Example::

    python3 node.py &
    python3 node.py 7901 localhost:7900 &
    python3 node.py 7902 localhost:7900 &
