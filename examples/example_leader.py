#!/usr/bin/env python3

import asyncio
import argparse
import tattle
import tattle.logging

quorum = 2

leader = None

async def choose_leader(_):
    global leader
    # the leader is always the first node in the sorted list of members
    quorum_nodes = sorted([n for n in cluster.members if n.status != tattle.NODE_STATUS_DEAD], key=lambda n: n.name)
    if len(quorum_nodes) >= quorum:
        new_leader = quorum_nodes[0].name
        if new_leader != leader:
            leader = new_leader
            if leader == cluster.local_node_name:
                log.info("I am the leader!")
            else:
                log.info("The leader is %s", new_leader)

    else:
        log.warn("Unable to establish quorum")


async def run():
    await cluster.start()

    if args.join is not None:
        await cluster.join(tattle.network.parse_address(args.join))


# parse arguments
parser = argparse.ArgumentParser()
parser.add_argument('port', type=int)
parser.add_argument('--join', '-j', metavar='NODE', type=str)
parser.add_argument('--debug', '-d', action='store_const', const=True)
args = parser.parse_args()

# init cluster
config = tattle.Configuration()
config.node_name = 'node-%d' % args.port
config.bind_port = args.port
cluster = tattle.Cluster(config)
cluster.subscribe('node.alive', choose_leader)
cluster.subscribe('node.dead', choose_leader)

# init logging
log = tattle.logging.init_logger(level=tattle.logging.DEBUG if args.debug else tattle.logging.INFO)

# lets go!
asyncio.get_event_loop().run_until_complete(run())
asyncio.get_event_loop().run_forever()
