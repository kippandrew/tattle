Gossip Protocol
---------------

Tattle uses a gossip protocol to manage cluster membership and disseminate information throughout the cluster.
Tattle's gossip protocol is based on the Scalable Weakly-consistent Infection-style Process Group Membership (SWIM)
gossip protocol designed at Cornell University by Abhinandan Das, Indranil Gupta and Ashish Motivala.

SWIM Protocol Overview
======================

Traditional cluster membership protocols suffer from scalability problems when a large number of members exist in the
cluster. Network resources can quickly become congested, or failure detection can be unpredictable. The SWIM protocol
was designed to allow greater scalability in distributed applications by creating a membership subsystem that provides
stable failure detection time, stable rate of false positives and low message load per cluster member, thus allowing
distributed applications that use it to scale well.

SWIM is comprised of three major components: the message dissemination component, the failure detection component, and
the suspicion mechanism.

The failure detection component works by periodically sending pings to a random node in the cluster. If node fails
respond in a configured amount of time one or more indirect pings sent via another node in the cluster.

The message dissemination component works by "piggybacking" membership status updates on top of the failure detection
component. When a PING or and ACK message is sent, membership updates are included. This reduces network throughput
by eliminating the need to propagate messages to all nodes in the cluster.

The suspicion mechanism is triggered when a node fails to respond to a ping. Rather then marking the node as dead
immediately, a timer is started giving the suspect node a chance to refute the status.

This is a brief overview of the SWIM protocol. The full details of the SWIM protocol can be found
:download:`here<swim.pdf>`.

SWIM Protocol Modifications
===========================

**Periodic State Sync**
  Members of a cluster will periodically perform a full membership state sync with another
  random node. This allows for faster convergence while introducing little overhead.

**Failure Detector Backoff**

**Dynamic Suspicion Timeout**
