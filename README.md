# Chord-DHT

Implementation Details
----------------------

The Chord protocol supports just one operation: given a key, it will determine the node responsible for storing the key's value. chord doesn't itself store keys and value; but provides primitives that allow higher-layer software to build a wide variety of storage system; CFS (Chord File System) is one such use of the Chord primitive.

1. Basic Query

The core usage of chord protocol is to query a key from a client (generally a node as well), i.e. to find sucessor(k).

- pass the query to a node's successor.
- if it can't find the key locally, it will lead to O(N) query time where N is no. of machines in the ring.

To avoid the linear search,

- a faster serach method by requiring each node to keep a finger table containing up to m entries, recall that m is the number of bits in hash key.

> - i{th} entry of node n will contain successor ((n+ 2 ^ {i-1}) mod 2^m). first entry of finger table is actually the node's immediate successor (and therefore and extra successor field is not needed).

- every time a node wants to look up a key k, it will pass the query to the closest successor or predecessor (depending on the finger table) of k in its finger table (the "largest" one on the circle whose ID is smaller than k), until a node finds out the key is stored in its immediate successor.

- with such a finger table, number of nodes that must be contacted to find a successor in an N-node network is O(log N).

2. Node Join

Whenever a new node joins, 3 invariants should be maintained (first two ensure correctness and the last one keeps querying fast):

- each node's successor points to its immediate successor correctly.
- each key is stored in successor(k)
- each node's finger table should be correct.

as the successor is the first entry of the finger table, we don't need to maintain this field separately any more. The following tasks should be done for a newly joined node n:

- initialize node n (the predecessor and the finger table)
- notify other nodes to update their predecessors and finger tables.
- the new node take over its responsible keys from its successor.
  
the predecessor of n can be easily obtained from the predecessor of successor(n) (in the previous circle).

As for its finger table, there are various initialization methods. The simplest one is to execute find successor queries for all m entries, resulting in O(mlog n) initialization time.

A better method is to check whether the i{th} entry in the finger table is still correct for the (i+1){th} entry. This will lead to O(log^2 n).

3. Stabilization

to ensure correct lookups, all successor pointers must be up to date. Therefore, a stabilization protocol is running periodically in the background which updates finger tables and successor pointers.

it works as follows :

- **Stailize()** : n asks its successor for its predecessor p and decides whether p should be n's successor instead (this is the case if p recently joined the system).

- **Notify()** : notifies n's successor of its existence, so it can change its predecessor to n.

- **Fix_fingers()** : updates finger tables/*

- **check_predecessor()** : periodically checks in predecessor is alive.

