# Kafka Lite design notes

## Functional requirements
- distributed commit log 
- streaming with grpc clients
- one point data consumption from grpc
- data ordered by topics

## Non functional requirements
- reliable
- scalable

Notes : 
things that leads to inefficiency
- small i/o
- excessive byte coping (using proto, and shared them)
- how do we handle messages delivery semantics


Lot of configuration
- producer acknowledgment
- messages delivery semantics


## HLD

### Design
GOING FOR A CP system
cluster of brokers, which are connected through raft.
It is to handle Metadata and partition distribution of the cluster
Using consistent hashing for partition distribution



High level structure:

### Internals
Log is a append log file. Each log consists of multiple segments and one activate segment.
Each segment is a store and an index to spead up access.

topic is a logical grouping for client messages.
each topic is split in partitions(specified by the client).
Each partition is a log, and is replicated by the replication factor.


![alt text](image.png)


# Resources 
- https://github.com/rqlite/rqlite/tree/master
- Book Building Distributed Services in Go