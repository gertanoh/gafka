# Building a gafka : A Kafka like system
Building a system that resembles Kafka. Producers shall be able to send data to the cluster, while consumers can read it.
It is for educational purposes, but who knows?


# V1 Features 
- [x] Log
- [x] Partition / Topic / Replication
- [x] Partition connectivity / Raft Group
- [x] Broker / Cluster Raft Group
- [x] Broker GRPC Server

Came to the conclusion that relying purely on gossip for metadata management can lead to inconsistencies and failures.

# V2 
let's follow the traditional controller-based kafka architecture.


# Goals
- Trivial to install    
- Ease to use
- Ease to operate (need metrics)

















# Resources
- https://kafka.apache.org/documentation
- https://kafka.apache.org/documentation/#log