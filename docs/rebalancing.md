# Client/Partition Rebalancing

This document is for figuring out how to rebalance clients and partitions.

https://www.lydtechconsulting.com/blog/kafka-message-keys#:~:text=Topic%20Partitions,consume%20from%20all%20the%20partitions.

https://medium.com/javarevisited/kafka-partitions-and-consumer-groups-in-6-mins-9e0e336c6c00

1. consumer joins/leaves
2. all consumers in the group stop fetching and commit current offsets
3. broker recalculates partition assignments
4. each consumer resumes from the committed offsets in newly assigned partitions


## Overview

- partitions ordered 0 to N
- clients use `Client` dataclass with a `partition` attribute. Unset is `None`.
- upon connection to the broker, `ClientRegistry` reassigns partitions to clients

## Requirements
- EVERY PARTITION MUST HAVE A CONSUMER
- If more partitions than clients, some clients have to consume from multiple partitions
- If more clients than partitions, some clients will be idle

## Rebalancing Algorithm

- OFFSETS TRAVEL WITH THE PARTITION, NOT THE CONSUMER

- if no clients have a partition assigned, assign first available partition to the client
- if some existing clients have assigned partitions, do a rebalance pass
- we rebalance by:
    - if partitions with no assigned clients are available, assign them to a client
    - else, if all partitions are assigned to a client, assign as a standby for existing partition (in sequence with lowest partition number first)

## Scratch

- Do I rebalance everything from scratch every time? Or do I 'insert' a new client and find best-fit?

```
partitions -> [p0, p1]
clients -> [c1, c2, c3]

n_partitions = len(partitions)
ps_matched = 0

default all clients to idle

rebalanced = False

while not rebalanced:
    if ps_matched == n_partitions:
        rebalanced = True
        continue

    for each client
        if client has no partition:
            if ps_matched >= n_partitions:
                client.idle = true
                continue
    
            client.pt = partitions[ps_matched]
            client.idle = false
            ps_matched += 1
        
        client has a partition but there are leftover partitions
        
        all clients have at least one partition

```

### Scenario 1: More clients than partitions
partitions -> `[p0, p1]`
clients -> `[c1, c2, c3]`
ps_matched = 0
n_partitions = 2

`c1` -> has no partition
`c1.pt` = `p0`
ps_matched = 1/2

`c2` -> has no partition
`c2.pt` = `p1`
ps_matched = 2/2

`c3` -> has no partition
ps_matched is equal to n_partitions, set client.idle = true and break

### Scenario 2: More partitions than clients
partitions -> `[p0, p1, p2]`
clients -> `[c1, c2]`
ps_matched = 0
n_partitions = 3

`c1` -> has no partition
`c1.pt` = `p0`
ps_matched = 1/3

`c2` -> has no partition
`c2.pt` = `p1`
ps_matched = 2/3

restart the main loop
`c1` -> has partition, continue


### Work in progress

Currently completely re-working the Consumer Registry to track both client id assignments as well as the offsets
This is so we can better separate offset tracking and client->topic/partition mapping for rebalancing actions
