# bodine — Distributed Message Broker

A lightweight distributed message broker built in Python, inspired by Kafka. Producers send messages to named topics over persistent TCP connections, and consumers poll for messages using a simple offset-based model.

---

## Architecture

```
┌──────────────┐         TCP          ┌─────────────────────────┐
│   Producer   │ ───────────────────► │                         │
└──────────────┘                      │         Broker          │
                                      │                         │
┌──────────────┐         TCP          │  Topics → Partitions    │
│   Consumer   │ ◄─────────────────── │  Offsets → Messages     │
└──────────────┘    (poll-based)      │  Write-ahead log (WAL)  │
                                      └─────────────────────────┘
```

**Wire protocol:** Length-prefixed JSON frames over raw TCP. Each message is preceded by a 4-byte big-endian integer indicating the payload length.

**Storage:** Each topic partition is backed by an append-only log file on disk. Consumers track their position via an offset (integer index into the log).

**Delivery semantics:** At-least-once. The broker acknowledges a message after it is flushed to disk. Consumers commit offsets explicitly after processing.

**Clustering:** A broker cluster uses a gossip-based membership layer for node discovery. Partitions are assigned to nodes, and producers route to the correct node via a consistent hashing ring.

---

## Project Structure

```
bodine/
├── broker/          # Broker server — TCP listener, topic management, WAL
├── client/          # Python client library — Producer and Consumer classes
├── examples/        # Minimal runnable demo scripts
├── docker-compose.yml
└── README.md
```

---

## Quickstart

```bash
docker compose up
# In separate terminals:
python examples/produce.py
python examples/consume.py
```

---

## Checklist

### Milestone 1 — Single node, in-memory, Docker demo
- [ ] TCP server that accepts producer and consumer connections
- [ ] Length-prefixed JSON framing (encode/decode)
- [ ] In-memory topic and message store
- [ ] `PRODUCE` command: write message to topic
- [ ] `CONSUME` command: read message at offset from topic
- [ ] Python client library with `Producer` and `Consumer` classes
- [ ] `examples/produce.py` — sends a message to a topic
- [ ] `examples/consume.py` — polls and prints messages
- [ ] `docker-compose.yml` with broker, producer, and consumer containers

### Milestone 2 — Persistence
- [ ] Append-only write-ahead log (WAL) per topic partition
- [ ] Broker recovers topic state from disk on startup
- [ ] `fsync` strategy configurable (per-write vs. periodic)
- [ ] Consumer offset is durable (stored to disk on commit)

### Milestone 3 — Consumer groups
- [ ] Consumer group registration and membership tracking
- [ ] Partition assignment across group members
- [ ] Rebalance triggered when a consumer joins or leaves
- [ ] Offset commits scoped to group + partition

### Milestone 4 — Clustering
- [ ] Gossip-based node discovery and membership
- [ ] Consistent hashing ring for partition-to-node assignment
- [ ] Producer routes `PRODUCE` to correct node
- [ ] Partition replication (leader + follower)
- [ ] Leader failover when a node becomes unreachable
- [ ] `docker-compose.yml` updated to run a 3-node cluster

### Milestone 5 — Observability & polish
- [ ] Structured logging (JSON) across broker, producer, consumer
- [ ] `/metrics` endpoint exposing throughput, lag, partition sizes
- [ ] Benchmarking script — measures producer throughput and consumer lag
- [ ] Architecture diagram (this README)
- [ ] Contribution guide and local dev setup docs
