# POC 2: Partitions & Consumer Groups

Builds on [POC 1](../poc1-basic-producer-consumer) to show how partitioning and consumer groups
enable parallel, ordered processing. A producer publishes `customer-orders` events keyed by
`customer_id` to a 3-partition topic, and multiple consumers in the same group split the
partitions between them.

## Setup

1. Start a local Kafka broker:
   ```bash
   docker compose up -d
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create the topic with 3 partitions (must be done explicitly — auto-created topics default to 1 partition):
   ```bash
   python create_topic.py
   ```

## Run

Start two or three consumers in separate terminals, each in the same consumer group.
Each process auto-labels its log lines with its own PID (e.g. `consumer-83421`) so you
can tell them apart:
```bash
python consumer.py
python consumer.py
```

Then run the producer (sends 30 order events across 6 customers):
```bash
python producer.py
```

## What to observe

- On startup, each consumer logs which partitions it was assigned — Kafka splits the 3
  partitions across the active consumers in the group (e.g. one gets 2 partitions, the other 1).
- Events for the same `customer_id` always land on the same partition (keyed partitioning),
  so a given customer's orders are always processed by the same consumer and stay in order.
- Start a third consumer (`python consumer.py`) while the group is idle and rerun
  the producer — Kafka triggers a rebalance and each consumer now gets 1 partition.
- Stop one consumer with Ctrl+C and rerun the producer — the remaining consumers rebalance to
  cover the freed partition.

## Tear down

```bash
docker compose down
```
