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

## Run (FastAPI)

Start the API:
```bash
uvicorn app:app --reload --port 8000
```

Docs at http://localhost:8000/docs, or use curl:

```bash
# Create the topic with 3 partitions (must be done explicitly —
# auto-created topics default to 1 partition)
curl -X POST http://localhost:8000/topic/create

# Publish 30 seed customer orders across 6 customers
curl -X POST http://localhost:8000/produce/seed

# Poll for messages for up to 5 seconds (default). Call this concurrently
# from two or three terminals to see partitions split across consumers
# in the same group.
curl "http://localhost:8000/consume?timeout_seconds=5"
```

## What to observe

- Each `/consume` call logs (server-side) which partitions it was assigned, and returns
  `consumer_name`/`assigned_partitions` in the response — Kafka splits the 3 partitions
  across concurrently polling consumers in the group (e.g. one gets 2 partitions, the other 1).
- Events for the same `customer_id` always land on the same partition (keyed partitioning),
  so a given customer's orders are always processed by the same consumer and stay in order.
- Call `/consume` from a third terminal while the others are idle, then re-seed with
  `/produce/seed` — Kafka triggers a rebalance and each consumer now gets 1 partition.
- Stop polling from one terminal and re-seed — the remaining consumers rebalance to cover
  the freed partition on their next `/consume` call.

## Tear down

```bash
docker compose down
```
