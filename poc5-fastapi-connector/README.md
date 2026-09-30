# POC 5: The Kafka Connect Pattern, via FastAPI

Real Kafka Connect setups (Debezium, JDBC connectors, Elasticsearch sinks, ...) need
extra infrastructure — a database with CDC enabled, a Connect worker, connector plugins.
This POC skips all of that and reimplements the same **source → topic → sink** pattern as
plain FastAPI endpoints, so you can see the mechanics without standing up anything beyond
the Kafka broker you've already used in the earlier POCs.

```text
POST/PUT/DELETE /customers  -->  customers-cdc (topic)  -->  POST /sink/sync
   (source connector)                                          (sink connector)
```

## Concepts

- **Source connector** — watches a system for changes and emits an event per change.
  `source.py` plays this role: every create/update/delete against the in-memory
  `customers_db` produces a change event to the `customers-cdc` topic, shaped like a real
  CDC record: `{"op": "c"|"u"|"d", "before": {...}|null, "after": {...}|null}`. A real
  connector (e.g. Debezium) gets this same shape by reading the database's transaction
  log instead of application code calling `producer.produce()` directly — the event
  contract is what matters, not how it's captured.
- **Sink connector** — reads a topic and applies each event to a target system.
  `sink.py` plays this role: `/sink/sync` consumes `customers-cdc` and replays each event
  into `replica_db` — insert/update on `"c"`/`"u"`, remove on `"d"`.
- 
## Setup

1. Start a local Kafka broker:
   ```bash
   docker compose up -d
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Start the API:
   ```bash
   uvicorn app:app --reload --port 8000
   ```

Interactive docs: http://localhost:8000/docs

## Run

Create, update, and delete a customer through the "source" endpoints:
```bash
curl -X POST http://localhost:8000/customers \
  -H "Content-Type: application/json" \
  -d '{"name": "Ada Lovelace", "email": "ada@example.com"}'

curl -X PUT http://localhost:8000/customers/1 \
  -H "Content-Type: application/json" \
  -d '{"name": "Ada Lovelace", "email": "ada@newmail.com"}'

curl -X DELETE http://localhost:8000/customers/1
```

Each call immediately emits a change event to `customers-cdc` — nothing is buffered.

Now run the "sink" to pull those events out of Kafka and apply them to the replica:
```bash
curl -X POST http://localhost:8000/sink/sync
```

Check what landed on each side:
```bash
curl http://localhost:8000/customers      # source table
curl http://localhost:8000/sink/replica   # replica, built entirely from Kafka events
```

## What to observe

- `/sink/sync` returns both the raw events it applied and the resulting replica state —
  you can see the `before`/`after`/`op` fields drive exactly what happens to `replica_db`.
- Because `sink.py` uses a real consumer group (`customers-sink-group`) with committed
  offsets, calling `/sink/sync` again after no new writes returns an empty `applied` list
  — it only sees new events, the same as any consumer group.
- Create a few more customers, then call `/sink/sync` once — it catches up on everything
  it missed in one pass. This is the same "catch up from committed offset" behavior a real
  sink connector relies on after being restarted.

## Tear down

```bash
docker compose down
```
