# POC 6: Real Debezium CDC on MySQL, Driven from FastAPI

This is a real Debezium setup — MySQL with binlog-based replication, a Kafka Connect
worker running the Debezium MySQL connector, and Kafka itself — with a small FastAPI app
on top so you write to the database and read the resulting change events through HTTP
endpoints instead of raw `mysql`/`curl` commands.

```text
FastAPI (/customers)  -->  MySQL (binlog)  -->  Debezium connector  -->  Kafka topic  -->  FastAPI (/cdc/events)
```

The FastAPI app is a *client* of this pipeline, not part of it — it never touches Kafka on
the write side. Debezium is the one reading MySQL's binary log and producing events;
FastAPI just triggers the writes and reads back what Debezium published.

## Concepts

- **CDC (Change Data Capture)** — Debezium tails MySQL's binary log (binlog) instead of
  polling, so it sees every row-level change exactly once, in commit order, the instant
  it's committed. This needs `binlog_format=ROW`, which the `debezium/example-mysql` image
  already has enabled.
- **Replication user** — MySQL requires a user with `REPLICATION SLAVE`/`REPLICATION
  CLIENT` grants to read the binlog. The `debezium/example-mysql` image ships one out of
  the box: `debezium`/`dbz`, used in `register-connector.json`.
- **Connector as config, not code** — `register-connector.json` is the entire integration:
  which database, which table, which user. No producer code was written for the source
  side; Debezium is a Kafka Connect source connector like any other.

## Setup

1. Start MySQL, Kafka, and Debezium Connect:
   ```bash
   docker compose up -d
   ```
   Give it ~20-30 seconds for Connect to come up after Kafka and MySQL.

2. Install FastAPI app dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Start the API:
   ```bash
   uvicorn app:app --reload --port 8000
   ```

4. Create the table (via the API, so nothing needs the `mysql` CLI):
   ```bash
   curl -X POST http://localhost:8000/setup
   ```

5. Register the Debezium connector. `connect_admin.py` reads `register-connector.json`
   and forwards it to Connect's REST API (port 8083) on your behalf, so this is reachable
   from Swagger at http://localhost:8000/docs instead of needing a separate curl call to
   a different port:
   ```bash
   curl -X POST http://localhost:8000/connect/register
   ```
   Check it's running:
   ```bash
   curl http://localhost:8000/connect/status
   ```

## Run

Write to MySQL through the FastAPI endpoints:
```bash
curl -X POST http://localhost:8000/customers \
  -H "Content-Type: application/json" \
  -d '{"name": "Ada Lovelace", "email": "ada@example.com"}'

curl -X PUT http://localhost:8000/customers/1 \
  -H "Content-Type: application/json" \
  -d '{"name": "Ada Lovelace", "email": "ada@newmail.com"}'

curl -X DELETE http://localhost:8000/customers/1
```

Now read the change events Debezium produced from those writes — no code in
`source.py` published anything to Kafka; this is purely what Debezium captured off the
binlog:
```bash
curl http://localhost:8000/cdc/events
```

## What to observe

- The insert shows up with `"op": "c"`, `"before": null`, and an `"after"` object with the
  new row.
- The update shows `"op": "u"` with both `"before"` (old email) and `"after"` (new email)
  — CDC gives you the diff, not just a notification that something changed.
- The delete shows `"op": "d"` with the old row in `"before"` and `"after": null`.
- `cdc_reader.py` uses a real consumer group (`cdc-reader-group`); call `/cdc/events`
  again with no new writes and you'll get an empty list — it only returns events it
  hasn't already consumed, same as any Kafka consumer.


## Tear down

```bash
docker compose down -v
```
