# POC 8: Schema Registry + Avro (Schema Evolution)

Earlier POCs serialized messages as plain JSON with no shared contract between
producer and consumer. This POC introduces Avro + Confluent Schema Registry,
which gives you:

1. **Compact binary serialization** — each record on the wire is just a
   5-byte magic-byte+schema-ID header followed by Avro binary, not a
   repeated-field-names JSON blob.
2. **A shared, versioned contract** — the registry stores every schema
   version for a subject (`<topic>-value`) and assigns it a global ID. The
   producer embeds that ID in each record instead of the schema itself.
3. **Compatibility enforcement** — the registry rejects a new schema version
   that breaks compatibility with what's already registered, catching
   breaking producer changes at publish time instead of at some downstream
   consumer's deserialization failure.

## Files

- `schemas/order_v1.avsc` — baseline schema: `order_id`, `customer`, `amount`.
- `schemas/order_v2_compatible.avsc` — adds `currency` **with a default** ->
  backward-compatible, old consumers reading new data still work.
- `schemas/order_v3_breaking.avsc` — adds `currency` **without a default** ->
  not backward-compatible, rejected by the registry.
- `producer.py` — `AvroProducer` that serializes against whichever schema
  file `SCHEMA_FILE` points at.
- `consumer.py` — `AvroConsumer` that reads records and resolves them
  against the registry automatically, regardless of which schema version
  wrote them.
- `check_compatibility.py` — calls the registry's `/compatibility` endpoint
  directly to check a candidate schema before ever producing with it.

## Setup

```bash
docker compose up -d
pip install -r requirements.txt
```

## Run: baseline

```bash
SCHEMA_FILE=schemas/order_v1.avsc python producer.py
python consumer.py
# Ctrl+C after you see both orders
```

## Run: compatible evolution (add optional field)

```bash
python check_compatibility.py schemas/order_v2_compatible.avsc
# -> compatible with full schema history: True

SCHEMA_FILE=schemas/order_v2_compatible.avsc python producer.py
python consumer.py
# both the v1-shaped and v2-shaped records print fine; consumer never
# restarted and needed no code change
```

## Run: breaking evolution (remove the default)

```bash
python check_compatibility.py schemas/order_v3_breaking.avsc
# -> compatible with full schema history: False
#    READER_FIELD_MISSING_DEFAULT_VALUE ... oldSchemaVersion: 1

SCHEMA_FILE=schemas/order_v3_breaking.avsc python producer.py
# confluent_kafka.avro.error.ClientError: ...409...
# READER_FIELD_MISSING_DEFAULT_VALUE, citing oldSchemaVersion 1 -- the
# registry refused to register order_v3_breaking.avsc as a new version
# of orders-value, so the producer never got to publish a record that
# a consumer still on the v1 schema couldn't read
```

**Gotcha worth knowing**: `check_compatibility.py` calls
`/compatibility/subjects/orders-value/versions?verbose=true` (plural
`versions`), not `.../versions/latest`. The `latest`-suffixed endpoint only
ever diffs a candidate schema against the single most recent version, even
when the subject's compatibility mode is `BACKWARD_TRANSITIVE` — so checking
v3 against v2 (which already has `currency` populated on every record)
reports it as compatible, hiding that it still can't read v1 data. The
plural `versions` endpoint, like the real `POST /subjects/.../versions`
registration call, walks the *entire* history and correctly flags v1 as the
version it breaks.

## What to observe

- The consumer code never changes across v1 -> v2; `AvroConsumer` fetches the
  *writer's* schema by the ID embedded in each record and reconciles it with
  the latest schema using Avro's schema-resolution rules (missing fields in
  the reader fall back to the writer's value or default).
- The registry's `BACKWARD` compatibility mode (set in `docker-compose.yml`)
  is what makes v2 succeed and v3 fail — it checks that a reader using the
  *new* schema can still read data written with the *previous* schema; v3
  fails because `currency` has no default so old (currency-less) records
  can't be read by a reader expecting it.

## Tear down

```bash
docker compose down -v
```
