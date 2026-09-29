# POC 4: Poison Pill Handling

Builds on [POC 3](../poc3-offsets-and-replay) to show that Kafka itself has no built-in
protection against a "poison pill" — a message that always fails to process. This POC
demonstrates the application-level pattern: bounded retries, then quarantine to a
dead-letter topic (DLQ) so the consumer can commit past the bad record and keep going.

## Setup

1. Start a local Kafka broker:
   ```bash
   docker compose up -d
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Run

Publish 10 events to the `events` topic. Two of them are poison pills:
- `event_id 4`: valid JSON, but flagged `"poison": true` so the consumer's business logic
  always raises on it.
- `event_id 7`: not valid JSON at all, simulating a corrupt payload.

```bash
python producer.py
```

Consume them:
```bash
python consumer.py
```

Watch the dead-letter topic that poisoned records get routed to:
```bash
python dlq_reader.py
```

## What to observe

- `consumer.py` commits offsets manually (`enable.auto.commit: False`) so a partition only
  advances once a record is handled — either processed successfully or given up on.
- For `event_id 4`, `process()` raises every time. The consumer catches the exception,
  `seek()`s back to that offset (instead of committing), and retries up to `MAX_RETRIES`
  times. Without this, the same record would be redelivered forever and every message
  behind it on that partition would be stuck.
- After `MAX_RETRIES` attempts, the consumer gives up, publishes the record to
  `events-dlq` (with headers recording the original topic/partition/offset and the failure
  reason), and *then* commits — deliberately skipping past the poison pill so the partition
  keeps moving.
- For `event_id 7`, `json.loads()` fails immediately — this is a deserialization error, not
  a processing error, so retrying is pointless. It's sent straight to the DLQ and skipped.
- `dlq_reader.py` reads `events-dlq` so you can inspect what got quarantined and why,
  without needing to reprocess the original topic.

## Tear down

```bash
docker compose down
```
