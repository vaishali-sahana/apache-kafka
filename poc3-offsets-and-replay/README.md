# POC 3: Offsets & Replay

Builds on [POC 2](../poc2-partitions-consumer-groups) to show how consumer offsets work: manual
commits, what happens when a commit is missed (crash before commit), and how to deliberately
rewind a consumer group to replay events it already processed.

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

Publish 20 events to the `events` topic:
```bash
python producer.py
```

Consume them with manual offset commits:
```bash
python consumer.py
```

### Simulate a crash before commit

Run the consumer with `CRASH_AFTER` set to exit right after processing a message but
*before* committing its offset:
```bash
CRASH_AFTER=5 python consumer.py
```
Then restart it normally:
```bash
python consumer.py
```

### Replay already-committed events

Rewind the consumer group back to the start of the topic, or to a specific offset, and
reprocess events that were already committed. Edit `TARGET_OFFSET` at the top of
`replay.py` (defaults to `0`, the start of the topic), then run:
```bash
python replay.py
```

## What to observe

- `consumer.py` commits the offset only after processing each message
  (`enable.auto.commit: False`), so the committed offset always trails what's been handled.
- With `CRASH_AFTER=5`, the process exits after processing message 5 but before committing
  it. Restarting the consumer re-delivers message 5 (and onward) — at-least-once delivery in
  action, since the last commit was for message 4.
- Run the consumer to completion (no crash), then run it again — it reads nothing new, because
  the committed offset is already at the end of the topic.
- `replay.py` seeks the group's assigned partitions to an explicit offset and commits as it
  re-reads, letting you deliberately reprocess events that were already consumed — useful for
  reprocessing after a bug fix or backfilling a downstream system.

## Tear down

```bash
docker compose down
```
