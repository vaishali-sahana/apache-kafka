# POC 10: Log Compaction

Every earlier POC uses `cleanup.policy=delete` (the default): records are
retained for a time/size window, then dropped, regardless of key. This POC
uses `cleanup.policy=compact`: the broker's log cleaner guarantees it keeps
**at least the last value for every key**, forever, and may discard every
earlier value for that key once a segment rolls and becomes eligible for
cleaning.

This is the same mechanism Kafka Streams relies on for its state-store
changelog topics (the thing poc9 hand-rolled with `state.json`) and the
building block behind "latest state per key" topics in general -- e.g. a
`customer-profiles` topic you can replay to rebuild a cache or a table.

## How it works

- `create_topic.py` explicitly creates `customer-profiles` with
  `cleanup.policy=compact` (auto-created topics use the cluster default,
  `delete`, so this step can't be skipped).
- `producer.py` sends three updates for `alice`, one for `bob`, one for
  `carol` followed by a **tombstone** (a record with a `None`/null value)
  for `carol`, then pads the topic with filler records so the active
  segment exceeds `segment.bytes` and rolls -- only a rolled, non-active
  segment is eligible for compaction.
- `reader.py` reads the topic from the beginning with a fresh consumer
  group each time, so what it prints reflects exactly what the broker
  currently retains -- not some consumer's own offset bookkeeping.

## Setup

```bash
docker compose up -d
pip install -r requirements.txt
python create_topic.py
```

## Run

```bash
python reader.py
# before compaction runs: 3 records for alice, 1 for bob, 1 for carol,
# 1 tombstone for carol -- every update is still in the log
```

```bash
python producer.py
```

Wait ~10-15s for the log cleaner's next pass (`log.cleaner.backoff.ms` is
set to 1s in `docker-compose.yml`, but the cleaner also waits for the
active segment to roll past `segment.ms`/`segment.bytes` first), then:

```bash
python reader.py
# after compaction: exactly 1 record for alice (the last update, plan=pro,
# email=alice@new.example), 1 record for bob, and carol is gone entirely
# -- the tombstone caused her key to be removed once compaction ran
```

Re-run `reader.py` a few times a couple of seconds apart if compaction
hasn't run yet; the cleaner runs on its own schedule, not on demand.

## What to observe

- `alice`'s two earlier values (`plan=free`, then `plan=pro` with the old
  email) are gone after compaction -- only her latest record survives, by
  key.
- `bob` only ever had one value, so he's unaffected either way.
- `carol`'s tombstone doesn't just overwrite her value with "null" -- it
  removes the key from the log entirely once compacted (after
  `delete.retention.ms`, the tombstone itself is removed too, so a
  consumer that was never caught up won't even see the delete happen).
- This is why compacted topics are safe to use as "the last known value
  per key", but unsafe as an audit log or event history -- compaction
  destroys the intermediate history by design.

## Tear down

```bash
docker compose down -v
```
