# POC 7: Idempotent Producer + Idempotent Consumer + Transactions

This POC separates three distinct notions of "exactly once" that people often
conflate:

1. **Idempotent producer** (`enable.idempotence=true`) — stops the *broker*
   from writing the same record twice when the producer retries a send after
   a lost ack. This is a Kafka client/broker feature, no app code needed.
2. **Idempotent consumer** — stops *your application* from applying the same
   message twice when the consumer legitimately receives it more than once
   (e.g. after a crash before the offset was committed).
3. **Transactions** (`transactional.id` + `send_offsets_to_transaction`) —
   idempotence alone only guarantees a single produce is deduped; it says
   nothing about a *read-process-write* pipeline where you consume from one
   topic and produce to another. Transactions make "consume + produce +
   commit the input offset" one atomic unit, so a crash mid-pipeline can
   never leave a half-done record (output written but input offset not
   advanced, or vice versa).

## Files

- `producer.py` — produces 10 payment events to `payments` with
  `enable.idempotence=true`, `acks=all`.
- `consumer.py` — manually-committing consumer that applies each payment to a
  `ledger.json` balance.
- `tx_pipeline.py` — transactional read-process-write: consumes from
  `payments`, writes an audited copy to `payments-audited`, and commits the
  input offset in the same transaction as the output produce.

## Setup

```bash
docker compose up -d
pip install -r requirements.txt
```

## Run: the bug (no dedup)

```bash
python producer.py

rm -f ledger.json
IDEMPOTENT=false CRASH_AFTER=5 python consumer.py
# processes payment_id 1-5, applies them to the ledger, crashes before
# committing offset 5 -> balance reflects payments 1-5

IDEMPOTENT=false python consumer.py
# resumes from the last COMMITTED offset (4), so payment_id 5 is redelivered
# and applied a second time -> balance is now wrong, payment 5 was double-counted
```

## Run: the fix (idempotent consumer)

```bash
rm -f ledger.json
CRASH_AFTER=5 python consumer.py
# same crash as above

python consumer.py
# payment_id 5 is redelivered, but ledger.json already has it in
# applied_payment_ids, so apply_payment() skips it -> balance is correct
```

## Run: transactional read-process-write (exactly-once pipeline)

```bash
python producer.py   # (re)populate payments if needed

CRASH_AFTER=5 python tx_pipeline.py
# writes audited copies 1-4 to payments-audited, commits each transaction;
# on message 5 it crashes with the transaction open (not committed)

python tx_pipeline.py
# on restart, the broker aborts the dangling transaction from payment_id=5
# and the consumer (isolation.level=read_committed) never saw its output;
# the input offset was never advanced either, so payment_id=5 is re-read
# and reprocessed cleanly -> payments-audited ends up with exactly one
# record per payment_id, never zero, never two
```

## What to observe

- In both ledger runs, the producer's `enable.idempotence=true` guaranteed
  each payment was written to the topic exactly once — the broker never
  duplicated a record.
- With `IDEMPOTENT=false`, `payment_id=5` gets applied twice because nothing
  tracks "have I already processed this key".
- In the transactional run, idempotence is still what prevents a duplicate
  *within* one produce — transactions add the atomicity across the
  consume-offset-commit and the produce-to-sink that idempotence alone
  doesn't give you.

## Tear down

```bash
docker compose down -v
rm -f ledger.json
```
