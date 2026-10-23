# POC 9: Windowed Aggregation (Hand-Rolled Stream Processing)

Every earlier POC processes records one at a time with a plain
produce/consume loop. This POC introduces the **idea** behind stream
processing frameworks like Kafka Streams: a processor that keeps running
state scoped to a time window, persisted so a restart doesn't lose it.


## How it works (`app.py`)

```
orders topic (key=customer_id, value={order_id, customer_id, amount})
  -> consume
  -> bucket event into a 30s tumbling window: window_start = floor(now / 30) * 30
  -> state[(customer_id, window_start)] += amount
  -> checkpoint state to state.json
  -> customer-order-totals topic (key=customer_id, value={customer_id, window_start, window_end, total})
  -> commit input offset
```

- **Tumbling window** — `window_start()` buckets the current wall-clock
  time into non-overlapping 30-second windows. All orders landing in the
  same 30s bucket accumulate into the same running total.
- **Grace period** — an order arriving more than `GRACE` (5s) after its
  window's end is dropped instead of silently reopening an old window,
  matching how Kafka Streams' grace period works.
- **State store, by hand** — `state` is an in-memory dict keyed by
  `(customer_id, window_start)`. It's checkpointed to `state.json` after
  every update and reloaded on startup, playing the role of a Kafka
  Streams state store's changelog topic: a restart resumes in-flight
  windows instead of restarting every total at 0.
- **Commit ordering** — the input offset is committed only *after* the
  output is produced and the state file is written, so a crash between
  those steps redelivers the order on restart rather than silently losing
  its contribution to the total.
- **No suppression** — like a Streams topology without `suppress()`, this
  emits an updated total to `customer-order-totals` on every order, not
  just once a window closes.

## Setup

```bash
docker compose up -d
pip install -r requirements.txt
```

## Run

Terminal 1 — start the aggregator:

```bash
python app.py
```

Terminal 2 — watch the output topic:

```bash
python consumer.py
```

Terminal 3 — produce orders for two customers, spaced 2s apart (`alice`:
10, 5, 20 / `bob`: 25, 15):

```bash
python producer.py
```

## What to observe

- All 5 orders (10s of wall-clock time at 2s spacing) land in the same 30s
  window. `customer-order-totals` prints `alice: 10.0`, then later
  `alice: 15.0`, then `alice: 35.0` for the same window -- each order's
  update, not a final-only result.
- Stop `app.py` (Ctrl+C) mid-run, inspect `state.json` -- it holds the
  in-flight window's running totals. Restart `python app.py` and resume
  `producer.py`: the log line `loaded N window(s) from state.json` shows
  the totals pick up where they left off instead of resetting to 0.
- Run `python producer.py` twice back-to-back: if the second run starts
  within the first run's 30s window, the two runs' totals combine; wait
  more than 35s (window + grace) between runs and they land in separate
  windows, each starting fresh from 0.
- Delete `state.json` before a run to see a cold start with no prior
  windows loaded.

## Tear down

```bash
docker compose down -v
rm -f state.json
```
