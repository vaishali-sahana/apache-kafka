# POC 1: Basic Producer & Consumer

Minimal Kafka POC: a producer publishes `orders` events to a topic, a consumer reads and prints them.

## Setup

1. Start a local Kafka broker:
   ```bash
   docker compose up -d
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Run (CLI scripts)

In one terminal, start the consumer (it waits for messages):
```bash
python consumer.py
```

In another terminal, run the producer (sends 10 order events):
```bash
python producer.py
```

## What to observe

- The producer logs/returns each message's partition and offset once delivered.
- The consumer prints (or returns as JSON) each order it reads, along with partition/offset.
- Stop and restart `consumer.py`, or call `/consume` on `consumer.py` again — since it uses `group.id=orders-consumer-group` with committed offsets, it resumes from where it left off rather than re-reading everything.

## Tear down

```bash
docker compose down
```
