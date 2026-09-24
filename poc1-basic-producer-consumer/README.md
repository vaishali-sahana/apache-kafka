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

## Run (FastAPI)

Option A — single combined app (producer mounted at `/producer`, consumer at `/consumer`):
```bash
uvicorn app:app --reload --port 8000
```
Docs at http://localhost:8000/producer/docs and http://localhost:8000/consumer/docs.

```bash
# Publish a single order
curl -X POST http://localhost:8000/producer/produce \
  -H "Content-Type: application/json" \
  -d '{"order_id": 1, "amount": 100}'

# Publish 10 seed orders (same as producer.py)
curl -X POST http://localhost:8000/producer/produce/seed

# Poll for messages for up to 5 seconds (default)
curl "http://localhost:8000/consumer/consume?timeout_seconds=5"
```

Option B — run producer and consumer APIs separately:
```bash
uvicorn producer_api:app --reload --port 8001
uvicorn consumer_api:app --reload --port 8002
```
Docs at http://localhost:8001/docs and http://localhost:8002/docs; same `/produce`, `/produce/seed`, `/consume` routes as above, without the `/producer` and `/consumer` prefixes.

## What to observe

- The producer logs/returns each message's partition and offset once delivered.
- The consumer prints (or returns as JSON) each order it reads, along with partition/offset.
- Stop and restart `consumer.py`, or call `/consume` on `consumer.py` again — since it uses `group.id=orders-consumer-group` with committed offsets, it resumes from where it left off rather than re-reading everything.

## Tear down

```bash
docker compose down
```
