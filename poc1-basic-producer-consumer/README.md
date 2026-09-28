# POC 1: Basic Producer & Consumer

Minimal Kafka POC exposed via FastAPI: a producer endpoint publishes 10 `orders` events to a
topic, a consumer endpoint polls and returns them as JSON.

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

Start the API:
```bash
uvicorn app:app --reload --port 8000
```

Interactive docs: http://localhost:8000/docs, or use curl:

```bash
# Publish 10 seed orders
curl -X POST http://localhost:8000/produce/order

## What to observe

- The producer returns each delivered order once `producer.flush()` completes.
- The consumer returns each order it read within the poll window, along with partition/offset.
- Call `/consume` again — since it uses `group.id=orders-consumer-group` with committed offsets,
  it resumes from where it left off rather than re-reading everything already consumed.


## Tear down

```bash
docker compose down
```
