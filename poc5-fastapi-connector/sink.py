import json
import time

from confluent_kafka import Consumer
from fastapi import APIRouter

from common_service import AppServices
from logger_config import MyLogger

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customers-cdc"
GROUP_ID = "customers-sink-group"

sink_router = APIRouter()
logger = MyLogger.get_logger(__name__)

# Stands in for "the target system" a real sink connector (e.g. Elasticsearch, a
# warehouse) would write to. Applying the CDC events here is exactly what a sink
# connector does under the hood.
replica_db = {}


@sink_router.post("/sink/sync")
def sync(timeout_seconds: float = 5.0):
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    applied = []
    try:
        consumer.subscribe([TOPIC])
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue

            event = json.loads(msg.value())
            customer_id = int(msg.key())

            if event["op"] in ("c", "u"):
                replica_db[customer_id] = event["after"]
            elif event["op"] == "d":
                replica_db.pop(customer_id, None)

            applied.append(event)

        return AppServices.app_response(
            200,
            "Replica synced",
            success=True,
            data={"applied": applied, "replica": list(replica_db.values())},
        )
    except Exception as exception:
        return AppServices.handle_exception(exception)
    finally:
        consumer.close()


@sink_router.get("/sink/replica")
def get_replica():
    return AppServices.app_response(
        200, "Replica table", success=True, data=list(replica_db.values())
    )
