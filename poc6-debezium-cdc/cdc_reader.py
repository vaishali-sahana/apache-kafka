import json
import time

from confluent_kafka import Consumer
from fastapi import APIRouter

from common_service import AppServices
from logger_config import MyLogger

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "poc6.poc6_inventory.customers"
GROUP_ID = "cdc-reader-group"

logger = MyLogger.get_logger(__name__)
cdc_router = APIRouter()


@cdc_router.get("/cdc/events")
def read_events(timeout_seconds: float = 5.0):
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    events = []
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
            if msg.value() is None:
                continue  # tombstone record following a delete event

            payload = json.loads(msg.value())["payload"]
            events.append(
                {
                    "op": payload["op"],
                    "before": payload["before"],
                    "after": payload["after"],
                }
            )

        return AppServices.app_response(
            200, "Debezium CDC events", success=True, data={"count": len(events), "events": events}
        )
    except Exception as exception:
        return AppServices.handle_exception(exception)
    finally:
        consumer.close()
