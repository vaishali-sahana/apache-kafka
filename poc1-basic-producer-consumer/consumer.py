import json
import time

from confluent_kafka import Consumer
from fastapi import APIRouter

from common_service import AppServices
from logger_config import MyLogger

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"
GROUP_ID = "orders-consumer-group"

consumer_router = APIRouter()

logger= MyLogger.get_logger(__name__)


@consumer_router.get("/consume")
def consume_orders(timeout_seconds: float = 5.0):
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        logger.info("consuming orders")
        consumer.subscribe([TOPIC])

        messages = []
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("Consumer error: %s", msg.error())
                continue
            messages.append(
                {
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "order": json.loads(msg.value()),
                }
            )

        return AppServices.app_response(
            200,
            "Orders consumed",
            success=True,
            data={"count": len(messages), "messages": messages},
        )
    except Exception as exception:
        return AppServices.handle_exception(exception)
    finally:
        consumer.close()
