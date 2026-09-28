import json
import os
import time

from confluent_kafka import Consumer
from fastapi import APIRouter

from common_service import AppServices
from logger_config import MyLogger

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customer-orders"
GROUP_ID = "customer-orders-group"

consumer_router = APIRouter()

logger = MyLogger.get_logger("consumer")


@consumer_router.get("/consume")
def consume_orders(timeout_seconds: float = 5.0):
    consumer_name = f"consumer-{os.getpid()}"
    assigned_partitions: list = []

    def on_assign(_, partitions):
        assigned_partitions.extend(p.partition for p in partitions)
        logger.info("[%s] assigned partitions: %s", consumer_name, assigned_partitions)

    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        logger.info("[%s] consuming customer orders", consumer_name)
        consumer.subscribe([TOPIC], on_assign=on_assign)

        messages = []
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("[%s] Consumer error: %s", consumer_name, msg.error())
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
            "Customer orders consumed",
            success=True,
            data={
                "consumer_name": consumer_name,
                "assigned_partitions": assigned_partitions,
                "count": len(messages),
                "messages": messages,
            },
        )
    except Exception as exception:
        return AppServices.handle_exception(exception)
    finally:
        consumer.close()
