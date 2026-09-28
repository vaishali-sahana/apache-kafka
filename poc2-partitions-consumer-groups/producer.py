import json
import time

from confluent_kafka import Producer
from fastapi import APIRouter

from common_service import AppServices
from logger_config import MyLogger

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customer-orders"
CUSTOMERS = ["cust-a", "cust-b", "cust-c", "cust-d", "cust-e", "cust-f"]

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

producer_router = APIRouter()

logger = MyLogger.get_logger("producer")


@producer_router.post("/produce/seed")
def produce_seed_orders():
    try:
        logger.info("producing seed customer orders")
        delivered = []
        for order_id in range(1, 31):
            customer_id = CUSTOMERS[order_id % len(CUSTOMERS)]
            order = {"order_id": order_id, "customer_id": customer_id, "amount": order_id * 10}

            producer.produce(
                TOPIC,
                key=customer_id,
                value=json.dumps(order),
            )
            time.sleep(0.2)
            delivered.append(order)
        producer.flush()
        return AppServices.app_response(
            200, "Seed customer orders produced", success=True, data={"delivered": delivered}
        )
    except Exception as exception:
        return AppServices.handle_exception(exception)
