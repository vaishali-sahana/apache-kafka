import json
import time

from confluent_kafka import Producer
from fastapi import APIRouter
from pydantic import BaseModel

from common_service import AppServices
from logger_config import MyLogger
BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

producer_router = APIRouter()


class Order(BaseModel):
    order_id: int
    amount: int


logger= MyLogger.get_logger(__name__)


@producer_router.post("/produce/order")
def produce_order():
    try:
        logger.info("producing seed orders")
        delivered = []
        for order_id in range(1, 11):
            order = {"order_id": order_id, "amount": order_id * 100}

            producer.produce(
                TOPIC,
                key=str(order_id),
                value=json.dumps(order),
            )
            time.sleep(0.5)
            delivered.append(order)
        producer.flush()
        return AppServices.app_response(
            200, "Seed orders produced", success=True, data={"delivered": delivered}
        )
    except Exception as exception:
        return AppServices.handle_exception(exception)
