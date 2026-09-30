import json

from confluent_kafka import Producer
from fastapi import APIRouter
from pydantic import BaseModel

from common_service import AppServices
from logger_config import MyLogger

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customers-cdc"

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
logger = MyLogger.get_logger(__name__)

source_router = APIRouter()

# Stands in for "the source database" a real source connector (e.g. Debezium) would
# watch. Kept in memory purely so this POC needs nothing but Kafka running.
customers_db = {}


class Customer(BaseModel):
    name: str
    email: str


def _emit(op: str, before: dict | None, after: dict | None, key: int):
    event = {"op": op, "before": before, "after": after}
    producer.produce(TOPIC, key=str(key), value=json.dumps(event))
    producer.flush()
    logger.info("emitted change event: %s", event)


@source_router.post("/customers")
def create_customer(customer: Customer):
    try:
        customer_id = len(customers_db) + 1
        row = {"id": customer_id, **customer.model_dump()}
        customers_db[customer_id] = row
        _emit("c", None, row, customer_id)
        return AppServices.app_response(201, "Customer created", success=True, data=row)
    except Exception as exception:
        return AppServices.handle_exception(exception)


@source_router.put("/customers/{customer_id}")
def update_customer(customer_id: int, customer: Customer):
    try:
        before = customers_db.get(customer_id)
        if before is None:
            return AppServices.app_response(404, "Customer not found", success=False)
        after = {"id": customer_id, **customer.model_dump()}
        customers_db[customer_id] = after
        _emit("u", before, after, customer_id)
        return AppServices.app_response(200, "Customer updated", success=True, data=after)
    except Exception as exception:
        return AppServices.handle_exception(exception)


@source_router.delete("/customers/{customer_id}")
def delete_customer(customer_id: int):
    try:
        before = customers_db.pop(customer_id, None)
        if before is None:
            return AppServices.app_response(404, "Customer not found", success=False)
        _emit("d", before, None, customer_id)
        return AppServices.app_response(200, "Customer deleted", success=True, data=before)
    except Exception as exception:
        return AppServices.handle_exception(exception)


@source_router.get("/customers")
def list_customers():
    return AppServices.app_response(
        200, "Source table", success=True, data=list(customers_db.values())
    )
