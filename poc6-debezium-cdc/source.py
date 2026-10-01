import pymysql
from fastapi import APIRouter
from pydantic import BaseModel

from common_service import AppServices
from logger_config import MyLogger

DB_CONFIG = dict(
    host="localhost", port=3307, database="poc6_inventory", user="root", password="root", autocommit=True
)

logger = MyLogger.get_logger(__name__)
source_router = APIRouter()


class Customer(BaseModel):
    name: str
    email: str


def _connect():
    return pymysql.connect(**DB_CONFIG)


@source_router.post("/setup")
def setup_table():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS customers ("
            "id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), email VARCHAR(255))"
        )
        cur.close()
        conn.close()
        return AppServices.app_response(200, "Table ready", success=True)
    except Exception as exception:
        return AppServices.handle_exception(exception)


@source_router.post("/customers")
def create_customer(customer: Customer):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO customers (name, email) VALUES (%s, %s)",
            (customer.name, customer.email),
        )
        customer_id = cur.lastrowid
        cur.close()
        conn.close()
        logger.info("inserted customer %s", customer_id)
        return AppServices.app_response(
            201, "Customer inserted", success=True, data={"id": customer_id, **customer.dict()}
        )
    except Exception as exception:
        return AppServices.handle_exception(exception)


@source_router.put("/customers/{customer_id}")
def update_customer(customer_id: int, customer: Customer):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute(
            "UPDATE customers SET name = %s, email = %s WHERE id = %s",
            (customer.name, customer.email, customer_id),
        )
        updated = cur.rowcount
        cur.close()
        conn.close()
        if not updated:
            return AppServices.app_response(404, "Customer not found", success=False)
        return AppServices.app_response(200, "Customer updated", success=True)
    except Exception as exception:
        return AppServices.handle_exception(exception)


@source_router.delete("/customers/{customer_id}")
def delete_customer(customer_id: int):
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("DELETE FROM customers WHERE id = %s", (customer_id,))
        deleted = cur.rowcount
        cur.close()
        conn.close()
        if not deleted:
            return AppServices.app_response(404, "Customer not found", success=False)
        return AppServices.app_response(200, "Customer deleted", success=True)
    except Exception as exception:
        return AppServices.handle_exception(exception)


@source_router.get("/customers")
def list_customers():
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("SELECT id, name, email FROM customers ORDER BY id")
        rows = [{"id": r[0], "name": r[1], "email": r[2]} for r in cur.fetchall()]
        cur.close()
        conn.close()
        return AppServices.app_response(200, "Source table", success=True, data=rows)
    except Exception as exception:
        return AppServices.handle_exception(exception)
