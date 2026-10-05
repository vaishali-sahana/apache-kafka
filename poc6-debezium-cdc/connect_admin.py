import json

import requests
from fastapi import APIRouter

from common_service import AppServices
from logger_config import MyLogger

CONNECT_URL = "http://localhost:8083"

logger = MyLogger.get_logger(__name__)
connect_router = APIRouter()


@connect_router.post("/connect/register")
def register_connector(config_file: str = "register-connector.json"):
    try:
        with open(config_file) as f:
            config = json.load(f)
        response = requests.post(f"{CONNECT_URL}/connectors", json=config, timeout=10)
        logger.info("register connector response: %s %s", response.status_code, response.text)
        return AppServices.app_response(
            response.status_code, "Connector registration submitted",
            success=response.ok, data=response.json(),
        )
    except Exception as exception:
        return AppServices.handle_exception(exception)

@connect_router.get("/connect/status")
def connector_status(name: str = "inventory-connector"):
    try:
        response = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=10)
        return AppServices.app_response(
            response.status_code, "Connector status", success=response.ok, data=response.json()
        )
    except Exception as exception:
        return AppServices.handle_exception(exception)


@connect_router.delete("/connect/unregister")
def unregister_connector(name: str = "inventory-connector"):
    try:
        response = requests.delete(f"{CONNECT_URL}/connectors/{name}", timeout=10)
        return AppServices.app_response(
            response.status_code, "Connector removed", success=response.ok
        )
    except Exception as exception:
        return AppServices.handle_exception(exception)
