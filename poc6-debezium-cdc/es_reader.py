import requests
from fastapi import APIRouter

from common_service import AppServices
from logger_config import MyLogger

ES_URL = "http://localhost:9200"
INDEX = "poc6.poc6_inventory.customers"

logger = MyLogger.get_logger(__name__)
es_router = APIRouter()


@es_router.get("/es/customers")
def search_customers():
    try:
        response = requests.get(f"{ES_URL}/{INDEX}/_search", timeout=10)
        if not response.ok:
            return AppServices.app_response(response.status_code, "Elasticsearch search failed", success=False, data=response.json())
        hits = response.json()["hits"]["hits"]
        docs = [{"id": hit["_id"], **hit["_source"]} for hit in hits]
        return AppServices.app_response(200, "Elasticsearch documents", success=True, data={"count": len(docs), "documents": docs})
    except Exception as exception:
        return AppServices.handle_exception(exception)
