from confluent_kafka.admin import AdminClient, NewTopic
from fastapi import APIRouter

from common_service import AppServices
from logger_config import MyLogger

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customer-orders"
NUM_PARTITIONS = 3

admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

topic_router = APIRouter()

logger = MyLogger.get_logger("topic")


@topic_router.post("/topic/create")
def create_topic():
    try:
        logger.info("creating topic '%s' with %s partitions", TOPIC, NUM_PARTITIONS)
        new_topic = NewTopic(TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=1)
        futures = admin.create_topics([new_topic])

        results = {}
        for topic, future in futures.items():
            try:
                future.result()
                results[topic] = "created"
            except Exception as exc:
                results[topic] = f"failed: {exc}"

        return AppServices.app_response(
            200, "Topic creation attempted", success=True, data={"results": results}
        )
    except Exception as exception:
        return AppServices.handle_exception(exception)
