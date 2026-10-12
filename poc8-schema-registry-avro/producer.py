import os
import time

from confluent_kafka.avro import AvroProducer
from confluent_kafka.avro.serializer import SerializerError

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"

# Point this at order_v1.avsc, order_v2_compatible.avsc, or
# order_v3_breaking.avsc to see what the registry accepts vs. rejects.
SCHEMA_FILE = os.environ.get("SCHEMA_FILE", "schemas/order_v1.avsc")


def load_schema(path):
    from confluent_kafka import avro

    return avro.load(path)


def main():
    value_schema = load_schema(SCHEMA_FILE)

    producer = AvroProducer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "schema.registry.url": SCHEMA_REGISTRY_URL,
        },
        default_value_schema=value_schema,
    )

    orders = [
        {"order_id": 1, "customer": "alice", "amount": 42.5},
        {"order_id": 2, "customer": "bob", "amount": 17.0},
    ]
    if "currency" in [f["name"] for f in value_schema.to_json()["fields"]]:
        for order in orders:
            order["currency"] = "USD"

    for order in orders:
        try:
            producer.produce(topic=TOPIC, value=order)
        except SerializerError as e:
            # Raised client-side when the schema registry rejects the
            # write because it's not compatible with the registered
            # schema for this subject (e.g. order_v3_breaking.avsc
            # dropping the default on `currency`).
            print(f"[producer] schema rejected by registry: {e}")
            return
        time.sleep(0.1)

    producer.flush()
    print(f"[producer] sent {len(orders)} orders using {SCHEMA_FILE}")


if __name__ == "__main__":
    main()
