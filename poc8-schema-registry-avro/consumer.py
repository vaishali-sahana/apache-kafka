from confluent_kafka.avro import AvroConsumer

BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"
GROUP_ID = "orders-group"


def main():
    consumer = AvroConsumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "schema.registry.url": SCHEMA_REGISTRY_URL,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    print("[consumer] listening on 'orders' (Ctrl+C to stop)")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[consumer] error: {msg.error()}")
                continue

            order = msg.value()
            # The consumer never needs to know which producer schema
            # version wrote this record - AvroConsumer fetches the
            # writer's schema by ID from the registry and resolves it
            # against the reader's (latest) schema automatically.
            print(f"[consumer] order={order}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
