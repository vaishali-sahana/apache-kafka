import json
import os

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customer-orders"
GROUP_ID = "customer-orders-group"
CONSUMER_NAME = f"consumer-{os.getpid()}"


def on_assign(consumer, partitions):
    assigned = [p.partition for p in partitions]
    print(f"[{CONSUMER_NAME}] assigned partitions: {assigned}")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC], on_assign=on_assign)

    print(f"[{CONSUMER_NAME}] Listening on topic '{TOPIC}'... (Ctrl+C to stop)")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{CONSUMER_NAME}] Consumer error: {msg.error()}")
                continue
            order = json.loads(msg.value())
            print(
                f"[{CONSUMER_NAME}] partition {msg.partition()} "
                f"offset {msg.offset()}: {order}"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
