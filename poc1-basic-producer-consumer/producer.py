import json
import time

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"Delivered to {msg.topic()} [partition {msg.partition()}] "
            f"at offset {msg.offset()}"
        )


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    for order_id in range(1, 11):
        order = {"order_id": order_id, "amount": order_id * 100}
        producer.produce(
            TOPIC,
            key=str(order_id),
            value=json.dumps(order),
            callback=delivery_report,
        )
        time.sleep(0.5)

    producer.flush()


if __name__ == "__main__":
    main()
