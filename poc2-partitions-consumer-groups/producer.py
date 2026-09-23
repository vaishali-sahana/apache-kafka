import json
import time

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customer-orders"
CUSTOMERS = ["cust-a", "cust-b", "cust-c", "cust-d", "cust-e", "cust-f"]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"Delivered key={msg.key().decode()} to partition {msg.partition()} "
            f"at offset {msg.offset()}"
        )


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    for order_id in range(1, 31):
        customer_id = CUSTOMERS[order_id % len(CUSTOMERS)]
        order = {"order_id": order_id, "customer_id": customer_id, "amount": order_id * 10}
        producer.produce(
            TOPIC,
            key=customer_id,
            value=json.dumps(order),
            callback=delivery_report,
        )
        time.sleep(0.2)

    producer.flush()


if __name__ == "__main__":
    main()
