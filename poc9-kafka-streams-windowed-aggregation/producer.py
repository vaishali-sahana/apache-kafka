import json
import time

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"

ORDERS = [
    ("alice", 10.0),
    ("bob", 25.0),
    ("alice", 5.0),
    ("bob", 15.0),
    ("alice", 20.0),
]

def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")
    else:
        print(f"delivered to {msg.topic()}[{msg.partition()}]@{msg.offset()}: {msg.value()}")

def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for order_id, (customer_id, amount) in enumerate(ORDERS, start=1):
        payload = {"order_id": order_id, "customer_id": customer_id, "amount": amount}
        producer.produce(TOPIC, key=customer_id, value=json.dumps(payload), callback=delivery_report)
        producer.poll(0)
        time.sleep(2)
    producer.flush()

if __name__ == "__main__":
    main()
