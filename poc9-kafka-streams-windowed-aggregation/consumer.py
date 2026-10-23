import json

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customer-order-totals"


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "poc9-totals-watcher",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])
    print(f"watching {TOPIC} ... Ctrl+C to stop")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"error: {msg.error()}")
                continue
            total = json.loads(msg.value())
            print(total)
            # print(f"customer={total['customer_id']} total={total['total']} as_of={total['as_of']}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
