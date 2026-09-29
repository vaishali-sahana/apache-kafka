import json
import time

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events"


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

    for event_id in range(1, 11):
        if event_id == 7:
            value = b"{not-valid-json"
        else:
            event = {
                "event_id": event_id,
                "payload": f"event-{event_id}",
                "poison": event_id == 4,
            }
            value = json.dumps(event).encode("utf-8")

        producer.produce(
            TOPIC,
            key=str(event_id),
            value=value,
            callback=delivery_report,
        )
        time.sleep(0.1)

    producer.flush()


if __name__ == "__main__":
    main()
