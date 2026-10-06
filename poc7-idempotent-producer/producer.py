import json
import time

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "payments"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(
            f"Delivered key={msg.key().decode()} to partition {msg.partition()} "
            f"at offset {msg.offset()}"
        )


def main():
    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            # Turns on the idempotent producer: the broker assigns this
            # producer a PID + per-partition sequence numbers, and drops any
            # produce request it has already written for that PID/sequence.
            # A retried send (e.g. after a timeout where the broker actually
            # wrote the record but the ack was lost) is deduped at the broker,
            # not written twice.
            "enable.idempotence": True,
            "acks": "all",
            "retries": 5,
        }
    )

    for payment_id in range(1, 11):
        payment = {"payment_id": payment_id, "amount": payment_id * 50}
        producer.produce(
            TOPIC,
            key=str(payment_id),
            value=json.dumps(payment),
            callback=delivery_report,
        )
        time.sleep(0.1)

    producer.flush()


if __name__ == "__main__":
    main()
