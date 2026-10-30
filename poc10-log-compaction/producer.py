import json
import time

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customer-profiles"


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed for key={msg.key()}: {err}")


def send(producer: Producer, key: str, value: dict | None) -> None:
    producer.produce(
        TOPIC,
        key=key.encode("utf-8"),
        # A None value is a tombstone: it marks the key for deletion once
        # the log cleaner compacts this segment.
        value=json.dumps(value).encode("utf-8") if value is not None else None,
        callback=delivery_report,
    )


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    # Three updates for "alice" (only the last should survive compaction),
    # one update for "bob", then a tombstone for "carol" after her one
    # update (she should disappear entirely once compacted).
    updates = [
        ("alice", {"email": "alice@old.example", "plan": "free"}),
        ("bob", {"email": "bob@example.com", "plan": "pro"}),
        ("alice", {"email": "alice@old.example", "plan": "pro"}),
        ("carol", {"email": "carol@example.com", "plan": "free"}),
        ("alice", {"email": "alice@new.example", "plan": "pro"}),
        ("carol", None),  # tombstone: delete carol
    ]

    for key, value in updates:
        send(producer, key, value)
        producer.flush()
        label = "tombstone" if value is None else value
        print(f"[producer] sent key={key} value={label}")
        time.sleep(0.2)


    print("[producer] padding segment so it rolls and becomes eligible for compaction...")
    for i in range(50):
        producer.produce(
            TOPIC,
            key=f"_pad-{i}".encode("utf-8"),
            value=(b"x" * 200),
            callback=delivery_report,
        )
        # Flush each pad record individually so librdkafka never batches
        # several of them into one produce request: a batch larger than
        # segment.bytes is rejected outright by the broker as
        # RECORD_LIST_TOO_LARGE, which silently dropped every pad record
        # when they were all queued up and flushed together.
        producer.flush(5)
    print("[producer] padding done")


if __name__ == "__main__":
    main()
