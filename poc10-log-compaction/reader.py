import uuid

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customer-profiles"


def on_assign(consumer, partitions):
    print(f"[reader] assigned partitions: {[(p.topic, p.partition) for p in partitions]}")
    consumer.assign(partitions)


def main():
    # A unique group id every run, with no committed offsets, so each read
    # starts from the beginning of the log and shows exactly what the
    # broker currently retains -- not where some prior consumer left off.
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"compaction-reader-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC], on_assign=on_assign)
    print(f"[reader] reading all records currently retained in '{TOPIC}'...")
    seen_keys = []

    empty_polls = 0
    try:
        while empty_polls < 10:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                empty_polls += 1
                continue
            empty_polls = 0
            if msg.error():
                print(f"[reader] error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8")
            if key.startswith("_pad-"):
                continue

            value = msg.value().decode("utf-8") if msg.value() is not None else "<tombstone>"
            seen_keys.append(key)
            print(f"[reader] offset={msg.offset()} key={key} value={value}")
    finally:
        consumer.close()

    print(f"[reader] done. {len(seen_keys)} non-padding record(s) retained in the log right now.")


if __name__ == "__main__":
    main()
