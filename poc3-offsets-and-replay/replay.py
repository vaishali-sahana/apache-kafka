import json

from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events"
GROUP_ID = "events-group"

# Offset to rewind every assigned partition to before replaying. Set to 0 to
# replay from the beginning of the topic, or any other offset to replay from there.
TARGET_OFFSET = 0


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    # Trigger partition assignment before we can seek.
    consumer.poll(timeout=5.0)
    partitions = consumer.assignment()
    if not partitions:
        print("[replay] no partitions assigned yet, retrying...")
        consumer.poll(timeout=5.0)
        partitions = consumer.assignment()

    for tp in partitions:
        seek_tp = TopicPartition(tp.topic, tp.partition, TARGET_OFFSET)
        consumer.seek(seek_tp)
        print(f"[replay] seeked partition {tp.partition} to offset {TARGET_OFFSET}")

    print("[replay] replaying events... (Ctrl+C to stop)")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[replay] error: {msg.error()}")
                continue

            event = json.loads(msg.value())
            print(f"[replay] replayed partition {msg.partition()} offset {msg.offset()}: {event}")
            consumer.commit(message=msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
