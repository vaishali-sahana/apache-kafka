import json
import os

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events"
GROUP_ID = "events-group"

# Set CRASH_AFTER=N to exit right after processing (but before committing)
# the Nth message, to simulate a crash and show replay of uncommitted work.
CRASH_AFTER = int(os.environ.get("CRASH_AFTER", "0"))


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            # Commit offsets manually, only after a message has been fully processed.
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    print(f"[consumer] Listening on topic '{TOPIC}' (manual commits)... (Ctrl+C to stop)")
    processed = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[consumer] error: {msg.error()}")
                continue

            event = json.loads(msg.value())
            processed += 1
            print(f"[consumer] processing partition {msg.partition()} offset {msg.offset()}: {event}")

            if CRASH_AFTER and processed == CRASH_AFTER:
                print(f"[consumer] simulating crash after {processed} messages, offset NOT committed")
                return

            consumer.commit(message=msg)
            print(f"[consumer] committed offset {msg.offset() + 1}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
