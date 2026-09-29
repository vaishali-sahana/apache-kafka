import json

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "events"
DLQ_TOPIC = "events-dlq"
GROUP_ID = "events-group"

# How many times to retry a record before giving up on it and routing it to
# the dead-letter topic instead of blocking the partition forever.
MAX_RETRIES = 3


def process(event: dict) -> None:
    """Business logic that a poison pill (event["poison"] is True) always fails."""
    if event.get("poison"):
        raise ValueError(f"cannot process event {event.get('event_id')}: poisoned payload")


def send_to_dlq(dlq_producer: Producer, msg, reason: str) -> None:
    dlq_producer.produce(
        DLQ_TOPIC,
        key=msg.key(),
        value=msg.value(),
        headers=[
            ("dlq-reason", reason.encode("utf-8")),
            ("dlq-source-topic", msg.topic().encode("utf-8")),
            ("dlq-source-partition", str(msg.partition()).encode("utf-8")),
            ("dlq-source-offset", str(msg.offset()).encode("utf-8")),
        ],
    )
    dlq_producer.flush()
    print(f"[consumer] sent partition {msg.partition()} offset {msg.offset()} to DLQ: {reason}")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    consumer.subscribe([TOPIC])

    # Retry counts per (partition, offset), so a poison pill gets a bounded
    # number of attempts instead of looping forever or being skipped on the
    # very first failure.
    retry_counts = {}

    print(f"[consumer] Listening on topic '{TOPIC}' (max {MAX_RETRIES} retries, then DLQ)... (Ctrl+C to stop)")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            key = (msg.partition(), msg.offset())

            # Deserialization failure: the payload itself is corrupt, so
            # retrying won't help. Treat it the same as an exhausted retry.
            try:
                event = json.loads(msg.value())
            except (json.JSONDecodeError, UnicodeDecodeError) as exception:
                print(f"[consumer] deserialization error at partition {msg.partition()} offset {msg.offset()}: {exception}")
                send_to_dlq(dlq_producer, msg, f"deserialization error: {exception}")
                consumer.commit(message=msg)
                retry_counts.pop(key, None)
                continue

            try:
                process(event)
            except Exception as exception:
                attempts = retry_counts.get(key, 0) + 1
                retry_counts[key] = attempts
                print(f"[consumer] processing error (attempt {attempts}/{MAX_RETRIES}) at partition {msg.partition()} offset {msg.offset()}: {exception}")

                if attempts < MAX_RETRIES:
                    # Don't commit: seek back to this offset so the same
                    # record is redelivered and retried on the next poll.
                    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
                    continue

                # Retries exhausted: quarantine the record in the DLQ and
                # commit past it so the partition can make progress again.
                send_to_dlq(dlq_producer, msg, f"exhausted {MAX_RETRIES} retries: {exception}")
                consumer.commit(message=msg)
                retry_counts.pop(key, None)
                continue

            print(f"[consumer] processed partition {msg.partition()} offset {msg.offset()}: {event}")
            consumer.commit(message=msg)
            retry_counts.pop(key, None)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
