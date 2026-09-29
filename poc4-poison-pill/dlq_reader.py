from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
DLQ_TOPIC = "events-dlq"
GROUP_ID = "dlq-reader-group"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([DLQ_TOPIC])

    print(f"[dlq_reader] Listening on '{DLQ_TOPIC}'... (Ctrl+C to stop)")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[dlq_reader] error: {msg.error()}")
                continue

            headers = {k: v.decode("utf-8") for k, v in (msg.headers() or [])}
            print(f"[dlq_reader] key={msg.key()} value={msg.value()} headers={headers}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
