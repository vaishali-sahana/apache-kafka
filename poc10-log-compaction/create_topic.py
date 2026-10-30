from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customer-profiles"


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    topic = NewTopic(
        TOPIC,
        num_partitions=1,
        replication_factor=1,
        config={
            "cleanup.policy": "compact",
            # Small segment so the demo doesn't have to wait for a large
            # segment to roll before the cleaner can compact it.
            "segment.bytes": "10240",
            "min.cleanable.dirty.ratio": "0.01",
        },
    )

    futures = admin.create_topics([topic])
    for name, future in futures.items():
        try:
            future.result()
            print(f"[create_topic] created compacted topic '{name}'")
        except Exception as exception:
            print(f"[create_topic] failed to create '{name}': {exception}")


if __name__ == "__main__":
    main()
