from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "customer-orders"
NUM_PARTITIONS = 3


def main():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    new_topic = NewTopic(TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=1)
    futures = admin.create_topics([new_topic])

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Created topic '{topic}' with {NUM_PARTITIONS} partitions")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")


if __name__ == "__main__":
    main()
