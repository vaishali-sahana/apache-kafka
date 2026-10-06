import json
import os

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

BOOTSTRAP_SERVERS = "localhost:9092"
SOURCE_TOPIC = "payments"
SINK_TOPIC = "payments-audited"
GROUP_ID = "payments-audit-group"

CRASH_AFTER = int(os.environ.get("CRASH_AFTER", "0"))


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            # Only ever read records from committed transactions, never the
            # dangling writes of a producer that crashed mid-transaction.
            "isolation.level": "read_committed",
        }
    )
    consumer.subscribe([SOURCE_TOPIC])

    producer = Producer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "enable.idempotence": True,
            "transactional.id": "payments-audit-tx-1",
        }
    )
    producer.init_transactions()

    processed = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            payment = json.loads(msg.value())

            producer.begin_transaction()
            producer.produce(
                SINK_TOPIC,
                key=msg.key(),
                value=json.dumps({**payment, "audited": True}),
            )
            # The consumer's offset commit travels inside the SAME
            # transaction as the produce, so "consumed + produced" becomes
            # one atomic unit: either both happen, or neither does. A crash
            # between produce() and this call leaves the transaction open;
            # it is aborted on recovery and the input offset is never
            # advanced, so the message is re-read and reprocessed from
            # scratch (no partial audited record, no skipped input).
            producer.send_offsets_to_transaction(
                [TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)],
                consumer.consumer_group_metadata(),
            )

            processed += 1
            if CRASH_AFTER and processed == CRASH_AFTER:
                print(
                    f"[tx-pipeline] simulating crash after {processed} messages, "
                    "transaction left open (will be aborted by the broker)"
                )
                return

            producer.commit_transaction()
            print(
                f"[tx-pipeline] committed payment_id={payment['payment_id']} "
                f"to {SINK_TOPIC} + advanced input offset, atomically"
            )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
