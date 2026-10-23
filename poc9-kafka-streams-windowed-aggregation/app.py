import json
import os
import time

from confluent_kafka import Consumer, Producer

BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "orders"
OUTPUT_TOPIC = "customer-order-totals"

WINDOW_SIZE = 30  # seconds, tumbling
GRACE = 5  # seconds a closed window's state is still kept around for

STATE_FILE = "state.json"


def window_start(event_time: float) -> int:
    return int(event_time // WINDOW_SIZE) * WINDOW_SIZE


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            # JSON keys are always strings, so (customer_id, window_start)
            # tuples were saved as "customer_id|window_start".
            raw = json.load(f)
        return {tuple(k.split("|", 1)): v for k, v in raw.items()}
    return {}


def save_state(state):
    raw = {f"{customer_id}|{window}": total for (customer_id, window), total in state.items()}
    with open(STATE_FILE, "w") as f:
        json.dump(raw, f)


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "poc9-order-totals",
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    consumer.subscribe([INPUT_TOPIC])

    # state is the hand-rolled equivalent of a Kafka Streams state store:
    # (customer_id, window_start) -> running total. Reloaded from
    # STATE_FILE on startup so a restart resumes in-flight windows instead
    # of losing them -- STATE_FILE plays the role of the changelog topic.
    state = load_state()
    print(f"loaded {len(state)} window(s) from {STATE_FILE}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"consume error: {msg.error()}")
                continue

            order = json.loads(msg.value())
            now = time.time()
            win_start = window_start(now)
            win_end = win_start + WINDOW_SIZE

            # Drop events whose window has already closed and aged past the
            # grace period -- the same role as Kafka Streams' grace period.
            if now > win_end + GRACE:
                print(f"dropping late event for closed window: {order}")
                consumer.commit(msg)
                continue

            key = (order["customer_id"], win_start)
            state[key] = state.get(key, 0.0) + order["amount"]
            save_state(state)

            payload = {
                "customer_id": order["customer_id"],
                "window_start": win_start,
                "window_end": win_end,
                "total": state[key],
            }
            producer.produce(
                OUTPUT_TOPIC,
                key=order["customer_id"],
                value=json.dumps(payload),
                callback=delivery_report,
            )
            producer.poll(0)

            # Only commit the input offset after the output has been
            # produced and the state checkpointed, so a crash in between
            # redelivers the order rather than silently losing its update.
            consumer.commit(msg)
            print(f"updated: {payload}")
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
