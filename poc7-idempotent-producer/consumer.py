import json
import os

from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "payments"
GROUP_ID = "payments-group"
LEDGER_FILE = "ledger.json"


CRASH_AFTER = int(os.environ.get("CRASH_AFTER", "0"))

IDEMPOTENT = os.environ.get("IDEMPOTENT", "true").lower() != "false"


def load_ledger():
    if os.path.exists(LEDGER_FILE):
        with open(LEDGER_FILE) as f:
            return json.load(f)
    return {"balance": 0, "applied_payment_ids": []}


def save_ledger(ledger):
    with open(LEDGER_FILE, "w") as f:
        json.dump(ledger, f)


def apply_payment(ledger, payment):
    payment_id = payment["payment_id"]

    if IDEMPOTENT and payment_id in ledger["applied_payment_ids"]:
        print(f"[consumer] payment_id={payment_id} already applied, skipping (idempotent)")
        return

    ledger["balance"] += payment["amount"]
    ledger["applied_payment_ids"].append(payment_id)
    print(f"[consumer] applied payment_id={payment_id}, balance is now {ledger['balance']}")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            # Commit offsets manually, only after a message has been fully
            # applied to the ledger, so a crash between apply and commit is
            # exactly the window this POC demonstrates.
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])

    ledger = load_ledger()
    print(f"[consumer] starting balance: {ledger['balance']} (idempotent={IDEMPOTENT})")

    processed = 0
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[consumer] error: {msg.error()}")
                continue

            payment = json.loads(msg.value())
            processed += 1
            apply_payment(ledger, payment)
            save_ledger(ledger)

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
