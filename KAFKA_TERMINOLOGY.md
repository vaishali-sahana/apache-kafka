## 1. What is Kafka?

**Apache Kafka is a distributed event-streaming platform** used to **publish, store, process, and consume large volumes of data/events in real time**.

In simple terms:

> Kafka acts like a highly scalable, fault-tolerant **message/event pipeline** between applications.

For example, imagine an e-commerce application:

```text
Customer places order
        ↓
     Order API
        ↓
      Kafka
   ┌────┼─────┐
   ↓    ↓     ↓
Payment Inventory Notification
Service  Service   Service
```

Instead of the Order API directly calling all three services, it publishes an event:

```text
OrderCreated
```

Kafka stores that event, and different services consume it independently.

---

# 2. Kafka Producer

A **producer** sends messages/events to Kafka.

Example:

```python
producer.send(
    "orders",
    {
        "order_id": 101,
        "amount": 5000
    }
)
```

Conceptually:

```text
Python Application
       |
       | event
       ↓
     Kafka
```

A producer doesn't normally send a message directly to a particular consumer.

It sends the event to a **topic**.

```text
Producer
   |
   ↓
orders topic
```

---

# 3. Kafka Consumer

A consumer reads messages from Kafka.

For example:

```text
orders topic
      |
      ↓
Payment Consumer
```

Another application could also consume the same topic:

```text
orders topic
      |
      ├──→ Payment Consumer
      |
      ├──→ Inventory Consumer
      |
      └──→ Analytics Consumer
```

This is one of Kafka's biggest advantages.

---

# 4. What is a Kafka Topic?

A **topic is a logical category/name for events**.

For example:

```text
orders
payments
users
transactions
notifications
logs
```

You can think of a topic somewhat like a named stream.

Example:

```text
Topic: orders

OrderCreated
OrderCreated
OrderCancelled
OrderCreated
OrderUpdated
```

A producer writes to the topic:

```text
Producer
   ↓
orders
```

Consumers read from the topic:

```text
orders
   ↓
Consumer
```

---

# 5. Kafka Partition

A topic is divided into **partitions**.

For example:

```text
Topic: orders

Partition 0
-------------
Order 1
Order 4
Order 7

Partition 1
-------------
Order 2
Order 5
Order 8

Partition 2
-------------
Order 3
Order 6
Order 9
```

Partitions are extremely important for Kafka scalability.

---

# 6. Why does Kafka use partitions?

Suppose one topic receives:

```text
1 million events/second
```

One machine may not be enough.

Kafka can divide the workload:

```text
             orders
          /     |     \
         /      |      \
        ↓       ↓       ↓
       P0      P1      P2
```

Different brokers can host different partitions.

Therefore Kafka can process large amounts of data in parallel.

---

# 7. Kafka Offset

Every message in a partition has an **offset**.

Example:

```text
Partition 0

Offset    Message
-------------------
0         Order A
1         Order B
2         Order C
3         Order D
4         Order E
```

The offset identifies the position of a message within that partition.

A consumer keeps track of where it is.

For example:

```text
Consumer processed:

0
1
2
```

Next message:

```text
3
```

---

# 8. Consumer Group

This is one of the **most important Kafka concepts**.

Suppose you have:

```text
Topic: orders

P0
P1
P2
```

And three consumers belong to the same consumer group:

```text
Consumer Group: payment-group

Consumer 1
Consumer 2
Consumer 3
```

Kafka can assign:

```text
P0 → Consumer 1
P1 → Consumer 2
P2 → Consumer 3
```

Therefore processing happens in parallel.

---

# 9. Kafka Broker

A **broker is a Kafka server**.

A Kafka cluster may contain multiple brokers.

Example:

```text
Kafka Cluster

Broker 1
Broker 2
Broker 3
Broker 4
```

Kafka distributes partitions among brokers.

For example:

```text
Broker 1 → P0
Broker 2 → P1
Broker 3 → P2
```

---

# 10. Kafka Cluster

Multiple brokers together form a Kafka cluster.

```text
              Kafka Cluster
       ┌─────────┼─────────┐
       ↓         ↓         ↓
    Broker 1  Broker 2  Broker 3
       |         |         |
      P0        P1        P2
```

This gives Kafka scalability and fault tolerance.

---

# 11. Replication

Kafka can maintain multiple copies of a partition.

Suppose:

```text
Replication Factor = 3
```

Then:

```text
Partition P0

Broker 1 → Leader
Broker 2 → Replica
Broker 3 → Replica
```

If Broker 1 fails, another replica can become the leader.

---

# 12. Leader and Follower

For each partition, Kafka has a leader replica and follower replicas.

Example:

```text
P0

Broker 1
   ↓
 Leader

Broker 2
   ↓
 Follower

Broker 3
   ↓
 Follower
```

Producers and consumers generally interact with the partition leader.

Followers replicate the data.

---

# 13. What happens if a broker fails?

Suppose:

```text
P0 Leader → Broker 1
```

Broker 1 crashes.

Kafka can elect another replica:

```text
Broker 2
   ↓
 New Leader
```

This provides fault tolerance.

---

# 14. Replication Factor

Replication factor specifies how many copies of partition data Kafka maintains.

Example:

```text
Replication Factor = 3
```

means:

```text
1 Leader
2 Followers
```

Example:

```text
P0
├── Broker 1 → Leader
├── Broker 2 → Replica
└── Broker 3 → Replica
```

---

# 15. Kafka Retention

Kafka doesn't necessarily delete an event immediately after consumption.

Instead, Kafka retains data based on policies such as:

```text
retention time
retention size
```

Example:

```text
Retention = 7 days
```

Kafka keeps the event for up to seven days, subject to the configured retention policy.

This allows consumers to replay old events.

---

# 16. Producer Acknowledgment

Kafka producers can configure acknowledgments.

Common settings:

```text
acks=0
acks=1
acks=all
```

### acks=0

Producer doesn't wait for broker acknowledgment.

```text
Producer → Kafka
```

Fast, but weaker delivery assurance.

### acks=1

Leader acknowledges the message.

```text
Producer
   ↓
Leader
   ↓
ACK
```

### acks=all

The leader waits for the required in-sync replicas to acknowledge the write.

This provides stronger durability.

---

# 18. Kafka use cases

Kafka is commonly used for:

### 1. Microservices communication

```text
Order Service
     ↓
 Kafka
     ↓
Payment Service
```

### 2. Real-time analytics

```text
Applications
     ↓
   Kafka
     ↓
Streaming processing
     ↓
Analytics
```

### 3. Data pipelines

```text
Database
   ↓
Kafka
   ↓
ETL/ELT
   ↓
Data Warehouse
```

### 4. IoT

```text
IoT Devices
     ↓
   Kafka
     ↓
Processing
```
