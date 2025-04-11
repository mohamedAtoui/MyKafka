# Simple Kafka Imitation

This project implements a simplified imitation of Apache Kafka, demonstrating core Kafka concepts like topics, partitions, producers, consumers, and message persistence.

## Overview

This implementation provides a lightweight, educational version of Kafka's core functionality, including:

- Topic creation with multiple partitions
- Message production and consumption
- Parallel processing capabilities
- Message persistence to disk
- Consumer groups and offset tracking

## Architecture

The system consists of three main components:

1. **Broker** (`src/broker/broker.py`): The central message hub that manages topics, partitions, and message routing.
2. **Producer** (`src/producer/producer.py`): Clients that send messages to topics.
3. **Consumer** (`src/consumer/consumer.py`): Clients that read messages from topics.

### Broker

The broker is the heart of the system, responsible for:
- Managing topics and partitions
- Handling message storage and retrieval
- Tracking consumer offsets
- Persisting messages to disk

### Producer

Producers connect to the broker and can:
- Create new topics with specified partition counts
- Send messages to specific topic-partitions
- Receive acknowledgments with message offsets

### Consumer

Consumers connect to the broker and can:
- Subscribe to specific topic-partitions
- Poll for new messages
- Track their position (offset) in each partition
- Process messages in parallel

## How It Works

### Topic Creation

Topics are created with a specified number of partitions. Each partition is an ordered, immutable sequence of messages.

```python
producer = SimpleProducer()
producer.connect()
producer.create_topic("demo-topic", partitions=3)
```

### Message Production

Producers send messages to specific topic-partitions. The broker assigns an offset to each message and returns it to the producer.

```python
response = producer.send("demo-topic", "Hello, Kafka!", partition=0)
print(f"Message sent with offset: {response['offset']}")
```

### Message Consumption

Consumers subscribe to topic-partitions and poll for new messages. The broker tracks each consumer's offset to ensure messages are delivered in order.

```python
consumer = SimpleConsumer()
consumer.connect()
consumer.subscribe("demo-topic", partition=0)
messages = consumer.poll()
for msg in messages:
    print(f"Received: {msg['value']} (offset: {msg['offset']})")
```

### Parallel Processing

The system supports parallel message production and consumption:

- Multiple producers can write to different partitions simultaneously
- Multiple consumers can read from different partitions in parallel
- Consumer groups can distribute partitions among consumers

### Message Persistence

Messages are persisted to disk in the `kafka_data` directory, organized by topic and partition:

```
kafka_data/
  demo-topic/
    partition-0.log
    partition-1.log
    partition-2.log
```

## Demonstration

The `tests/test.py` file demonstrates the system's capabilities:

1. Starts a broker
2. Creates a topic with multiple partitions
3. Demonstrates parallel message production
4. Shows consumer groups and parallel consumption
5. Verifies message persistence

## Running the Demo

To run the demonstration:

```bash
python tests/test.py
```

## Implementation Details

### Communication Protocol

The system uses a simple JSON-based protocol over TCP sockets:

- Producers and consumers connect to the broker via TCP
- Messages are serialized as JSON
- The broker processes requests and returns JSON responses

### Message Format

Messages include:
- Type (produce, consume, create_topic)
- Topic name
- Partition number
- Message value
- Consumer ID (for consumer requests)
- Offset (for consumer requests)

### Offset Management

The broker tracks consumer offsets to ensure:
- Messages are delivered in order
- Each message is delivered exactly once
- Consumers can resume from where they left off

## Limitations

This is a simplified implementation and lacks many features of the real Kafka:

- No replication or fault tolerance
- No message compression
- No message retention policies
- No security features
- Limited scalability

## Future Improvements

Potential enhancements:
- Implement message compression
- Add message retention policies
- Support for message keys
- Implement consumer groups with rebalancing
- Add security features
