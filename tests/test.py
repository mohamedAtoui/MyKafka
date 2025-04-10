import sys
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.producer.producer import SimpleProducer
from src.consumer.consumer import SimpleConsumer
from src.broker.broker import SimpleBroker

def start_broker():
    """Start the Kafka broker in a separate thread"""
    broker = SimpleBroker(host='localhost', port=9092)
    broker_thread = threading.Thread(target=broker.start)
    broker_thread.daemon = True
    broker_thread.start()
    time.sleep(1)  # Give broker time to start
    return broker

def producer_task(producer_id, topic, partition, messages):
    """Simulate a producer sending messages"""
    producer = SimpleProducer()
    producer.connect()
    
    for msg in messages:
        response = producer.send(topic, f"Producer-{producer_id}: {msg}", partition)
        print(f"Producer {producer_id} sent message to partition {partition}: {response}")
        time.sleep(0.1)
    
    producer.disconnect()

def consumer_task(consumer_id, topic, partitions):
    """Simulate a consumer reading messages"""
    consumer = SimpleConsumer()
    consumer.connect()
    
    # Subscribe to multiple partitions
    for partition in partitions:
        consumer.subscribe(topic, partition)
    
    messages_received = 0
    start_time = time.time()
    
    while time.time() - start_time < 10:  # Run for 10 seconds
        messages = consumer.poll()
        if messages:
            for msg in messages:
                print(f"Consumer {consumer_id} received: {msg['value']} (offset: {msg['offset']})")
                messages_received += 1
        time.sleep(0.1)
    
    consumer.disconnect()
    return messages_received

def demonstrate_kafka_features():
    """Demonstrate key Kafka features"""
    print("Starting Kafka Features Demonstration...")
    
    # 1. Start the broker
    broker = start_broker()
    print("\n1. Broker started successfully")
    
    # 2. Create a producer and set up topics
    setup_producer = SimpleProducer()
    setup_producer.connect()
    
    # Demonstrate multi-partition topic creation
    topic_name = "demo-topic"
    num_partitions = 3
    response = setup_producer.create_topic(topic_name, partitions=num_partitions)
    print(f"\n2. Created topic with {num_partitions} partitions: {response}")
    setup_producer.disconnect()
    
    # 3. Demonstrate parallel message production
    print("\n3. Demonstrating parallel message production")
    with ThreadPoolExecutor(max_workers=3) as executor:
        # Start multiple producers writing to different partitions
        producer_futures = []
        for i in range(3):
            messages = [f"Message-{j}" for j in range(5)]
            future = executor.submit(producer_task, i, topic_name, i, messages)
            producer_futures.append(future)
        
        # Wait for producers to complete
        for future in producer_futures:
            future.result()
    
    # 4. Demonstrate consumer groups and parallel consumption
    print("\n4. Demonstrating consumer groups and parallel consumption")
    with ThreadPoolExecutor(max_workers=2) as executor:
        # Start multiple consumers reading from different partition sets
        consumer_futures = []
        
        # Consumer 1 reads from partitions 0 and 1
        future1 = executor.submit(consumer_task, 1, topic_name, [0, 1])
        consumer_futures.append(future1)
        
        # Consumer 2 reads from partition 2
        future2 = executor.submit(consumer_task, 2, topic_name, [2])
        consumer_futures.append(future2)
        
        # Wait for consumers and get message counts
        total_messages = sum(future.result() for future in consumer_futures)
    
    print(f"\n5. Demonstration completed. Total messages processed: {total_messages}")
    
    # 6. Verify persistence
    print("\n6. Verifying message persistence:")
    data_dir = "kafka_data"
    if os.path.exists(data_dir):
        for partition in range(num_partitions):
            partition_file = os.path.join(data_dir, topic_name, f"partition-{partition}.log")
            if os.path.exists(partition_file):
                with open(partition_file, 'r') as f:
                    messages = f.readlines()
                    print(f"Partition {partition} has {len(messages)} persisted messages")

if __name__ == "__main__":
    demonstrate_kafka_features()