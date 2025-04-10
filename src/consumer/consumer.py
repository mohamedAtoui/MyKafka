import socket
import json
import time
import uuid

class SimpleConsumer:
    def __init__(self, host='localhost', port=9092):
        self.host = host
        self.port = port
        self.consumer_id = str(uuid.uuid4())
        self.offsets = {}  # topic-partition -> offset
        
    def connect(self):
        """Connect to the broker"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        
    def disconnect(self):
        """Disconnect from the broker"""
        if hasattr(self, 'socket'):
            self.socket.close()
            
    def subscribe(self, topic, partition=0):
        """Subscribe to a topic-partition"""
        self.offsets.setdefault(f"{topic}-{partition}", 0)
        
    def poll(self, timeout_ms=1000):
        """Poll for new messages"""
        messages = []
        
        for topic_partition, offset in self.offsets.items():
            # Split from the right side once, to handle topic names with hyphens
            parts = topic_partition.rsplit('-', 1)
            if len(parts) != 2:
                continue  # Skip invalid topic-partition format
            
            topic, partition = parts
            partition = int(partition)
            
            message = {
                'type': 'consume',
                'topic': topic,
                'partition': partition,
                'offset': offset,
                'consumer_id': self.consumer_id
            }
            
            self.socket.send(json.dumps(message).encode('utf-8'))
            response = json.loads(self.socket.recv(4096).decode('utf-8'))
            
            if response['status'] == 'success' and response['messages']:
                messages.extend(response['messages'])
                self.offsets[topic_partition] = response['new_offset']
                
        return messages

if __name__ == "__main__":
    consumer = SimpleConsumer()
    consumer.connect()
    
    # Subscribe to a topic
    consumer.subscribe("test-topic", partition=0)
    consumer.subscribe("test-topic", partition=1)
    consumer.subscribe("test-topic", partition=2)
    
    # Poll for messages
    try:
        while True:
            messages = consumer.poll()
            if messages:
                for msg in messages:
                    print(f"Received: {msg['value']} (offset: {msg['offset']})")
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.disconnect()
