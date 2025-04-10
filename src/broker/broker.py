import socket
import threading
import json
import time
import os
from collections import defaultdict, deque

class SimpleBroker:
    def __init__(self, host='localhost', port=9092):
        self.host = host
        self.port = port
        self.topics = defaultdict(lambda: defaultdict(deque))  # topic -> partition -> messages
        self.consumers = {}  # consumer_id -> {topic, partition, offset}
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.running = False
        self.data_dir = "kafka_data"
        os.makedirs(self.data_dir, exist_ok=True)
        
    def start(self):
        """Start the broker and listen for connections"""
        self.socket.bind((self.host, self.port))
        self.socket.listen(5)
        self.running = True
        print(f"Broker started on {self.host}:{self.port}")
        
        try:
            while self.running:
                client_socket, address = self.socket.accept()
                client_thread = threading.Thread(target=self.handle_client, args=(client_socket, address))
                client_thread.daemon = True
                client_thread.start()
        except KeyboardInterrupt:
            print("Shutting down broker...")
        finally:
            self.socket.close()
            
    def handle_client(self, client_socket, address):
        """Handle client connections"""
        print(f"Connection from {address}")
        try:
            while self.running:
                data = client_socket.recv(4096)
                if not data:
                    break
                    
                message = json.loads(data.decode('utf-8'))
                response = self.process_message(message)
                client_socket.send(json.dumps(response).encode('utf-8'))
        except Exception as e:
            print(f"Error handling client: {e}")
        finally:
            client_socket.close()
            
    def process_message(self, message):
        """Process incoming messages based on their type"""
        msg_type = message.get('type')
        
        if msg_type == 'produce':
            return self.handle_produce(message)
        elif msg_type == 'consume':
            return self.handle_consume(message)
        elif msg_type == 'create_topic':
            return self.handle_create_topic(message)
        else:
            return {'status': 'error', 'message': 'Unknown message type'}
            
    def handle_produce(self, message):
        """Handle produce requests from producers"""
        topic = message.get('topic')
        partition = message.get('partition', 0)
        value = message.get('value')
        
        if not topic or value is None:
            return {'status': 'error', 'message': 'Missing topic or value'}
            
        # Add message to the topic-partition
        self.topics[topic][partition].append({
            'value': value,
            'timestamp': time.time(),
            'offset': len(self.topics[topic][partition])
        })
        
        # Persist to disk
        self.persist_message(topic, partition, value)
        
        return {
            'status': 'success', 
            'offset': len(self.topics[topic][partition]) - 1
        }
        
    def handle_consume(self, message):
        """Handle consume requests from consumers"""
        topic = message.get('topic')
        partition = message.get('partition', 0)
        offset = message.get('offset', 0)
        consumer_id = message.get('consumer_id')
        
        if not topic or consumer_id is None:
            return {'status': 'error', 'message': 'Missing topic or consumer_id'}
            
        # Register or update consumer
        self.consumers[consumer_id] = {
            'topic': topic,
            'partition': partition,
            'offset': offset
        }
        
        # Get messages from the requested offset
        if topic in self.topics and partition in self.topics[topic]:
            if offset < len(self.topics[topic][partition]):
                messages = list(self.topics[topic][partition])[offset:offset+10]
                return {
                    'status': 'success',
                    'messages': messages,
                    'new_offset': offset + len(messages)
                }
        
        return {'status': 'success', 'messages': [], 'new_offset': offset}
        
    def handle_create_topic(self, message):
        """Handle topic creation requests"""
        topic = message.get('topic')
        partitions = message.get('partitions', 1)
        
        if not topic:
            return {'status': 'error', 'message': 'Missing topic name'}
            
        # Create topic with specified partitions
        for i in range(partitions):
            self.topics[topic][i] = deque()
            
        return {'status': 'success', 'topic': topic, 'partitions': partitions}
        
    def persist_message(self, topic, partition, value):
        """Persist messages to disk (simplified)"""
        topic_dir = os.path.join(self.data_dir, topic)
        os.makedirs(topic_dir, exist_ok=True)
        
        partition_file = os.path.join(topic_dir, f"partition-{partition}.log")
        with open(partition_file, 'a') as f:
            f.write(f"{json.dumps(value)}\n")

if __name__ == "__main__":
    broker = SimpleBroker()
    broker.start()
