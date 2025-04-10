import socket
import json
import time

class SimpleProducer:
    def __init__(self, host='localhost', port=9092):
        self.host = host
        self.port = port
        
    def connect(self):
        """Connect to the broker"""
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((self.host, self.port))
        
    def disconnect(self):
        """Disconnect from the broker"""
        if hasattr(self, 'socket'):
            self.socket.close()
            
    def create_topic(self, topic, partitions=1):
        """Create a new topic"""
        message = {
            'type': 'create_topic',
            'topic': topic,
            'partitions': partitions
        }
        
        self.socket.send(json.dumps(message).encode('utf-8'))
        response = json.loads(self.socket.recv(4096).decode('utf-8'))
        return response
        
    def send(self, topic, value, partition=0):
        """Send a message to a topic"""
        message = {
            'type': 'produce',
            'topic': topic,
            'partition': partition,
            'value': value
        }
        
        self.socket.send(json.dumps(message).encode('utf-8'))
        response = json.loads(self.socket.recv(4096).decode('utf-8'))
        return response

if __name__ == "__main__":
    producer = SimpleProducer()
    producer.connect()
    
    # Create a topic
    producer.create_topic("test-topic", partitions=3)
    
    # Send some messages
    for i in range(10):
        response = producer.send("test-topic", f"Message {i}", partition=i % 3)
        print(f"Sent message, response: {response}")
        time.sleep(0.5)
        
    producer.disconnect()
