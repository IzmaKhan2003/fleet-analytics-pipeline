"""Kafka Producer for Fleet Analytics"""
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

class FleetKafkaProducer:
    def __init__(self, bootstrap_servers="localhost:9092"):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            acks='all',
            retries=3
        )
        print("✓ Kafka Producer connected")
    
    def send_telemetry(self, vehicle_id, data):
        try:
            future = self.producer.send('vehicle-telemetry', value=data, key=vehicle_id.encode())
            future.get(timeout=10)
            return True
        except KafkaError as e:
            print(f"Error: {e}")
            return False
    
    def send_delivery(self, data):
        try:
            future = self.producer.send('deliveries', value=data)
            future.get(timeout=10)
            return True
        except KafkaError as e:
            print(f"Error: {e}")
            return False
    
    def send_incident(self, data):
        try:
            future = self.producer.send('incidents', value=data)
            future.get(timeout=10)
            return True
        except KafkaError as e:
            print(f"Error: {e}")
            return False
    
    def close(self):
        self.producer.flush()
        self.producer.close()

if __name__ == "__main__":
    print("Testing Kafka Producer...")
    
    producer = FleetKafkaProducer()
    
    # Test telemetry
    test_data = {
        'vehicle_id': 'VEH-00001',
        'speed': 75.5,
        'latitude': 24.8607,
        'longitude': 67.0011
    }
    
    if producer.send_telemetry('VEH-00001', test_data):
        print("✓ Test message sent successfully!")
    
    producer.close()
    print("✓ Producer test complete!")
