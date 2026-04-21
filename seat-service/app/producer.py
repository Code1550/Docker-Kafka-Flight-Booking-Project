"""Kafka Producer for Seat Service"""

import json
import os
from kafka import KafkaProducer


class KafkaProducerClient:
    def __init__(self):
        self.kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
        self.producer = None
    
    def connect(self):
        """Connect to Kafka"""
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_brokers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def close(self):
        """Close Kafka connection"""
        if self.producer:
            self.producer.close()
    
    def send_message(self, topic: str, message: dict):
        """Send message to Kafka topic"""
        future = self.producer.send(topic, value=message)
        future.get(timeout=10)
