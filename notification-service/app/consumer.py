"""Kafka Consumer for Notification Service"""

import json
import os
from kafka import KafkaConsumer
from app.service import NotificationService


def main():
    """Main consumer loop"""
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
    
    consumer = KafkaConsumer(
        "booking-confirmations",
        "payment-confirmations",
        bootstrap_servers=kafka_brokers,
        group_id="notification-service",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    
    notification_service = NotificationService()
    
    try:
        for message in consumer:
            data = message.value
            print(f"Sending notification for: {data}")
            
            # Send notification
            notification_service.send_notification(
                user_id=data.get("user_id"),
                message=f"Status: {data.get('status')}"
            )
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
