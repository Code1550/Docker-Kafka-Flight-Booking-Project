"""Kafka Consumer for Booking Service"""

import json
import os
from kafka import KafkaConsumer
from app.service import BookingService
from app.producer import KafkaProducerClient


def main():
    """Main consumer loop"""
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
    
    consumer = KafkaConsumer(
        "booking-requests",
        bootstrap_servers=kafka_brokers,
        group_id="booking-service",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    
    producer = KafkaProducerClient()
    producer.connect()
    
    booking_service = BookingService()
    
    try:
        for message in consumer:
            booking_data = message.value
            print(f"Received booking request: {booking_data}")
            
            # Process booking
            booking_id = booking_service.create_booking(booking_data)
            
            # Send confirmation
            confirmation = {
                "booking_id": booking_id,
                "status": "confirmed",
                "user_id": booking_data.get("user_id")
            }
            producer.send_message("booking-confirmations", confirmation)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
