"""Kafka Consumer for Seat Service"""

import json
import os
from kafka import KafkaConsumer
from app.service import SeatService
from app.producer import KafkaProducerClient


def main():
    """Main consumer loop"""
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
    
    consumer = KafkaConsumer(
        "booking-requests",
        bootstrap_servers=kafka_brokers,
        group_id="seat-service",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    
    producer = KafkaProducerClient()
    producer.connect()
    
    seat_service = SeatService()
    
    try:
        for message in consumer:
            booking_data = message.value
            print(f"Checking seat availability: {booking_data}")
            
            # Check seat availability
            available = seat_service.check_availability(
                booking_data.get("flight_id"),
                booking_data.get("seat_id")
            )
            
            # Send seat confirmation
            confirmation = {
                "flight_id": booking_data.get("flight_id"),
                "seat_id": booking_data.get("seat_id"),
                "available": available
            }
            producer.send_message("seat-availability", confirmation)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
