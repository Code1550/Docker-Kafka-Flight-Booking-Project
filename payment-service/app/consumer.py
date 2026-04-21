"""Kafka Consumer for Payment Service"""

import json
import os
from kafka import KafkaConsumer
from app.service import PaymentService
from app.producer import KafkaProducerClient


def main():
    """Main consumer loop"""
    kafka_brokers = os.getenv("KAFKA_BROKERS", "localhost:9092").split(",")
    
    consumer = KafkaConsumer(
        "booking-confirmations",
        bootstrap_servers=kafka_brokers,
        group_id="payment-service",
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest'
    )
    
    producer = KafkaProducerClient()
    producer.connect()
    
    payment_service = PaymentService()
    
    try:
        for message in consumer:
            booking_data = message.value
            print(f"Processing payment for booking: {booking_data}")
            
            # Process payment
            payment_id = payment_service.process_payment(booking_data)
            
            # Send payment confirmation
            confirmation = {
                "payment_id": payment_id,
                "booking_id": booking_data.get("booking_id"),
                "status": "completed"
            }
            producer.send_message("payment-confirmations", confirmation)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
