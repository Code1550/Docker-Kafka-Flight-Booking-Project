"""Booking Service Business Logic"""

import uuid
from app.db.connection import get_session
from app.db.models import Booking


class BookingService:
    def __init__(self):
        self.session = get_session()
    
    def create_booking(self, booking_data: dict) -> str:
        """Create a new booking"""
        booking_id = str(uuid.uuid4())
        
        booking = Booking(
            id=booking_id,
            user_id=booking_data.get("user_id"),
            flight_id=booking_data.get("flight_id"),
            seat_id=booking_data.get("seat_id"),
            quantity=booking_data.get("quantity", 1),
            status="confirmed"
        )
        
        self.session.add(booking)
        self.session.commit()
        
        return booking_id
    
    def get_booking(self, booking_id: str):
        """Get booking by ID"""
        return self.session.query(Booking).filter(Booking.id == booking_id).first()
