"""Payment Service Business Logic"""

import uuid
from app.db.connection import get_session
from app.db.models import Payment


class PaymentService:
    def __init__(self):
        self.session = get_session()
    
    def process_payment(self, booking_data: dict) -> str:
        """Process payment for booking"""
        payment_id = str(uuid.uuid4())
        
        payment = Payment(
            id=payment_id,
            booking_id=booking_data.get("booking_id"),
            amount=booking_data.get("amount", 0),
            status="completed"
        )
        
        self.session.add(payment)
        self.session.commit()
        
        return payment_id
