"""Database models for Booking Service"""

from sqlalchemy import Column, String, Integer, DateTime, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Booking(Base):
    __tablename__ = "bookings"
    
    id = Column(String, primary_key=True)
    user_id = Column(String, nullable=False)
    flight_id = Column(String, nullable=False)
    seat_id = Column(String, nullable=False)
    quantity = Column(Integer, default=1)
    status = Column(String, default="pending")
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
