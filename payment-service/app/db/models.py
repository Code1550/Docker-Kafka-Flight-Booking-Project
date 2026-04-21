"""Database models for Payment Service"""

from sqlalchemy import Column, String, Float, DateTime, func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Payment(Base):
    __tablename__ = "payments"
    
    id = Column(String, primary_key=True)
    booking_id = Column(String, nullable=False)
    amount = Column(Float, default=0)
    status = Column(String, default="pending")
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, onupdate=func.now())
