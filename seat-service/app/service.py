"""Seat Service Business Logic"""

from app.cache.redis_client import RedisClient


class SeatService:
    def __init__(self):
        self.redis_client = RedisClient()
    
    def check_availability(self, flight_id: str, seat_id: str) -> int:
        """Check seat availability"""
        return self.redis_client.get_seat_count(flight_id, seat_id)
    
    def reserve_seat(self, flight_id: str, seat_id: str) -> bool:
        """Reserve a seat"""
        return self.redis_client.decrement_seat_count(flight_id, seat_id)
    
    def release_seat(self, flight_id: str, seat_id: str) -> bool:
        """Release a reserved seat"""
        return self.redis_client.increment_seat_count(flight_id, seat_id)
