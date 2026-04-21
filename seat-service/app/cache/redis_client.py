"""Redis client for seat caching"""

import os
import redis


class RedisClient:
    def __init__(self):
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        self.redis = redis.from_url(redis_url)
    
    def get_seat_count(self, flight_id: str, seat_id: str) -> int:
        """Get seat count for a flight"""
        key = f"seat:{flight_id}:{seat_id}"
        count = self.redis.get(key)
        return int(count) if count else 0
    
    def set_seat_count(self, flight_id: str, seat_id: str, count: int):
        """Set seat count for a flight"""
        key = f"seat:{flight_id}:{seat_id}"
        self.redis.set(key, count)
    
    def increment_seat_count(self, flight_id: str, seat_id: str) -> int:
        """Increment seat count"""
        key = f"seat:{flight_id}:{seat_id}"
        return self.redis.incr(key)
    
    def decrement_seat_count(self, flight_id: str, seat_id: str) -> int:
        """Decrement seat count"""
        key = f"seat:{flight_id}:{seat_id}"
        count = self.redis.get(key)
        if count and int(count) > 0:
            return self.redis.decr(key)
        return 0
