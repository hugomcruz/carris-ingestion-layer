"""Redis client for managing vehicle location data structures"""

import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime

from redis import asyncio as aioredis

from .models import VehicleState, TripPosition

logger = logging.getLogger(__name__)


class RedisClient:
    """
    Decoupled Redis client for vehicle location data storage
    
    Data Structures:
    1. Latest Vehicle State: vehicle:{vehicle_id} -> HASH
    2. Active Trip Track: trip:{trip_id}:{service_date}:track -> STREAM
    3. Activity Index: active_vehicles -> SET
    4. Trip Status: trip:{trip_id}:{service_date}:status -> STRING with TTL
    """
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.client: Optional[aioredis.Redis] = None
        
    async def connect(self):
        """Establish connection to Redis with unlimited connection pooling"""
        # Create connection pool without max_connections limit
        # Connection limiting is handled by semaphore in DataPublisher
        self.client = await aioredis.from_url(
            self.redis_url,
            encoding="utf-8",
            decode_responses=True,
            socket_keepalive=True,
            socket_keepalive_options={},
            health_check_interval=30  # Check connection health every 30s
        )
        logger.info("Connected to Redis with unlimited connection pooling")
        
    async def disconnect(self):
        """Close Redis connection"""
        if self.client:
            await self.client.close()
            logger.info("Disconnected from Redis")
    
    async def ping(self) -> bool:
        """Check Redis connectivity"""
        try:
            return await self.client.ping()
        except Exception as e:
            logger.error(f"Redis ping failed: {e}")
            return False
    
    # Vehicle State Operations (HASH)
    
    async def set_vehicle_state(self, vehicle_state: VehicleState):
        """Store or update vehicle state in Redis HASH"""
        key = f"vehicle:{vehicle_state.vehicle_id}"
        await self.client.hset(key, mapping=vehicle_state.to_redis_dict())
        logger.debug(f"Updated vehicle state: {vehicle_state.vehicle_id}")
        
    async def get_vehicle_state(self, vehicle_id: str) -> Optional[Dict[str, str]]:
        """Retrieve vehicle state from Redis HASH"""
        key = f"vehicle:{vehicle_id}"
        state = await self.client.hgetall(key)
        return state if state else None
    
    async def delete_vehicle_state(self, vehicle_id: str):
        """Remove vehicle state"""
        key = f"vehicle:{vehicle_id}"
        await self.client.delete(key)
        logger.debug(f"Deleted vehicle state: {vehicle_id}")
    
    # Trip Track Operations (STREAM)
    
    async def append_trip_position(self, trip_id: str, service_date: str, position: TripPosition) -> str:
        """Append position to trip track STREAM"""
        key = f"trip:{trip_id}:{service_date}:track"
        stream_id = await self.client.xadd(key, position.to_stream_dict())
        logger.debug(f"Appended position to trip {trip_id} ({service_date}): {stream_id}")
        return stream_id
    
    async def get_trip_track(
        self, 
        trip_id: str,
        service_date: str,
        count: int = 100,
        start: str = "-",
        end: str = "+"
    ) -> List[tuple]:
        """Read trip track positions from STREAM"""
        key = f"trip:{trip_id}:{service_date}:track"
        entries = await self.client.xrange(key, min=start, max=end, count=count)
        return entries
    
    async def get_full_trip_track(self, trip_id: str, service_date: str) -> List[tuple]:
        """Read all trip track positions from STREAM"""
        key = f"trip:{trip_id}:{service_date}:track"
        # Get all entries by not limiting count
        entries = await self.client.xrange(key, min="-", max="+")
        return entries
    
    async def trim_trip_track(self, trip_id: str, service_date: str, max_length: int = 1000):
        """Trim trip track to maximum length (keep most recent)"""
        key = f"trip:{trip_id}:{service_date}:track"
        await self.client.xtrim(key, maxlen=max_length, approximate=True)
        logger.debug(f"Trimmed trip track: {trip_id} ({service_date})")
    
    async def delete_trip_track(self, trip_id: str, service_date: str):
        """Remove trip track"""
        key = f"trip:{trip_id}:{service_date}:track"
        await self.client.delete(key)
        logger.debug(f"Deleted trip track: {trip_id} ({service_date})")
    
    # Activity Index Operations (SET)
    
    async def add_active_vehicle(self, vehicle_id: str):
        """Add vehicle to active vehicles SET"""
        await self.client.sadd("active_vehicles", vehicle_id)
        logger.debug(f"Added to active vehicles: {vehicle_id}")
    
    async def remove_active_vehicle(self, vehicle_id: str):
        """Remove vehicle from active vehicles SET"""
        await self.client.srem("active_vehicles", vehicle_id)
        logger.debug(f"Removed from active vehicles: {vehicle_id}")
    
    async def get_active_vehicles(self) -> set:
        """Get all active vehicle IDs"""
        return await self.client.smembers("active_vehicles")
    
    async def is_vehicle_active(self, vehicle_id: str) -> bool:
        """Check if vehicle is in active set"""
        return await self.client.sismember("active_vehicles", vehicle_id)
    
    # Trip Status Operations (STRING with TTL)
    
    async def set_trip_status(
        self, 
        trip_id: str,
        service_date: str,
        status: str = "active",
        ttl_seconds: int = 3600
    ):
        """Set trip status with TTL"""
        key = f"trip:{trip_id}:{service_date}:status"
        await self.client.setex(key, ttl_seconds, status)
        logger.debug(f"Set trip status: {trip_id} ({service_date}) -> {status}")
    
    async def get_trip_status(self, trip_id: str, service_date: str) -> Optional[str]:
        """Get trip status"""
        key = f"trip:{trip_id}:{service_date}:status"
        return await self.client.get(key)
    
    async def get_trip_completion(self, trip_id: str, service_date: str) -> Optional[Dict[str, str]]:
        """Get trip completion metrics"""
        key = f"trip:{trip_id}:{service_date}:completion"
        completion = await self.client.hgetall(key)
        return completion if completion else None
    
    async def delete_trip_status(self, trip_id: str, service_date: str):
        """Remove trip status"""
        key = f"trip:{trip_id}:{service_date}:status"
        await self.client.delete(key)
        logger.debug(f"Deleted trip status: {trip_id} ({service_date})")
    
    # Utility Operations
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        active_count = await self.client.scard("active_vehicles")
        vehicle_keys = await self.client.keys("vehicle:*")
        trip_keys = await self.client.keys("trip:*:*:track")
        
        return {
            "active_vehicles_count": active_count,
            "total_vehicle_states": len(vehicle_keys),
            "total_trip_tracks": len(trip_keys),
            "timestamp": datetime.utcnow().isoformat()
        }
