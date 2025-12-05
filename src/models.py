"""Data models for vehicle location data"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class Position(BaseModel):
    """Geographic position with latitude and longitude"""
    latitude: float
    longitude: float
    bearing: Optional[float] = None
    speed: Optional[float] = None  # meters per second


class VehicleDescriptor(BaseModel):
    """Vehicle identification information"""
    id: str
    label: Optional[str] = None
    license_plate: Optional[str] = None


class TripDescriptor(BaseModel):
    """Trip identification information"""
    trip_id: str
    route_id: Optional[str] = None
    direction_id: Optional[int] = None
    start_time: Optional[str] = None
    start_date: Optional[str] = None


class VehiclePosition(BaseModel):
    """Normalized vehicle position data"""
    vehicle_id: str
    license_plate: Optional[str] = None
    trip_id: Optional[str] = None
    route_id: Optional[str] = None
    position: Position
    timestamp: int  # Unix timestamp
    current_status: Optional[str] = None  # INCOMING_AT, STOPPED_AT, IN_TRANSIT_TO
    stop_id: Optional[str] = None
    stop_sequence: Optional[int] = None
    congestion_level: Optional[str] = None
    occupancy_status: Optional[str] = None
    service_date: Optional[str] = None  # YYYYMMDD format
    
    # Metadata
    ingested_at: datetime = Field(default_factory=datetime.utcnow)


class VehicleState(BaseModel):
    """Current state of a vehicle stored in Redis HASH"""
    vehicle_id: str
    license_plate: Optional[str] = None
    trip_id: Optional[str] = None
    route_id: Optional[str] = None
    latitude: float
    longitude: float
    bearing: Optional[float] = None
    speed: Optional[float] = None
    timestamp: int
    current_status: Optional[str] = None
    stop_id: Optional[str] = None
    last_updated: str  # ISO format datetime
    status: str = "active"  # active or inactive
    
    # GTFS enrichment fields
    route_short_name: Optional[str] = None
    route_long_name: Optional[str] = None
    trip_headsign: Optional[str] = None
    stop_name: Optional[str] = None
    direction_id: Optional[int] = None  # Trip direction (0 or 1)
    
    # Shape matching fields
    shape_dist_traveled: Optional[float] = None  # Distance along shape
    shape_bearing: Optional[int] = None  # Calculated bearing from movement (0-360)
    two_shape_bearing: Optional[int] = None  # Bearing between 2 closest shape points (0-360)
    shape_speed: Optional[float] = None  # Speed calculated from shape distance (m/s)
    service_date: Optional[str] = None  # YYYYMMDD format - persisted throughout trip
    
    def to_redis_dict(self) -> dict:
        """Convert to flat dictionary for Redis HASH"""
        return {
            "vehicle_id": self.vehicle_id,
            "license_plate": self.license_plate or "",
            "trip_id": self.trip_id or "",
            "route_id": self.route_id or "",
            "latitude": str(self.latitude),
            "longitude": str(self.longitude),
            "bearing": str(self.bearing) if self.bearing is not None else "",
            "speed": str(self.speed) if self.speed is not None else "",
            "timestamp": str(self.timestamp),
            "current_status": self.current_status or "",
            "stop_id": self.stop_id or "",
            "last_updated": self.last_updated,
            "status": self.status,
            "route_short_name": self.route_short_name or "",
            "route_long_name": self.route_long_name or "",
            "trip_headsign": self.trip_headsign or "",
            "stop_name": self.stop_name or "",
            "direction_id": str(self.direction_id) if self.direction_id is not None else "",
            "shape_dist_traveled": str(self.shape_dist_traveled) if self.shape_dist_traveled is not None else "",
            "shape_bearing": str(self.shape_bearing) if self.shape_bearing is not None else "",
            "two_shape_bearing": str(self.two_shape_bearing) if self.two_shape_bearing is not None else "",
            "shape_speed": str(self.shape_speed) if self.shape_speed is not None else "",
            "service_date": self.service_date or "",
        }


class TripPosition(BaseModel):
    """Position entry for a trip track stored in Redis STREAM"""
    vehicle_id: str
    latitude: float
    longitude: float
    bearing: Optional[float] = None
    speed: Optional[float] = None
    timestamp: int
    current_status: Optional[str] = None
    stop_id: Optional[str] = None
    stop_sequence: Optional[int] = None
    service_date: str  # YYYYMMDD format
    
    def to_stream_dict(self) -> dict:
        """Convert to flat dictionary for Redis STREAM"""
        return {
            "vehicle_id": self.vehicle_id,
            "lat": str(self.latitude),
            "lon": str(self.longitude),
            "bearing": str(self.bearing) if self.bearing is not None else "",
            "speed": str(self.speed) if self.speed is not None else "",
            "ts": str(self.timestamp),
            "status": self.current_status or "",
            "stop_id": self.stop_id or "",
            "stop_sequence": str(self.stop_sequence) if self.stop_sequence is not None else "",
            "service_date": self.service_date,
        }


class TripTransition(BaseModel):
    """Detected trip transition event"""
    vehicle_id: str
    previous_trip_id: Optional[str]
    new_trip_id: str
    timestamp: int
    previous_service_date: Optional[str] = None  # Service date of previous trip
    new_service_date: str  # Service date of new trip
    previous_vehicle_state_key: Optional[str] = None  # Redis key for old state snapshot
    detected_at: datetime = Field(default_factory=datetime.utcnow)


class TripCompletion(BaseModel):
    """Trip completion metrics calculated when a trip ends"""
    trip_id: str
    service_date: str  # YYYYMMDD format
    vehicle_id: str
    license_plate: Optional[str] = None
    start_time: int  # Unix timestamp (actual from tracking)
    end_time: int  # Unix timestamp (actual from tracking)
    duration_seconds: int
    stops_served: int  # Count of unique stop_sequence values
    total_positions: int  # Total position records in the trip
    route_short_name: Optional[str] = None  # GTFS route short name
    route_long_name: Optional[str] = None  # GTFS route long name
    scheduled_start_time: Optional[str] = None  # Scheduled departure time from GTFS
    scheduled_end_time: Optional[str] = None  # Scheduled arrival time from GTFS
    completion_method: str = "UNKNOWN"  # TRANSITION, INACTIVITY, or UNKNOWN
    completed_at: datetime = Field(default_factory=datetime.utcnow)
    
    def to_redis_dict(self) -> dict:
        """Convert to flat dictionary for Redis HASH"""
        return {
            "trip_id": self.trip_id,
            "service_date": self.service_date,
            "vehicle_id": self.vehicle_id,
            "license_plate": self.license_plate or "",
            "start_time": str(self.start_time),
            "end_time": str(self.end_time),
            "duration_seconds": str(self.duration_seconds),
            "stops_served": str(self.stops_served),
            "total_positions": str(self.total_positions),
            "route_short_name": self.route_short_name or "",
            "route_long_name": self.route_long_name or "",
            "scheduled_start_time": self.scheduled_start_time or "",
            "scheduled_end_time": self.scheduled_end_time or "",
            "completion_method": self.completion_method,
            "completed_at": self.completed_at.isoformat(),
        }
