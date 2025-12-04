"""Normalization module for transforming GTFS protobuf data into application models"""

import logging
import time
from typing import List, Optional
from datetime import datetime

from google.transit import gtfs_realtime_pb2

from .models import VehiclePosition, Position, VehicleDescriptor, TripDescriptor
from .utils import get_service_date

logger = logging.getLogger(__name__)


class DataNormalizer:
    """
    Transform raw GTFS protobuf data into normalized application models
    
    Decoupled from data sources and storage layers
    """
    
    @staticmethod
    def normalize_feed(
        feed: gtfs_realtime_pb2.FeedMessage
    ) -> List[VehiclePosition]:
        """
        Normalize entire GTFS feed into VehiclePosition objects
        
        Args:
            feed: GTFS FeedMessage protobuf object
            
        Returns:
            List of normalized VehiclePosition objects
        """
        positions = []
        
        for entity in feed.entity:
            if entity.HasField('vehicle'):
                try:
                    position = DataNormalizer._normalize_vehicle_entity(entity.vehicle)
                    if position:
                        positions.append(position)
                except Exception as e:
                    logger.warning(f"Failed to normalize entity {entity.id}: {e}")
                    continue
        
        logger.info(f"Normalized {len(positions)} vehicle positions")
        return positions
    
    @staticmethod
    def _normalize_vehicle_entity(
        vehicle: gtfs_realtime_pb2.VehiclePosition
    ) -> Optional[VehiclePosition]:
        """
        Normalize a single VehiclePosition protobuf entity
        
        Args:
            vehicle: GTFS VehiclePosition protobuf object
            
        Returns:
            Normalized VehiclePosition or None if invalid
        """
        # Extract vehicle ID (required)
        if not vehicle.HasField('vehicle') or not vehicle.vehicle.id:
            logger.warning("Vehicle entity missing vehicle ID, skipping")
            return None
        
        vehicle_id = vehicle.vehicle.id
        
        # Extract license plate (optional)
        license_plate = vehicle.vehicle.license_plate if vehicle.vehicle.HasField('license_plate') else None
        
        # Extract position (required)
        if not vehicle.HasField('position'):
            logger.warning(f"Vehicle {vehicle_id} missing position, skipping")
            return None
        
        position = Position(
            latitude=vehicle.position.latitude,
            longitude=vehicle.position.longitude,
            bearing=vehicle.position.bearing if vehicle.position.HasField('bearing') else None,
            speed=vehicle.position.speed if vehicle.position.HasField('speed') else None,
        )
        
        # Extract trip information (optional)
        trip_id = None
        route_id = None
        if vehicle.HasField('trip'):
            trip_id = vehicle.trip.trip_id if vehicle.trip.HasField('trip_id') else None
            route_id = vehicle.trip.route_id if vehicle.trip.HasField('route_id') else None
        
        # Extract timestamp
        timestamp = vehicle.timestamp if vehicle.HasField('timestamp') else int(datetime.utcnow().timestamp())
        
        # Skip vehicles with stale data (> 180 seconds old)
        current_time = int(time.time())
        age_seconds = current_time - timestamp
        if age_seconds > 180:
            logger.debug(f"Skipping vehicle {vehicle_id} with stale timestamp (age: {age_seconds}s)")
            return None
        
        # Extract current status
        current_status = None
        if vehicle.HasField('current_status'):
            status_map = {
                0: "INCOMING_AT",
                1: "STOPPED_AT",
                2: "IN_TRANSIT_TO",
            }
            current_status = status_map.get(vehicle.current_status)
        
        # Extract stop ID
        stop_id = vehicle.stop_id if vehicle.HasField('stop_id') else None
        
        # Extract stop sequence
        stop_sequence = vehicle.current_stop_sequence if vehicle.HasField('current_stop_sequence') else None
        
        # Extract congestion level
        congestion_level = None
        if vehicle.HasField('congestion_level'):
            congestion_map = {
                0: "UNKNOWN_CONGESTION_LEVEL",
                1: "RUNNING_SMOOTHLY",
                2: "STOP_AND_GO",
                3: "CONGESTION",
                4: "SEVERE_CONGESTION",
            }
            congestion_level = congestion_map.get(vehicle.congestion_level)
        
        # Extract occupancy status
        occupancy_status = None
        if vehicle.HasField('occupancy_status'):
            occupancy_map = {
                0: "EMPTY",
                1: "MANY_SEATS_AVAILABLE",
                2: "FEW_SEATS_AVAILABLE",
                3: "STANDING_ROOM_ONLY",
                4: "CRUSHED_STANDING_ROOM_ONLY",
                5: "FULL",
                6: "NOT_ACCEPTING_PASSENGERS",
            }
            occupancy_status = occupancy_map.get(vehicle.occupancy_status)
        
        # Derive service date from timestamp
        service_date = get_service_date(timestamp) if trip_id else None
        
        return VehiclePosition(
            vehicle_id=vehicle_id,
            license_plate=license_plate,
            trip_id=trip_id,
            route_id=route_id,
            position=position,
            timestamp=timestamp,
            current_status=current_status,
            stop_id=stop_id,
            stop_sequence=stop_sequence,
            congestion_level=congestion_level,
            occupancy_status=occupancy_status,
            service_date=service_date,
        )
