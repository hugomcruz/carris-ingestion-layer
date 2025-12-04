"""Publisher module for writing vehicle data to Redis"""

import asyncio
import logging
from typing import List, Optional
from datetime import datetime
import time

from .models import VehiclePosition, VehicleState, TripPosition
from .redis_client import RedisClient
from .trip_detector import TripTransitionDetector
from .utils import get_service_date

logger = logging.getLogger(__name__)


class DataPublisher:
    """
    Publish normalized vehicle data to Redis data structures
    
    Coordinates writing to:
    - Vehicle state HASHes
    - Trip track STREAMs
    - Activity index SETs
    - Trip status keys
    """
    
    def __init__(
        self, 
        redis_client: RedisClient,
        trip_detector: TripTransitionDetector,
        gtfs_enrichment=None,
        max_concurrent_operations: Optional[int] = None
    ):
        self.redis_client = redis_client
        self.trip_detector = trip_detector
        self.gtfs_enrichment = gtfs_enrichment
        # Semaphore to limit concurrent Redis operations
        # Use provided value or fall back to config
        if max_concurrent_operations is None:
            from .config import settings
            max_concurrent_operations = settings.max_concurrent_redis_operations
        self._semaphore = asyncio.Semaphore(max_concurrent_operations)
        logger.info(f"DataPublisher initialized with max {max_concurrent_operations} concurrent Redis operations")
        
    async def publish_positions(self, positions: List[VehiclePosition]):
        """
        Publish a batch of vehicle positions to Redis using parallel processing
        
        Args:
            positions: List of normalized vehicle positions
        """
        if not positions:
            logger.warning("No positions to publish")
            return
        
        logger.info(f"Publishing {len(positions)} vehicle positions")
        start_time = time.time()
        
        # Detect transitions first (requires reads) - parallelized with semaphore
        async def detect_with_limit(position):
            async with self._semaphore:
                return await self.trip_detector.detect_transition(position)
        
        transition_tasks = [detect_with_limit(position) for position in positions]
        transition_results = await asyncio.gather(*transition_tasks, return_exceptions=True)
        
        transitions = []
        for position, result in zip(positions, transition_results):
            if isinstance(result, Exception):
                logger.error(f"Failed to detect transition for vehicle {position.vehicle_id}: {result}")
            elif result:
                transitions.append(result)
                logger.info(
                    f"Transition queued for vehicle {position.vehicle_id}: "
                    f"{result.previous_trip_id} -> {result.new_trip_id} "
                    f"(new route_id: {position.route_id})"
                )
        
        # Process all vehicles in parallel with connection limiting
        async def process_with_limit(position, transitions):
            async with self._semaphore:
                return await self._process_vehicle_position(position, transitions)
        
        process_tasks = [
            process_with_limit(position, transitions) 
            for position in positions
        ]
        process_results = await asyncio.gather(*process_tasks, return_exceptions=True)
        
        # Collect successful results for pipeline write
        pipe = self.redis_client.client.pipeline()
        successful_count = 0
        
        for position, result in zip(positions, process_results):
            if isinstance(result, Exception):
                logger.error(f"Failed to process vehicle {position.vehicle_id}: {result}")
                continue
            
            if result:
                vehicle_state, trip_position = result
                vehicle_id = position.vehicle_id
                
                # Add to pipeline
                pipe.hset(f"vehicle:{vehicle_id}", mapping=vehicle_state.to_redis_dict())
                pipe.sadd("active_vehicles", vehicle_id)
                
                if trip_position and position.trip_id and position.service_date:
                    pipe.xadd(f"trip:{position.trip_id}:{position.service_date}:track", trip_position.to_stream_dict())
                    pipe.set(f"trip:{position.trip_id}:{position.service_date}:status", "active")
                
                successful_count += 1
        
        # Execute all commands in pipeline
        try:
            await pipe.execute()
            elapsed = time.time() - start_time
            logger.info(
                f"Successfully published {successful_count}/{len(positions)} positions "
                f"in {elapsed:.2f}s ({len(positions)/elapsed:.1f} vehicles/sec)"
            )
        except Exception as e:
            logger.error(f"Failed to execute pipeline: {e}")
            raise
        
        # Handle transitions after bulk insert
        for transition in transitions:
            try:
                await self.trip_detector.handle_transition(transition)
            except Exception as e:
                logger.error(f"Failed to handle transition: {e}")
    
    async def _process_vehicle_position(
        self, 
        position: VehiclePosition, 
        transitions: list
    ) -> Optional[tuple]:
        """
        Process a single vehicle position with enrichment and calculations
        
        Returns:
            Tuple of (VehicleState, TripPosition) or None on error
        """
        try:
            vehicle_id = position.vehicle_id
            
            # Enrich with GTFS static data if available
            enriched_data = {}
            shape_dist_traveled = None
            shape_bearing = None
            two_shape_bearing = None
            shape_speed = None
            
            # Get old vehicle position from Redis for calculations
            old_state = await self.redis_client.get_vehicle_state(vehicle_id)
            
            if self.gtfs_enrichment and self.gtfs_enrichment.is_loaded:
                position_dict = {
                    "trip_id": position.trip_id,
                    "route_id": position.route_id,
                    "stop_id": position.stop_id,
                    "stop_sequence": position.stop_sequence,
                }
                enriched_data = self.gtfs_enrichment.enrich_vehicle_position(position_dict)
                
                # Match position to shape for distance tracking
                if position.trip_id:
                    shape_match = self.gtfs_enrichment.match_position_to_shape(
                        position.trip_id,
                        position.position.latitude,
                        position.position.longitude
                    )
                    
                    if shape_match:
                        shape_dist_traveled = shape_match.get("shape_dist_traveled")
                        
                        # Calculate speed from shape distance if we have old position
                        if old_state and shape_dist_traveled is not None:
                            try:
                                old_shape_dist_str = old_state.get('shape_dist_traveled', '')
                                old_timestamp_str = old_state.get('timestamp', '0')
                                
                                # Convert and validate old values
                                if old_shape_dist_str and old_shape_dist_str.strip():
                                    old_shape_dist = float(old_shape_dist_str)
                                    old_timestamp = int(old_timestamp_str) if old_timestamp_str else 0
                                    
                                    if old_timestamp > 0:
                                        distance_meters = shape_dist_traveled - old_shape_dist
                                        time_seconds = position.timestamp - old_timestamp
                                        
                                        # Only calculate if reasonable time elapsed and positive distance
                                        if time_seconds > 0 and distance_meters >= 0 and time_seconds < 300:
                                            shape_speed = distance_meters / time_seconds  # m/s
                                            logger.debug(
                                                f"Vehicle {vehicle_id} shape_speed: {shape_speed:.2f} m/s "
                                                f"({shape_speed * 3.6:.1f} km/h) "
                                                f"dist: {distance_meters:.1f}m, time: {time_seconds}s"
                                            )
                                        else:
                                            logger.debug(
                                                f"Vehicle {vehicle_id} skipped speed calc: "
                                                f"time={time_seconds}s, dist={distance_meters:.1f}m"
                                            )
                                else:
                                    logger.debug(f"Vehicle {vehicle_id} no previous shape_dist_traveled")
                                    
                            except (ValueError, TypeError) as e:
                                logger.warning(
                                    f"Failed to calculate shape speed for vehicle {vehicle_id}: {e} "
                                    f"old_shape_dist={old_shape_dist_str}, old_timestamp={old_timestamp_str}"
                                )
                        elif not old_state:
                            logger.debug(f"Vehicle {vehicle_id} no old_state for speed calculation")

                    
                    # Calculate bearing between two closest shape points
                    two_shape_bearing = self.gtfs_enrichment.get_two_closest_shape_points_bearing(
                        position.trip_id,
                        position.position.latitude,
                        position.position.longitude
                    )
            
            # Calculate bearing from GPS movement (not using shape)
            if old_state and self.gtfs_enrichment:
                try:
                    old_lat = float(old_state.get('latitude', 0))
                    old_lon = float(old_state.get('longitude', 0))
                    
                    if old_lat != 0 and old_lon != 0:
                        # Only calculate if position has changed significantly
                        if (abs(old_lat - position.position.latitude) > 0.00001 or 
                            abs(old_lon - position.position.longitude) > 0.00001):
                            shape_bearing = self.gtfs_enrichment.calculate_bearing(
                                old_lat, old_lon,
                                position.position.latitude,
                                position.position.longitude
                            )
                except (ValueError, TypeError) as e:
                    logger.debug(f"Failed to calculate bearing for vehicle {vehicle_id}: {e}")
            
            # Create vehicle state
            vehicle_state = VehicleState(
                vehicle_id=vehicle_id,
                license_plate=position.license_plate,
                trip_id=position.trip_id,
                route_id=position.route_id,
                latitude=position.position.latitude,
                longitude=position.position.longitude,
                bearing=position.position.bearing,
                speed=position.position.speed,
                timestamp=position.timestamp,
                current_status=position.current_status,
                stop_id=position.stop_id,
                last_updated=datetime.utcnow().isoformat(),
                status="active",  # Mark as active on new position
                # Enriched GTFS fields
                route_short_name=enriched_data.get("route_short_name"),
                route_long_name=enriched_data.get("route_long_name"),
                trip_headsign=enriched_data.get("trip_headsign"),
                stop_name=enriched_data.get("stop_name"),
                direction_id=enriched_data.get("direction_id"),
                # Shape matching fields
                shape_dist_traveled=shape_dist_traveled,
                shape_bearing=shape_bearing,
                two_shape_bearing=two_shape_bearing,
                shape_speed=shape_speed,
                # Service date - retrieve from position or previous state, or derive new
                service_date=position.service_date or (old_state.get('service_date') if old_state else None) or (get_service_date(position.timestamp) if position.trip_id else None),
            )
            
            # Log vehicle state updates for vehicles in transition
            if any(t.vehicle_id == vehicle_id for t in transitions):
                logger.debug(
                    f"Updating vehicle {vehicle_id} state: "
                    f"trip_id={position.trip_id}, route_id={position.route_id}, "
                    f"route_short_name={enriched_data.get('route_short_name')}"
                )
            
            # Create trip position if on a trip
            trip_position = None
            if position.trip_id and position.service_date:
                trip_position = TripPosition(
                    vehicle_id=vehicle_id,
                    latitude=position.position.latitude,
                    longitude=position.position.longitude,
                    bearing=position.position.bearing,
                    speed=position.position.speed,
                    timestamp=position.timestamp,
                    current_status=position.current_status,
                    stop_id=position.stop_id,
                    stop_sequence=position.stop_sequence,
                    service_date=position.service_date,
                )
            
            return (vehicle_state, trip_position)
            
        except Exception as e:
            logger.error(f"Failed to process position for vehicle {position.vehicle_id}: {e}")
            return None
    
    async def cleanup_inactive_vehicles(self, inactivity_timeout_seconds: int):
        """
        Mark vehicles that haven't reported within the timeout period as inactive
        If inactive for > 1 hour, complete their active trips
        
        Args:
            inactivity_timeout_seconds: Time in seconds after which a vehicle is considered inactive
        """
        # Get all active vehicles from Redis
        redis_active = await self.redis_client.get_active_vehicles()
        
        if not redis_active:
            return
        
        current_time = int(time.time())
        inactive_vehicles = []
        trip_completion_needed = []  # Vehicles inactive > 1 hour
        one_hour = 3600
        
        # Check each vehicle's last update time
        for vehicle_id in redis_active:
            vehicle_state = await self.redis_client.get_vehicle_state(vehicle_id)
            
            if not vehicle_state:
                continue
            
            # Skip if already marked inactive
            if vehicle_state.get('status') == 'inactive':
                continue
            
            # Get timestamp from vehicle state
            try:
                last_timestamp = int(vehicle_state.get('timestamp', 0))
                time_since_update = current_time - last_timestamp
                
                if time_since_update > inactivity_timeout_seconds:
                    inactive_vehicles.append(vehicle_id)
                    
                    # If inactive for > 1 hour and has a trip, complete it
                    if time_since_update > one_hour:
                        trip_id = vehicle_state.get('trip_id')
                        if trip_id:
                            trip_completion_needed.append({
                                'vehicle_id': vehicle_id,
                                'trip_id': trip_id,
                                'inactive_duration': time_since_update
                            })
                    
                    logger.debug(
                        f"Vehicle {vehicle_id} inactive for {time_since_update}s "
                        f"(threshold: {inactivity_timeout_seconds}s)"
                    )
            except (ValueError, TypeError) as e:
                logger.warning(f"Invalid timestamp for vehicle {vehicle_id}: {e}")
        
        # Complete trips for vehicles inactive > 1 hour
        if trip_completion_needed:
            logger.info(f"Completing {len(trip_completion_needed)} trips for inactive vehicles")
            for item in trip_completion_needed:
                try:
                    trip_completion = await self.trip_detector._calculate_trip_metrics(
                        trip_id=item['trip_id'],
                        vehicle_id=item['vehicle_id']
                    )
                    
                    if trip_completion:
                        pipe = self.redis_client.client.pipeline()
                        
                        # Store completion metrics
                        pipe.hset(
                            f"trip:{item['trip_id']}:completion",
                            mapping=completion.to_redis_dict()
                        )
                        # No TTL - keep completion data permanently
                        
                        # Mark trip as completed
                        pipe.set(f"trip:{item['trip_id']}:status", "completed")
                        
                        await pipe.execute()
                        
                        logger.info(
                            f"Completed trip {item['trip_id']} for inactive vehicle {item['vehicle_id']} "
                            f"(inactive: {item['inactive_duration']}s, duration: {trip_completion.duration_seconds}s)"
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to complete trip {item['trip_id']} for vehicle {item['vehicle_id']}: {e}"
                    )
        
        # Mark vehicles as inactive
        if inactive_vehicles:
            logger.info(f"Marking {len(inactive_vehicles)} vehicles as inactive")
            
            # Use pipeline to update status field
            pipe = self.redis_client.client.pipeline()
            for vehicle_id in inactive_vehicles:
                pipe.hset(f"vehicle:{vehicle_id}", "status", "inactive")
            
            try:
                await pipe.execute()
                logger.info(
                    f"Marked {len(inactive_vehicles)} vehicles as inactive "
                    f"(inactive for >{inactivity_timeout_seconds}s)"
                )
            except Exception as e:
                logger.error(f"Failed to mark vehicles as inactive: {e}")
        else:
            logger.debug(f"No vehicles to mark inactive (checked {len(redis_active)} vehicles)")
