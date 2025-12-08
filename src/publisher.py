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
        # In-memory cache to track vehicle positions and avoid redundant Redis writes
        self._position_cache = {}  # {vehicle_id: position_hash}
        self._first_run = True
        logger.info(f"DataPublisher initialized with max {max_concurrent_operations} concurrent Redis operations")
    
    def _compute_position_hash(self, position: VehiclePosition) -> str:
        """
        Compute a hash of the position data to detect changes.
        Only includes fields that matter for change detection.
        """
        return (
            f"{position.trip_id}|"
            f"{position.route_id}|"
            f"{position.position.latitude:.6f}|"
            f"{position.position.longitude:.6f}|"
            f"{position.position.bearing}|"
            f"{position.position.speed}|"
            f"{position.timestamp}|"
            f"{position.current_status}|"
            f"{position.stop_id}|"
            f"{position.stop_sequence}|"
            f"{position.service_date}"
        )
        
    async def publish_positions(self, positions: List[VehiclePosition]):
        """
        Publish a batch of vehicle positions to Redis using parallel processing.
        Uses in-memory cache to only publish changed positions.
        
        Args:
            positions: List of normalized vehicle positions
        """
        if not positions:
            logger.warning("No positions to publish")
            return
        
        logger.info(f"Publishing {len(positions)} vehicle positions")
        start_time = time.time()
        
        # Filter positions based on in-memory cache
        positions_to_publish = []
        unchanged_count = 0
        new_cache = {}
        
        for position in positions:
            vehicle_id = position.vehicle_id
            position_hash = self._compute_position_hash(position)
            
            # Store in new cache
            new_cache[vehicle_id] = position_hash
            
            # On first run, publish everything
            if self._first_run:
                positions_to_publish.append(position)
            else:
                # Check if position changed
                cached_hash = self._position_cache.get(vehicle_id)
                if cached_hash != position_hash:
                    positions_to_publish.append(position)
                else:
                    unchanged_count += 1
        
        # Update cache with new data
        self._position_cache = new_cache
        if self._first_run:
            self._first_run = False
            logger.info("First run completed - cache initialized with all positions")
        
        if unchanged_count > 0:
            logger.info(f"Skipping {unchanged_count} unchanged positions (using cache)")
        
        if not positions_to_publish:
            logger.info("No changed positions to publish")
            return
        
        if not positions_to_publish:
            logger.info("No changed positions to publish")
            return
        
        logger.info(f"Publishing {len(positions_to_publish)} changed positions (filtered from {len(positions)} total)")
        
        # Detect transitions first (requires reads) - parallelized with semaphore
        async def detect_with_limit(position):
            async with self._semaphore:
                return await self.trip_detector.detect_transition(position)
        
        transition_tasks = [detect_with_limit(position) for position in positions_to_publish]
        transition_results = await asyncio.gather(*transition_tasks, return_exceptions=True)
        
        transitions = []
        for position, result in zip(positions_to_publish, transition_results):
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
            for position in positions_to_publish
        ]
        process_results = await asyncio.gather(*process_tasks, return_exceptions=True)
        
        # Collect successful results for pipeline write
        pipe = self.redis_client.client.pipeline()
        successful_count = 0
        
        for position, result in zip(positions_to_publish, process_results):
            if isinstance(result, Exception):
                logger.error(f"Failed to process vehicle {position.vehicle_id}: {result}")
                continue
            
            if result:
                vehicle_state, trip_position = result
                vehicle_id = position.vehicle_id
                
                # Add to pipeline
                pipe.hset(f"vehicle:{vehicle_id}", mapping=vehicle_state.to_redis_dict())
                pipe.sadd("active_vehicles", vehicle_id)
                
                # Publish vehicle update to channel for real-time subscribers
                import json
                update_message = {
                    "vehicle_id": vehicle_id,
                    "trip_id": position.trip_id,
                    "route_id": position.route_id,
                    "latitude": position.position.latitude,
                    "longitude": position.position.longitude,
                    "bearing": position.position.bearing,
                    "speed": position.position.speed,
                    "timestamp": position.timestamp,
                    "service_date": position.service_date,
                    "status": "active"
                }
                pipe.publish("vehicle:updates", json.dumps(update_message))
                
                if trip_position and position.trip_id and position.service_date:
                    pipe.xadd(f"trip:{position.trip_id}:{position.service_date}:track", trip_position.to_stream_dict())
                    pipe.set(f"trip:{position.trip_id}:{position.service_date}:status", "active")
                
                successful_count += 1
        
        # Execute all commands in pipeline
        try:
            await pipe.execute()
            elapsed = time.time() - start_time
            logger.info(
                f"Successfully published {successful_count}/{len(positions_to_publish)} changed positions "
                f"(out of {len(positions)} total) "
                f"in {elapsed:.2f}s ({len(positions_to_publish)/elapsed:.1f} vehicles/sec)"
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
            
            # Determine if this is a new trip (trip started or changed)
            is_new_trip = False
            old_trip_id = old_state.get('trip_id') if old_state else None
            if position.trip_id and (not old_trip_id or old_trip_id != position.trip_id):
                is_new_trip = True
            
            # Get scheduled times and actual start time for new trips
            scheduled_start_time = None
            scheduled_end_time = None
            actual_start_time = None
            
            if is_new_trip and position.trip_id and position.service_date:
                from src.utils import gtfs_time_to_timestamp
                
                # Fetch scheduled times from GTFS
                if self.gtfs_enrichment and self.gtfs_enrichment.is_loaded:
                    trip_stops = self.gtfs_enrichment.get_trip_stops(position.trip_id)
                    if trip_stops and len(trip_stops) > 0:
                        # First stop scheduled departure
                        first_stop = min(trip_stops, key=lambda x: x.get("stop_sequence", 999999))
                        departure = first_stop.get("departure_time")
                        if departure:
                            departure_str = str(departure) if not isinstance(departure, str) else departure
                            timestamp = gtfs_time_to_timestamp(departure_str, position.service_date)
                            if timestamp:
                                scheduled_start_time = str(timestamp)
                        
                        # Last stop scheduled arrival
                        last_stop = max(trip_stops, key=lambda x: x.get("stop_sequence", 0))
                        arrival = last_stop.get("arrival_time")
                        if arrival:
                            arrival_str = str(arrival) if not isinstance(arrival, str) else arrival
                            timestamp = gtfs_time_to_timestamp(arrival_str, position.service_date)
                            if timestamp:
                                scheduled_end_time = str(timestamp)
            else:
                # Preserve existing values from old state if not a new trip
                if old_state:
                    scheduled_start_time = old_state.get('scheduled_start_time') or None
                    scheduled_end_time = old_state.get('scheduled_end_time') or None
                    actual_start_time = old_state.get('actual_start_time') or None
            
            # Set actual_start_time when stop_sequence=1 is first encountered (ignore stop_sequence=0)
            if position.trip_id and position.stop_sequence == 1:
                # Only set if not already set (first time at stop_sequence=1)
                if old_state:
                    old_actual_start = old_state.get('actual_start_time')
                    if not old_actual_start:
                        actual_start_time = str(position.timestamp)
                else:
                    # No old state, set it now
                    actual_start_time = str(position.timestamp)
            
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
                current_stop_sequence=position.stop_sequence,
                last_updated=int(datetime.utcnow().timestamp()),
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
                # Trip timing fields
                scheduled_start_time=scheduled_start_time,
                scheduled_end_time=scheduled_end_time,
                actual_start_time=actual_start_time,
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
                        service_date = vehicle_state.get('service_date')
                        if trip_id:
                            trip_completion_needed.append({
                                'vehicle_id': vehicle_id,
                                'trip_id': trip_id,
                                'service_date': service_date,
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
                        service_date=item.get('service_date'),
                        vehicle_id=item['vehicle_id'],
                        completion_method="INACTIVITY"
                    )
                    
                    if trip_completion:
                        pipe = self.redis_client.client.pipeline()
                        
                        service_date = item.get('service_date') or trip_completion.service_date
                        
                        # Store completion metrics
                        pipe.hset(
                            f"trip:{item['trip_id']}:{service_date}:completion",
                            mapping=trip_completion.to_redis_dict()
                        )
                        # No TTL - keep completion data permanently
                        
                        # Mark trip as completed
                        pipe.set(f"trip:{item['trip_id']}:{service_date}:status", "completed")
                        
                        # Delete vehicle state key and remove from active set
                        pipe.delete(f"vehicle:{item['vehicle_id']}")
                        pipe.srem("active_vehicles", item['vehicle_id'])
                        
                        await pipe.execute()
                        
                        # Remove from cache since vehicle was deleted
                        if item['vehicle_id'] in self._position_cache:
                            del self._position_cache[item['vehicle_id']]
                        
                        logger.info(
                            f"Completed trip {item['trip_id']} for inactive vehicle {item['vehicle_id']} "
                            f"(inactive: {item['inactive_duration']}s, duration: {trip_completion.duration_seconds}s) "
                            f"- vehicle state deleted and removed from cache"
                        )
                except Exception as e:
                    logger.error(
                        f"Failed to complete trip {item['trip_id']} for vehicle {item['vehicle_id']}: {e}"
                    )
        
        # Mark remaining vehicles as inactive (those without trips or inactive < 1 hour)
        # Vehicles with completed trips have already been deleted above
        if inactive_vehicles:
            vehicles_with_completed_trips = {item['vehicle_id'] for item in trip_completion_needed}
            vehicles_to_mark_inactive = [v for v in inactive_vehicles if v not in vehicles_with_completed_trips]
            
            if vehicles_to_mark_inactive:
                logger.info(f"Marking {len(vehicles_to_mark_inactive)} vehicles as inactive")
                
                # Use pipeline to update status field and publish status change
                import json
                pipe = self.redis_client.client.pipeline()
                for vehicle_id in vehicles_to_mark_inactive:
                    pipe.hset(f"vehicle:{vehicle_id}", "status", "inactive")
                    
                    # Get vehicle state to publish complete update
                    vehicle_state = await self.redis_client.get_vehicle_state(vehicle_id)
                    if vehicle_state:
                        status_update_message = {
                            "vehicle_id": vehicle_id,
                            "trip_id": vehicle_state.get('trip_id'),
                            "route_id": vehicle_state.get('route_id'),
                            "latitude": float(vehicle_state.get('latitude', 0)),
                            "longitude": float(vehicle_state.get('longitude', 0)),
                            "bearing": float(vehicle_state.get('bearing')) if vehicle_state.get('bearing') else None,
                            "speed": float(vehicle_state.get('speed')) if vehicle_state.get('speed') else None,
                            "timestamp": int(vehicle_state.get('timestamp', 0)),
                            "service_date": vehicle_state.get('service_date'),
                            "status": "inactive"
                        }
                        pipe.publish("vehicle:updates", json.dumps(status_update_message))
                
                try:
                    await pipe.execute()
                    
                    # Remove inactive vehicles from cache
                    for vehicle_id in vehicles_to_mark_inactive:
                        if vehicle_id in self._position_cache:
                            del self._position_cache[vehicle_id]
                    
                    logger.info(
                        f"Marked {len(vehicles_to_mark_inactive)} vehicles as inactive "
                        f"(inactive for >{inactivity_timeout_seconds}s) and removed from cache"
                    )
                except Exception as e:
                    logger.error(f"Failed to mark vehicles as inactive: {e}")
        else:
            logger.debug(f"No vehicles to mark inactive (checked {len(redis_active)} vehicles)")
