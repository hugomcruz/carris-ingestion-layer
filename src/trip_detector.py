"""Trip transition detector for identifying when vehicles change trips"""

import logging
from typing import Optional, Dict, Set

from .models import VehiclePosition, TripTransition, TripCompletion
from .redis_client import RedisClient
from .utils import get_service_date

logger = logging.getLogger(__name__)


class TripTransitionDetector:
    """
    Detect when vehicles transition between trips
    
    Compares current trip_id with previous state stored in Redis
    """
    
    def __init__(self, redis_client: RedisClient, gtfs_enrichment=None):
        self.redis_client = redis_client
        self.gtfs_enrichment = gtfs_enrichment
        
    async def detect_transition(
        self, 
        vehicle_position: VehiclePosition
    ) -> Optional[TripTransition]:
        """
        Detect if vehicle has transitioned to a new trip
        
        Args:
            vehicle_position: Current vehicle position with trip information
            
        Returns:
            TripTransition object if transition detected, None otherwise
        """
        vehicle_id = vehicle_position.vehicle_id
        current_trip_id = vehicle_position.trip_id
        current_service_date = vehicle_position.service_date
        
        # Skip if no trip information
        if not current_trip_id:
            return None
        
        # Derive service date if not provided
        if not current_service_date:
            current_service_date = get_service_date(vehicle_position.timestamp)
        
        # Get previous vehicle state from Redis
        previous_state = await self.redis_client.get_vehicle_state(vehicle_id)
        
        # No previous state - this is a new vehicle
        if not previous_state:
            logger.debug(f"New vehicle detected: {vehicle_id} on trip {current_trip_id}")
            return None
        
        previous_trip_id = previous_state.get('trip_id', '')
        previous_route_id = previous_state.get('route_id', '')
        previous_service_date = previous_state.get('service_date', '')
        
        # Log for debugging
        logger.debug(
            f"Vehicle {vehicle_id}: previous_trip={previous_trip_id}, "
            f"current_trip={current_trip_id}, previous_route={previous_route_id}, "
            f"current_route={vehicle_position.route_id}"
        )
        
        # No previous trip - vehicle is starting a trip
        if not previous_trip_id:
            logger.info(f"Vehicle {vehicle_id} starting trip {current_trip_id} (service_date: {current_service_date})")
            return TripTransition(
                vehicle_id=vehicle_id,
                previous_trip_id=None,
                new_trip_id=current_trip_id,
                timestamp=vehicle_position.timestamp,
                previous_service_date=None,
                new_service_date=current_service_date,
            )
        
        # Check if trip has changed
        if previous_trip_id != current_trip_id:
            # Save old vehicle state for async processing
            state_snapshot_key = f"vehicle:{vehicle_id}:transition:{previous_trip_id}"
            try:
                await self.redis_client.client.hset(
                    state_snapshot_key,
                    mapping=previous_state
                )
                # No TTL - keep state snapshots permanently
            except Exception as e:
                logger.error(f"Failed to save vehicle state snapshot: {e}")
                state_snapshot_key = None
            
            logger.info(
                f"Trip transition detected for vehicle {vehicle_id}: "
                f"{previous_trip_id} ({previous_service_date}) -> {current_trip_id} ({current_service_date})"
            )
            return TripTransition(
                vehicle_id=vehicle_id,
                previous_trip_id=previous_trip_id,
                new_trip_id=current_trip_id,
                timestamp=vehicle_position.timestamp,
                previous_service_date=previous_service_date or None,
                new_service_date=current_service_date,
                previous_vehicle_state_key=state_snapshot_key
            )
        
        # No transition detected
        return None
    
    async def handle_transition(self, transition: TripTransition) -> Optional[TripCompletion]:
        """
        Handle trip transition by calculating metrics and cleaning up old trip data
        
        Args:
            transition: TripTransition event
            
        Returns:
            TripCompletion with metrics if previous trip had data, None otherwise
        """
        if not transition.previous_trip_id:
            return None
            
        try:
            # Get the full trip track to calculate metrics
            trip_completion = await self._calculate_trip_metrics(
                trip_id=transition.previous_trip_id,
                service_date=transition.previous_service_date,
                vehicle_id=transition.vehicle_id,
                old_state_key=transition.previous_vehicle_state_key,
                completion_method="TRANSITION"
            )
            
            # Use pipeline for batch operations
            pipe = self.redis_client.client.pipeline()
            
            # Store trip completion metrics
            if trip_completion:
                pipe.hset(
                    f"trip:{transition.previous_trip_id}:{transition.previous_service_date}:completion",
                    mapping=trip_completion.to_redis_dict()
                )
                # No TTL - keep completion data permanently
                
                logger.info(
                    f"Trip {transition.previous_trip_id} ({transition.previous_service_date}) completed: "
                    f"duration={trip_completion.duration_seconds}s, "
                    f"stops={trip_completion.stops_served}, "
                    f"vehicle={trip_completion.vehicle_id}"
                )
            
            # Mark previous trip as completed
            pipe.set(
                f"trip:{transition.previous_trip_id}:{transition.previous_service_date}:status",
                "completed"
            )
            
            # Clean up old vehicle state snapshot
            if transition.previous_vehicle_state_key:
                pipe.delete(transition.previous_vehicle_state_key)
            
            await pipe.execute()
            logger.debug(f"Handled transition: marked trip {transition.previous_trip_id} as completed")
            
            return trip_completion
            
        except Exception as e:
            logger.error(f"Failed to handle transition for trip {transition.previous_trip_id}: {e}")
            return None
    
    async def _calculate_trip_metrics(
        self, 
        trip_id: str,
        service_date: Optional[str],
        vehicle_id: str,
        old_state_key: Optional[str] = None,
        completion_method: str = "UNKNOWN"
    ) -> Optional[TripCompletion]:
        """
        Calculate trip completion metrics from trip track data
        
        Args:
            trip_id: The completed trip ID
            service_date: The service date (YYYYMMDD) for the trip
            vehicle_id: The vehicle ID
            old_state_key: Optional Redis key for old vehicle state snapshot
            completion_method: How the trip was completed (TRANSITION, INACTIVITY, or UNKNOWN)
            
        Returns:
            TripCompletion with calculated metrics, or None if no data
        """
        try:
            # Get all positions for this trip
            # If service_date is missing, we can't retrieve the track (need to handle gracefully)
            if not service_date:
                logger.warning(f"No service_date provided for trip {trip_id}, cannot retrieve track data")
                return None
            
            entries = await self.redis_client.get_full_trip_track(trip_id, service_date)
            
            if not entries or len(entries) == 0:
                logger.warning(f"No track data found for trip {trip_id}")
                return None
            
            # Extract timestamps and stop sequences
            timestamps = []
            stop_sequences: Set[int] = set()
            license_plate = None
            
            for entry_id, entry_data in entries:
                # Extract timestamp from entry data
                if 'ts' in entry_data:
                    try:
                        timestamps.append(int(entry_data['ts']))
                    except (ValueError, TypeError):
                        pass
                
                # Extract stop sequence (unique stops served)
                if 'stop_sequence' in entry_data and entry_data['stop_sequence']:
                    try:
                        stop_sequences.add(int(entry_data['stop_sequence']))
                    except (ValueError, TypeError):
                        pass
            
            # Get license plate from old vehicle state snapshot if available
            if old_state_key:
                try:
                    old_state = await self.redis_client.client.hgetall(old_state_key)
                    if old_state and 'license_plate' in old_state:
                        license_plate = old_state['license_plate']
                except Exception as e:
                    logger.warning(f"Failed to retrieve old state from {old_state_key}: {e}")
            
            # Fallback to current vehicle state if needed
            if not license_plate:
                vehicle_state = await self.redis_client.get_vehicle_state(vehicle_id)
                if vehicle_state and 'license_plate' in vehicle_state:
                    license_plate = vehicle_state['license_plate']
            
            # Calculate metrics
            if not timestamps:
                logger.warning(f"No valid timestamps found for trip {trip_id}")
                return None
            
            start_time = min(timestamps)
            end_time = max(timestamps)
            duration_seconds = end_time - start_time
            stops_served = len(stop_sequences)
            total_positions = len(entries)
            
            # Enrich with GTFS static data
            route_short_name = None
            route_long_name = None
            scheduled_start_time = None
            scheduled_end_time = None
            
            if self.gtfs_enrichment and self.gtfs_enrichment.is_loaded:
                # Get trip info to find route
                trip_info = self.gtfs_enrichment.get_trip_info(trip_id)
                if trip_info:
                    route_id = trip_info.get("route_id")
                    if route_id:
                        route_info = self.gtfs_enrichment.get_route_info(route_id)
                        if route_info:
                            route_short_name = route_info.get("route_short_name")
                            route_long_name = route_info.get("route_long_name")
                
                # Get scheduled times from stop_times and convert to timestamps
                trip_stops = self.gtfs_enrichment.get_trip_stops(trip_id)
                if trip_stops and len(trip_stops) > 0 and service_date:
                    from src.utils import gtfs_time_to_timestamp
                    
                    # First stop (stop_sequence=1 or minimum)
                    first_stop = min(trip_stops, key=lambda x: x.get("stop_sequence", 999999))
                    departure = first_stop.get("departure_time")
                    if departure:
                        departure_str = str(departure) if not isinstance(departure, str) else departure
                        timestamp = gtfs_time_to_timestamp(departure_str, service_date)
                        if timestamp:
                            scheduled_start_time = str(timestamp)
                    
                    # Last stop (maximum stop_sequence)
                    last_stop = max(trip_stops, key=lambda x: x.get("stop_sequence", 0))
                    arrival = last_stop.get("arrival_time")
                    if arrival:
                        arrival_str = str(arrival) if not isinstance(arrival, str) else arrival
                        timestamp = gtfs_time_to_timestamp(arrival_str, service_date)
                        if timestamp:
                            scheduled_end_time = str(timestamp)
            
            return TripCompletion(
                trip_id=trip_id,
                service_date=service_date or "unknown",
                vehicle_id=vehicle_id,
                license_plate=license_plate if license_plate else None,
                start_time=start_time,
                end_time=end_time,
                duration_seconds=duration_seconds,
                stops_served=stops_served,
                total_positions=total_positions,
                route_short_name=route_short_name,
                route_long_name=route_long_name,
                scheduled_start_time=scheduled_start_time,
                scheduled_end_time=scheduled_end_time,
                completion_method=completion_method,
            )
            
        except Exception as e:
            logger.error(f"Failed to calculate metrics for trip {trip_id}: {e}")
            return None
