"""GTFS data enrichment from PostgreSQL database - cached in memory"""

import logging
from typing import Dict, Optional, Any, TYPE_CHECKING
from datetime import datetime, time as dt_time

try:
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy import text
    SQLALCHEMY_AVAILABLE = True
except ImportError:
    SQLALCHEMY_AVAILABLE = False
    if TYPE_CHECKING:
        from sqlalchemy.ext.asyncio import AsyncSession
    else:
        AsyncSession = Any

logger = logging.getLogger(__name__)


class GTFSEnrichment:
    """
    Load and cache GTFS static data in memory for enriching vehicle positions
    
    Data is loaded once at startup and refreshed daily at configured hour.
    Keeps data structures in memory for fast lookups without database queries.
    """
    
    def __init__(self, db_url: str, refresh_hour: int = 4):
        if not SQLALCHEMY_AVAILABLE:
            logger.warning(
                "SQLAlchemy not available. GTFS enrichment will be disabled. "
                "Install: pip install 'sqlalchemy[asyncio]' aiomysql"
            )
        
        self.db_url = db_url
        self.refresh_hour = refresh_hour
        self.engine = None
        
        # In-memory caches
        self.routes: Dict[str, dict] = {}  # route_id -> route data
        self.trips: Dict[str, dict] = {}   # trip_id -> trip data
        self.stops: Dict[str, dict] = {}   # stop_id -> stop data
        self.stop_times: Dict[str, list] = {}  # trip_id -> list of stop_times
        self.shapes: Dict[str, list] = {}  # shape_id -> list of (lat, lon, dist) ordered by sequence
        
        # State
        self.last_loaded: Optional[datetime] = None
        self.is_loaded: bool = False
        
    async def connect(self):
        """Establish database connection"""
        if not SQLALCHEMY_AVAILABLE:
            logger.warning("Skipping database connection - SQLAlchemy not available")
            return
            
        try:
            self.engine = create_async_engine(
                self.db_url,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            logger.info("Connected to GTFS database")
        except Exception as e:
            logger.error(f"Failed to connect to GTFS database: {e}")
            raise
    
    async def disconnect(self):
        """Close database connection"""
        if not SQLALCHEMY_AVAILABLE:
            return
            
        if self.engine:
            await self.engine.dispose()
            logger.info("Disconnected from GTFS database")
    
    async def load_data_on_startup(self):
        """
        Load GTFS data on startup from database
        """
        if not SQLALCHEMY_AVAILABLE:
            logger.warning("Skipping GTFS data load - SQLAlchemy not available")
            return
        
        logger.info("Loading GTFS data from database on startup")
        await self._load_from_database()
    
    async def refresh_data_daily(self):
        """
        Daily refresh: reload GTFS data from database
        """
        if not SQLALCHEMY_AVAILABLE:
            logger.warning("Skipping GTFS refresh check - SQLAlchemy not available")
            return
        
        logger.info("Daily GTFS refresh triggered, reloading from database")
        await self._load_from_database()
    
    async def _load_from_database(self):
        """Load GTFS data from database"""
        logger.info("Loading GTFS data from database...")
        start_time = datetime.now()
        
        try:
            async with AsyncSession(self.engine) as session:
                # Load routes
                await self._load_routes(session)
                
                # Load trips
                await self._load_trips(session)
                
                # Load stops
                await self._load_stops(session)
                
                # Load stop_times (for trip sequences)
                await self._load_stop_times(session)
                
                # Load shapes (for route geometry)
                await self._load_shapes(session)
            
            self.last_loaded = datetime.now()
            self.is_loaded = True
            
            elapsed = (datetime.now() - start_time).total_seconds()
            logger.info(
                f"GTFS data loaded from database in {elapsed:.2f}s - "
                f"Routes: {len(self.routes)}, "
                f"Trips: {len(self.trips)}, "
                f"Stops: {len(self.stops)}, "
                f"Stop Times: {len(self.stop_times)}, "
                f"Shapes: {len(self.shapes)}"
            )
            
        except Exception as e:
            logger.error(f"Failed to load GTFS data: {e}", exc_info=True)
            self.is_loaded = False
            raise
    
    async def _load_routes(self, session: AsyncSession):
        """Load routes table into memory"""
        query = text("""
            SELECT route_id, route_short_name, route_long_name, route_type, route_color
            FROM routes
        """)
        result = await session.execute(query)
        
        self.routes.clear()
        for row in result:
            self.routes[row.route_id] = {
                "route_id": row.route_id,
                "route_short_name": row.route_short_name,
                "route_long_name": row.route_long_name,
                "route_type": row.route_type,
                "route_color": row.route_color if hasattr(row, 'route_color') else None,
            }
        
        logger.debug(f"Loaded {len(self.routes)} routes")
    
    async def _load_trips(self, session: AsyncSession):
        """Load trips table into memory"""
        query = text("""
            SELECT trip_id, route_id, service_id, trip_headsign, direction_id, 
                   block_id, shape_id
            FROM trips
        """)
        result = await session.execute(query)
        
        self.trips.clear()
        for row in result:
            self.trips[row.trip_id] = {
                "trip_id": row.trip_id,
                "route_id": row.route_id,
                "service_id": row.service_id,
                "trip_headsign": row.trip_headsign if hasattr(row, 'trip_headsign') else None,
                "direction_id": row.direction_id if hasattr(row, 'direction_id') else None,
                "block_id": row.block_id if hasattr(row, 'block_id') else None,
                "shape_id": row.shape_id if hasattr(row, 'shape_id') else None,
            }
        
        logger.debug(f"Loaded {len(self.trips)} trips")
    
    async def _load_stops(self, session: AsyncSession):
        """Load stops table into memory"""
        query = text("""
            SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon, 
                   location_type, parent_station
            FROM stops
        """)
        result = await session.execute(query)
        
        self.stops.clear()
        for row in result:
            self.stops[row.stop_id] = {
                "stop_id": row.stop_id,
                "stop_code": row.stop_code if hasattr(row, 'stop_code') else None,
                "stop_name": row.stop_name,
                "stop_lat": float(row.stop_lat) if row.stop_lat else None,
                "stop_lon": float(row.stop_lon) if row.stop_lon else None,
                "location_type": row.location_type if hasattr(row, 'location_type') else None,
                "parent_station": row.parent_station if hasattr(row, 'parent_station') else None,
            }
        
        logger.debug(f"Loaded {len(self.stops)} stops")
    
    async def _load_stop_times(self, session: AsyncSession):
        """Load stop_times table into memory (grouped by trip_id)"""
        query = text("""
            SELECT trip_id, stop_id, stop_sequence, arrival_time, departure_time,
                   stop_headsign, pickup_type, drop_off_type
            FROM stop_times
            ORDER BY trip_id, stop_sequence
        """)
        result = await session.execute(query)
        
        self.stop_times.clear()
        for row in result:
            if row.trip_id not in self.stop_times:
                self.stop_times[row.trip_id] = []
            
            self.stop_times[row.trip_id].append({
                "stop_id": row.stop_id,
                "stop_sequence": row.stop_sequence,
                "arrival_time": row.arrival_time if hasattr(row, 'arrival_time') else None,
                "departure_time": row.departure_time if hasattr(row, 'departure_time') else None,
                "stop_headsign": row.stop_headsign if hasattr(row, 'stop_headsign') else None,
            })
        
        logger.debug(f"Loaded stop_times for {len(self.stop_times)} trips")
    
    async def _load_shapes(self, session: AsyncSession):
        """Load shapes table into memory (grouped by shape_id, ordered by sequence)"""
        query = text("""
            SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled
            FROM shapes
            ORDER BY shape_id, shape_pt_sequence
        """)
        result = await session.execute(query)
        
        self.shapes.clear()
        for row in result:
            if row.shape_id not in self.shapes:
                self.shapes[row.shape_id] = []
            
            self.shapes[row.shape_id].append({
                "lat": float(row.shape_pt_lat),
                "lon": float(row.shape_pt_lon),
                "sequence": row.shape_pt_sequence,
                "dist_traveled": float(row.shape_dist_traveled) if row.shape_dist_traveled else None,
            })
        
        logger.debug(f"Loaded {len(self.shapes)} shapes")
    
    def should_refresh(self) -> bool:
        """Check if data should be refreshed based on time"""
        if not self.is_loaded:
            return True
        
        if not self.last_loaded:
            return True
        
        now = datetime.now()
        current_time = now.time()
        refresh_time = dt_time(hour=self.refresh_hour, minute=0, second=0)
        
        # Check if we've passed the refresh hour today and haven't loaded yet today
        if current_time >= refresh_time and self.last_loaded.date() < now.date():
            return True
        
        return False
    
    def enrich_vehicle_position(self, position_data: dict) -> dict:
        """
        Enrich vehicle position with GTFS static data
        
        Args:
            position_data: Dictionary with vehicle position data
            
        Returns:
            Enriched position data with GTFS information
        """
        if not self.is_loaded:
            logger.warning("GTFS data not loaded, cannot enrich")
            return position_data
        
        enriched = position_data.copy()
        
        # Enrich trip information
        trip_id = position_data.get("trip_id")
        if trip_id and trip_id in self.trips:
            trip_data = self.trips[trip_id]
            enriched["trip_headsign"] = trip_data.get("trip_headsign")
            enriched["direction_id"] = trip_data.get("direction_id")
            enriched["service_id"] = trip_data.get("service_id")
            enriched["shape_id"] = trip_data.get("shape_id")
            
            # Enrich route information
            route_id = trip_data.get("route_id")
            if route_id and route_id in self.routes:
                route_data = self.routes[route_id]
                enriched["route_short_name"] = route_data.get("route_short_name")
                enriched["route_long_name"] = route_data.get("route_long_name")
                enriched["route_type"] = route_data.get("route_type")
                enriched["route_color"] = route_data.get("route_color")
        
        # Enrich stop information
        stop_id = position_data.get("stop_id")
        if stop_id and stop_id in self.stops:
            stop_data = self.stops[stop_id]
            enriched["stop_name"] = stop_data.get("stop_name")
            enriched["stop_code"] = stop_data.get("stop_code")
            enriched["stop_lat"] = stop_data.get("stop_lat")
            enriched["stop_lon"] = stop_data.get("stop_lon")
        
        # Enrich with scheduled stop times if available
        if trip_id and trip_id in self.stop_times:
            stop_sequence = position_data.get("stop_sequence")
            if stop_sequence is not None:
                # Find the matching stop in the trip's stop_times
                for st in self.stop_times[trip_id]:
                    if st["stop_sequence"] == stop_sequence:
                        enriched["scheduled_arrival"] = st.get("arrival_time")
                        enriched["scheduled_departure"] = st.get("departure_time")
                        break
        
        return enriched
    
    def get_route_info(self, route_id: str) -> Optional[dict]:
        """Get route information by route_id"""
        return self.routes.get(route_id)
    
    def get_trip_info(self, trip_id: str) -> Optional[dict]:
        """Get trip information by trip_id"""
        return self.trips.get(trip_id)
    
    def get_stop_info(self, stop_id: str) -> Optional[dict]:
        """Get stop information by stop_id"""
        return self.stops.get(stop_id)
    
    def get_trip_stops(self, trip_id: str) -> Optional[list]:
        """Get ordered list of stops for a trip"""
        return self.stop_times.get(trip_id)
    
    def get_shape_for_trip(self, trip_id: str) -> Optional[list]:
        """Get shape points for a trip"""
        trip_info = self.trips.get(trip_id)
        if not trip_info:
            return None
        
        shape_id = trip_info.get("shape_id")
        if not shape_id:
            return None
        
        return self.shapes.get(shape_id)
    
    @staticmethod
    def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate the great circle distance in meters between two points
        on the earth (specified in decimal degrees)
        """
        import math
        
        # Convert decimal degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.asin(math.sqrt(a))
        r = 6371000  # Radius of earth in meters
        return c * r
    
    @staticmethod
    def calculate_bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> int:
        """
        Calculate the bearing (forward azimuth) from point 1 to point 2
        Returns bearing in degrees (0-360) as integer
        """
        import math
        
        # Convert to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
        
        dlon = lon2 - lon1
        
        x = math.sin(dlon) * math.cos(lat2)
        y = math.cos(lat1) * math.sin(lat2) - math.sin(lat1) * math.cos(lat2) * math.cos(dlon)
        
        initial_bearing = math.atan2(x, y)
        
        # Convert from radians to degrees
        initial_bearing = math.degrees(initial_bearing)
        
        # Normalize to 0-360 and round to integer
        bearing = round((initial_bearing + 360) % 360)
        
        return bearing
    
    def match_position_to_shape(
        self, 
        trip_id: str, 
        lat: float, 
        lon: float
    ) -> Optional[dict]:
        """
        Find the closest point on the trip's shape to the given position
        
        Returns:
            dict with 'shape_dist_traveled' and 'closest_lat', 'closest_lon'
            or None if shape not found
        """
        shape_points = self.get_shape_for_trip(trip_id)
        if not shape_points or len(shape_points) == 0:
            return None
        
        min_distance = float('inf')
        closest_point = None
        
        for point in shape_points:
            distance = self.haversine_distance(
                lat, lon,
                point["lat"], point["lon"]
            )
            
            if distance < min_distance:
                min_distance = distance
                closest_point = point
        
        if closest_point:
            return {
                "shape_dist_traveled": closest_point.get("dist_traveled"),
                "closest_lat": closest_point["lat"],
                "closest_lon": closest_point["lon"],
                "distance_to_shape": min_distance
            }
        
        return None
    
    def get_two_closest_shape_points_bearing(
        self,
        trip_id: str,
        lat: float,
        lon: float
    ) -> Optional[int]:
        """
        Find the closest shape point, then use it with its next consecutive point
        to calculate bearing in the direction of travel
        
        Returns:
            Bearing in degrees (0-360) as integer or None if shape not found
        """
        shape_points = self.get_shape_for_trip(trip_id)
        if not shape_points or len(shape_points) < 2:
            return None
        
        # Find closest shape point to vehicle position
        min_distance = float('inf')
        closest_index = 0
        
        for i, point in enumerate(shape_points):
            distance = self.haversine_distance(
                lat, lon,
                point["lat"], point["lon"]
            )
            if distance < min_distance:
                min_distance = distance
                closest_index = i
        
        # Use closest point and the next consecutive point in the sequence
        # This ensures we always get the direction of travel along the route
        if closest_index < len(shape_points) - 1:
            # Use current and next point (forward direction)
            point_1 = shape_points[closest_index]
            point_2 = shape_points[closest_index + 1]
        else:
            # We're at the last point, use previous and current (still forward)
            point_1 = shape_points[closest_index - 1]
            point_2 = shape_points[closest_index]
        
        return self.calculate_bearing(
            point_1["lat"], point_1["lon"],
            point_2["lat"], point_2["lon"]
        )
