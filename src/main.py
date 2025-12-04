"""FastAPI application entry point"""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .config import settings
from .ingestion_service import IngestionService

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global ingestion service instance
ingestion_service: IngestionService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager
    
    Handles startup and shutdown events
    """
    global ingestion_service
    
    # Startup
    logger.info("Application starting up")
    ingestion_service = IngestionService()
    await ingestion_service.start()
    
    yield
    
    # Shutdown
    logger.info("Application shutting down")
    await ingestion_service.stop()


# Create FastAPI application
app = FastAPI(
    title=settings.app_name,
    description="Vehicle location ingestion service for Carris GTFS real-time data",
    version="1.0.0",
    lifespan=lifespan
)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": settings.app_name,
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
async def health_check():
    """
    Health check endpoint
    
    Verifies Redis and GTFS API connectivity
    """
    redis_ok = await ingestion_service.redis_client.ping()
    gtfs_ok = await ingestion_service.gtfs_fetcher.health_check()
    
    healthy = redis_ok and gtfs_ok
    
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "status": "healthy" if healthy else "unhealthy",
            "redis": "connected" if redis_ok else "disconnected",
            "gtfs_api": "reachable" if gtfs_ok else "unreachable"
        }
    )


@app.get("/stats")
async def get_stats():
    """
    Get ingestion statistics
    
    Returns counts of active vehicles, vehicle states, and trip tracks
    """
    stats = await ingestion_service.get_stats()
    return stats


@app.post("/trigger")
async def manual_trigger():
    """
    Manually trigger an ingestion cycle
    
    Useful for testing and debugging
    """
    try:
        await ingestion_service.manual_trigger()
        return {"status": "success", "message": "Ingestion cycle triggered"}
    except Exception as e:
        logger.error(f"Manual trigger failed: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)}
        )


@app.get("/vehicles")
async def get_active_vehicles():
    """
    Get list of currently active vehicle IDs
    """
    vehicle_ids = await ingestion_service.redis_client.get_active_vehicles()
    return {
        "count": len(vehicle_ids),
        "vehicle_ids": sorted(list(vehicle_ids))
    }


@app.get("/vehicles/{vehicle_id}")
async def get_vehicle_state(vehicle_id: str):
    """
    Get current state of a specific vehicle
    """
    state = await ingestion_service.redis_client.get_vehicle_state(vehicle_id)
    
    if not state:
        return JSONResponse(
            status_code=404,
            content={"error": "Vehicle not found"}
        )
    
    return state


@app.get("/trips/{trip_id}/track")
async def get_trip_track(trip_id: str, count: int = 100):
    """
    Get position track for a specific trip
    
    Args:
        trip_id: Trip identifier
        count: Maximum number of positions to return (default: 100)
    """
    entries = await ingestion_service.redis_client.get_trip_track(
        trip_id,
        count=count
    )
    
    if not entries:
        return JSONResponse(
            status_code=404,
            content={"error": "Trip not found"}
        )
    
    # Format entries
    positions = []
    for entry_id, data in entries:
        positions.append({
            "id": entry_id,
            "data": data
        })
    
    return {
        "trip_id": trip_id,
        "count": len(positions),
        "positions": positions
    }


@app.get("/trips/{trip_id}/status")
async def get_trip_status(trip_id: str):
    """
    Get status of a specific trip
    """
    status = await ingestion_service.redis_client.get_trip_status(trip_id)
    
    if not status:
        return JSONResponse(
            status_code=404,
            content={"error": "Trip not found or expired"}
        )
    
    return {
        "trip_id": trip_id,
        "status": status
    }


@app.get("/trips/{trip_id}/completion")
async def get_trip_completion(trip_id: str):
    """
    Get completion metrics for a completed trip
    
    Returns:
        - trip_id: Trip identifier
        - vehicle_id: Vehicle that completed the trip
        - license_plate: Vehicle license plate
        - start_time: Unix timestamp of first position (actual)
        - end_time: Unix timestamp of last position (actual)
        - duration_seconds: Trip duration in seconds (actual)
        - stops_served: Number of unique stops served
        - total_positions: Total position records
        - route_short_name: Route number/identifier
        - route_long_name: Route full name/description
        - scheduled_start_time: Scheduled departure time from GTFS
        - scheduled_end_time: Scheduled arrival time from GTFS
        - completed_at: When the completion was recorded
    """
    completion = await ingestion_service.redis_client.get_trip_completion(trip_id)
    
    if not completion:
        return JSONResponse(
            status_code=404,
            content={"error": "Trip completion data not found or expired"}
        )
    
    return {
        "trip_id": completion.get("trip_id"),
        "vehicle_id": completion.get("vehicle_id"),
        "license_plate": completion.get("license_plate"),
        "start_time": int(completion.get("start_time", 0)),
        "end_time": int(completion.get("end_time", 0)),
        "duration_seconds": int(completion.get("duration_seconds", 0)),
        "stops_served": int(completion.get("stops_served", 0)),
        "total_positions": int(completion.get("total_positions", 0)),
        "route_short_name": completion.get("route_short_name"),
        "route_long_name": completion.get("route_long_name"),
        "scheduled_start_time": completion.get("scheduled_start_time"),
        "scheduled_end_time": completion.get("scheduled_end_time"),
        "completed_at": completion.get("completed_at")
    }
