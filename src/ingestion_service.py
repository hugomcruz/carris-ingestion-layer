"""Ingestion orchestrator - coordinates the entire pipeline"""

import asyncio
import logging
from typing import Optional

from .config import settings
from .redis_client import RedisClient
from .gtfs_fetcher import GTFSFetcher
from .gtfs_enrichment import GTFSEnrichment
from .normalizer import DataNormalizer
from .trip_detector import TripTransitionDetector
from .publisher import DataPublisher

logger = logging.getLogger(__name__)


class IngestionService:
    """
    Main ingestion service that orchestrates the data pipeline
    
    Pipeline flow:
    1. Fetch vehicle positions from GTFS API (GTFSFetcher)
    2. Normalize protobuf data (DataNormalizer)
    3. Detect trip transitions (TripTransitionDetector)
    4. Publish to Redis (DataPublisher)
    
    Runs as a background task on a configurable schedule
    """
    
    def __init__(self):
        # Initialize components
        self.redis_client = RedisClient(settings.redis_url)
        self.gtfs_fetcher = GTFSFetcher(settings.gtfs_api_url)
        self.gtfs_enrichment = GTFSEnrichment(
            settings.db_url, 
            settings.gtfs_refresh_hour
        )
        self.normalizer = DataNormalizer()
        self.trip_detector = TripTransitionDetector(self.redis_client, self.gtfs_enrichment)
        self.publisher = DataPublisher(
            self.redis_client, 
            self.trip_detector,
            self.gtfs_enrichment
        )
        
        # State
        self.is_running = False
        self.task: Optional[asyncio.Task] = None
        self.cleanup_task: Optional[asyncio.Task] = None
        self.gtfs_refresh_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Initialize all components and start background task"""
        logger.info("Starting Ingestion Service")
        
        # Connect to dependencies
        await self.redis_client.connect()
        await self.gtfs_fetcher.connect()
        await self.gtfs_enrichment.connect()
        
        # Load GTFS data into memory on startup
        await self.gtfs_enrichment.load_data_on_startup()
        
        # Start background polling task
        self.is_running = True
        self.task = asyncio.create_task(self._polling_loop())
        
        # Start background cleanup task
        self.cleanup_task = asyncio.create_task(self._cleanup_loop())
        
        # Start GTFS refresh task
        self.gtfs_refresh_task = asyncio.create_task(self._gtfs_refresh_loop())
        
        logger.info("Ingestion Service started")
    
    async def stop(self):
        """Stop background task and disconnect from dependencies"""
        logger.info("Stopping Ingestion Service")
        
        # Stop polling loop
        self.is_running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        # Stop cleanup loop
        if self.cleanup_task:
            self.cleanup_task.cancel()
            try:
                await self.cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Stop GTFS refresh loop
        if self.gtfs_refresh_task:
            self.gtfs_refresh_task.cancel()
            try:
                await self.gtfs_refresh_task
            except asyncio.CancelledError:
                pass
        
        # Disconnect from dependencies
        await self.gtfs_fetcher.disconnect()
        await self.gtfs_enrichment.disconnect()
        await self.redis_client.disconnect()
        
        logger.info("Ingestion Service stopped")
    
    async def _polling_loop(self):
        """
        Main polling loop - runs every POLL_INTERVAL_SECONDS
        """
        logger.info(f"Starting polling loop (interval: {settings.poll_interval_seconds}s)")
        
        while self.is_running:
            try:
                await self._ingest_cycle()
            except Exception as e:
                logger.error(f"Error in ingestion cycle: {e}", exc_info=True)
            
            # Wait for next cycle
            await asyncio.sleep(settings.poll_interval_seconds)
    
    async def _cleanup_loop(self):
        """
        Background cleanup loop - runs asynchronously from ingestion
        
        Checks for and removes inactive vehicles at regular intervals
        """
        logger.info(
            f"Starting cleanup loop (interval: {settings.poll_interval_seconds}s, "
            f"timeout: {settings.vehicle_inactivity_timeout_seconds}s)"
        )
        
        while self.is_running:
            try:
                await self._cleanup_cycle()
            except Exception as e:
                logger.error(f"Error in cleanup cycle: {e}", exc_info=True)
            
            # Wait for next cleanup cycle
            await asyncio.sleep(settings.poll_interval_seconds)
    
    async def _cleanup_cycle(self):
        """
        Single cleanup cycle - removes inactive vehicles
        """
        logger.debug("Starting cleanup cycle")
        await self.publisher.cleanup_inactive_vehicles(
            settings.vehicle_inactivity_timeout_seconds
        )
    
    async def _gtfs_refresh_loop(self):
        """
        Background GTFS refresh loop - checks daily at configured hour
        
        Checks if cache file has been updated and reloads from cache if changed
        """
        logger.info(f"Starting GTFS refresh loop (refresh at {settings.gtfs_refresh_hour}:00)")
        
        while self.is_running:
            try:
                if self.gtfs_enrichment.should_refresh():
                    logger.info("GTFS data refresh check triggered")
                    await self.gtfs_enrichment.refresh_data_daily()
            except Exception as e:
                logger.error(f"Error in GTFS refresh cycle: {e}", exc_info=True)
            
            # Check every 5 minutes if refresh is needed
            await asyncio.sleep(300)
    
    async def _ingest_cycle(self):
        """
        Single ingestion cycle
        
        Executes the complete pipeline:
        Fetch -> Normalize -> Detect -> Publish
        """
        logger.info("Starting ingestion cycle")
        
        # 1. Fetch vehicle positions from GTFS API
        feed = await self.gtfs_fetcher.fetch_vehicle_positions()
        if not feed:
            logger.warning("Failed to fetch vehicle positions, skipping cycle")
            return
        
        # 2. Normalize protobuf data to application models
        positions = self.normalizer.normalize_feed(feed)
        if not positions:
            logger.warning("No positions to process, skipping cycle")
            return
        
        # 3. Publish to Redis (includes trip transition detection)
        await self.publisher.publish_positions(positions)
        
        logger.info(f"Ingestion cycle completed - processed {len(positions)} vehicles")
    
    async def manual_trigger(self):
        """
        Manually trigger an ingestion cycle (for testing/debugging)
        
        Returns:
            Number of positions processed
        """
        logger.info("Manual ingestion trigger")
        await self._ingest_cycle()
    
    async def get_stats(self):
        """Get ingestion statistics from Redis"""
        return await self.redis_client.get_stats()
