"""GTFS Real-time API fetcher for vehicle positions"""

import logging
from typing import Optional

import httpx
from google.transit import gtfs_realtime_pb2

logger = logging.getLogger(__name__)


class GTFSFetcher:
    """
    Decoupled HTTP client for fetching GTFS real-time vehicle positions
    
    Fetches protobuf data from the Carris GTFS API endpoint
    """
    
    def __init__(self, api_url: str, timeout: int = 10):
        self.api_url = api_url
        self.timeout = timeout
        self.client: Optional[httpx.AsyncClient] = None
        
    async def connect(self):
        """Initialize HTTP client"""
        self.client = httpx.AsyncClient(
            timeout=self.timeout,
            follow_redirects=True
        )
        logger.info(f"GTFS Fetcher initialized for: {self.api_url}")
        
    async def disconnect(self):
        """Close HTTP client"""
        if self.client:
            await self.client.aclose()
            logger.info("GTFS Fetcher closed")
    
    async def fetch_vehicle_positions(self) -> Optional[gtfs_realtime_pb2.FeedMessage]:
        """
        Fetch and decode vehicle positions from GTFS API
        
        Returns:
            FeedMessage protobuf object or None if error occurs
        """
        try:
            logger.debug(f"Fetching vehicle positions from {self.api_url}")
            
            response = await self.client.get(self.api_url)
            response.raise_for_status()
            
            # Parse protobuf
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            
            entity_count = len(feed.entity)
            logger.info(f"Successfully fetched {entity_count} vehicle positions")
            
            return feed
            
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching vehicle positions: {e.response.status_code}")
            return None
            
        except httpx.RequestError as e:
            logger.error(f"Request error fetching vehicle positions: {e}")
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error fetching vehicle positions: {e}")
            return None
    
    async def health_check(self) -> bool:
        """
        Check if the GTFS API endpoint is accessible
        
        Returns:
            True if endpoint is reachable, False otherwise
        """
        try:
            response = await self.client.get(self.api_url)
            return response.status_code == 200
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False
