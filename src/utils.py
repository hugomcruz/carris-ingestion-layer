"""Utility functions for the ingestion service"""

from datetime import datetime, timedelta
import pytz
from typing import Optional


# Lisbon timezone
LISBON_TZ = pytz.timezone('Europe/Lisbon')


def get_service_date(timestamp: int) -> str:
    """
    Derive service date from Unix timestamp in Lisbon timezone.
    
    Returns the date in YYYYMMDD format. This represents the date when
    the trip started, which remains consistent throughout the trip's
    lifecycle regardless of midnight transitions.
    
    Args:
        timestamp: Unix timestamp (seconds since epoch)
        
    Returns:
        Service date as YYYYMMDD string (e.g., "20251204")
    """
    dt = datetime.fromtimestamp(timestamp, tz=LISBON_TZ)
    return dt.strftime('%Y%m%d')


def gtfs_time_to_timestamp(gtfs_time: str, service_date: str) -> Optional[int]:
    """
    Convert GTFS time string to Unix timestamp based on service_date.
    
    GTFS times can exceed 24:00:00 for trips that go past midnight.
    For example, "25:30:00" means 01:30:00 the next day.
    
    Args:
        gtfs_time: GTFS time string in format "HH:MM:SS" (can be >24 hours)
        service_date: Service date in YYYYMMDD format
        
    Returns:
        Unix timestamp (seconds since epoch) or None if invalid
        
    Examples:
        >>> gtfs_time_to_timestamp("14:30:00", "20251207")
        # Returns timestamp for 2025-12-07 14:30:00 in Lisbon timezone
        
        >>> gtfs_time_to_timestamp("24:15:00", "20251207")
        # Returns timestamp for 2025-12-08 00:15:00 in Lisbon timezone
        
        >>> gtfs_time_to_timestamp("25:30:00", "20251207")
        # Returns timestamp for 2025-12-08 01:30:00 in Lisbon timezone
    """
    if not gtfs_time or not service_date:
        return None
    
    try:
        # Parse GTFS time (can be HH:MM:SS where HH >= 24)
        time_parts = gtfs_time.split(':')
        if len(time_parts) != 3:
            return None
        
        hours = int(time_parts[0])
        minutes = int(time_parts[1])
        seconds = int(time_parts[2])
        
        # Parse service date (YYYYMMDD)
        year = int(service_date[0:4])
        month = int(service_date[4:6])
        day = int(service_date[6:8])
        
        # Create base datetime for the service date at midnight in Lisbon timezone
        base_dt = LISBON_TZ.localize(datetime(year, month, day, 0, 0, 0))
        
        # Handle times past midnight (24:00+)
        if hours >= 24:
            # Add days for hours past 24
            days_to_add = hours // 24
            hours = hours % 24
            base_dt = base_dt + timedelta(days=days_to_add)
        
        # Add the time components
        final_dt = base_dt + timedelta(hours=hours, minutes=minutes, seconds=seconds)
        
        # Convert to Unix timestamp
        return int(final_dt.timestamp())
        
    except (ValueError, IndexError) as e:
        # Invalid time format or date
        return None
