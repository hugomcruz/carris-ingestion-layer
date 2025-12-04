"""Utility functions for the ingestion service"""

from datetime import datetime
import pytz


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
