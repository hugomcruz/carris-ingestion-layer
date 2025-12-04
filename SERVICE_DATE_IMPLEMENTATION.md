# Service Date Implementation Summary

## Problem
The Redis trip storage was using `trip_id` as the key (e.g., `trip:{trip_id}:track`), but GTFS `trip_id` values repeat daily. This caused:
- Data from different days to overwrite each other
- No way to distinguish the same trip on different service dates
- Issues with trips crossing midnight (changing Redis keys mid-trip)

## Solution
Implemented a `service_date` field (YYYYMMDD format) to uniquely identify trip instances per day.

## Changes Made

### 1. Data Models (`src/models.py`)
- Added `service_date` field to:
  - `VehiclePosition` (Optional[str])
  - `VehicleState` (Optional[str])
  - `TripPosition` (str - required)
  - `TripTransition` (previous_service_date and new_service_date)
  - `TripCompletion` (str - required)
- Updated `to_redis_dict()` and `to_stream_dict()` methods to include service_date

### 2. Utility Function (`src/utils.py`)
- Created new file with `get_service_date(timestamp: int) -> str` function
- Converts Unix timestamp to YYYYMMDD format in Lisbon timezone
- Used for deriving service date when not available from other sources

### 3. Data Normalizer (`src/normalizer.py`)
- Imports `get_service_date` utility
- Derives service_date from timestamp when creating VehiclePosition objects
- Only sets service_date if vehicle has a trip_id

### 4. Trip Detector (`src/trip_detector.py`)
- Retrieves service_date from previous vehicle state in Redis
- Uses service_date from current position or derives it as fallback
- Passes service_date through TripTransition events
- Updated `_calculate_trip_metrics()` to require service_date parameter
- Uses service_date when querying trip tracks from Redis

### 5. Redis Client (`src/redis_client.py`)
- Updated all trip-related methods to require `service_date` parameter:
  - `append_trip_position(trip_id, service_date, position)`
  - `get_trip_track(trip_id, service_date, ...)`
  - `get_full_trip_track(trip_id, service_date)`
  - `trim_trip_track(trip_id, service_date, ...)`
  - `delete_trip_track(trip_id, service_date)`
  - `set_trip_status(trip_id, service_date, ...)`
  - `get_trip_status(trip_id, service_date)`
  - `get_trip_completion(trip_id, service_date)`
  - `delete_trip_status(trip_id, service_date)`
- Updated Redis key patterns from `trip:{trip_id}:*` to `trip:{trip_id}:{service_date}:*`
- Updated `get_stats()` to use new key pattern for counting trip tracks

### 6. Data Publisher (`src/publisher.py`)
- Imports `get_service_date` utility
- Updated `_process_vehicle_position()` to set service_date in VehicleState:
  - Uses position.service_date if available
  - Falls back to old_state service_date if present
  - Derives new service_date if needed
- Updated TripPosition creation to include service_date (required field)
- Modified pipeline operations to use new Redis key pattern with service_date

### 7. Test File (`test_shape_speed.py`)
- Updated to read service_date from vehicle state
- Uses new key pattern `trip:{trip_id}:{service_date}:track` when querying tracks
- Added warning if vehicle has trip_id but missing service_date

### 8. Documentation (`README.md`)
- Updated Redis data structure documentation to show new key patterns
- Added service_date field to VehicleState HASH documentation
- Added comprehensive "Service Date Handling" section explaining:
  - How service_date is derived
  - Persistence behavior throughout trip lifecycle
  - Midnight crossing handling
  - Benefits of the approach

### 9. Dependencies (`requirements.txt`)
- Added `pytz==2024.1` for timezone handling

## Redis Key Changes

### Before
```
trip:{trip_id}:track           # Stream of positions
trip:{trip_id}:status          # Trip status
trip:{trip_id}:completion      # Trip completion metrics
```

### After
```
trip:{trip_id}:{service_date}:track       # e.g., trip:1234:20251204:track
trip:{trip_id}:{service_date}:status      # e.g., trip:1234:20251204:status
trip:{trip_id}:{service_date}:completion  # e.g., trip:1234:20251204:completion
```

## Service Date Behavior

1. **On Trip Start**: Service date is derived from the timestamp in Lisbon timezone when the first position is received
2. **During Trip**: Service date is persisted in Redis vehicle state and reused for all subsequent positions
3. **Midnight Crossing**: A trip starting at 11:30 PM on Dec 4 maintains `service_date=20251204` even after midnight
4. **New Trip**: When vehicle transitions to a new trip, a new service_date is assigned based on transition timestamp

## Benefits

✅ **Unique Trip Identification**: Each trip instance per day has a unique Redis key
✅ **Midnight Handling**: Trips crossing midnight don't change keys mid-trip
✅ **Data Isolation**: Prevents data collisions between same trip on different days
✅ **Query Flexibility**: Easy to query trips for a specific date using Redis pattern matching
✅ **Retention Policies**: Can implement TTL or cleanup based on service date
✅ **GTFS Alignment**: Concept aligns with GTFS static schedule service dates

## Testing Recommendations

1. Test with vehicles that cross midnight
2. Verify service_date persistence across multiple position updates
3. Check trip transition handling with service_date
4. Validate Redis key patterns are correct
5. Test fallback behavior when service_date is missing
