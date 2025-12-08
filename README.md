# Carris Vehicle Location Ingestion Service

A modular, decoupled FastAPI service for ingesting real-time vehicle location data from the Carris GTFS API and storing it in Redis.

## 🏗️ Architecture

The service follows a clean, modular architecture with separated concerns:

```
┌─────────────────────────────────────────────────────────────────┐
│                        FastAPI Application                       │
│                  (Health checks, Stats, Triggers)                │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Ingestion Service                            │
│              (Orchestrates the pipeline every 30s)               │
└──┬───────────────────┬────────────────┬──────────────────────┬──┘
   │                   │                │                      │
   ▼                   ▼                ▼                      ▼
┌────────┐      ┌──────────┐    ┌────────────┐      ┌──────────────┐
│ GTFS   │      │  Data    │    │   Trip     │      │     Data     │
│Fetcher │─────▶│Normalizer│───▶│  Detector  │─────▶│  Publisher   │
└────────┘      └──────────┘    └────────────┘      └───────┬──────┘
     │                                                        │
     ▼                                                        ▼
┌─────────────┐                                    ┌──────────────────┐
│ Carris GTFS │                                    │  Redis Client    │
│     API     │                                    │                  │
└─────────────┘                                    └────────┬─────────┘
                                                            │
                                                            ▼
                                                   ┌─────────────────┐
                                                   │      Redis      │
                                                   │                 │
                                                   │ • Vehicle HASH  │
                                                   │ • Trip STREAM   │
                                                   │ • Active SET    │
                                                   └─────────────────┘
```

## 📦 Components

### Core Modules

1. **GTFSFetcher** (`gtfs_fetcher.py`)
   - Fetches protobuf data from Carris API
   - Handles HTTP requests and error recovery
   - Decoupled from processing logic

2. **DataNormalizer** (`normalizer.py`)
   - Transforms GTFS protobuf into Pydantic models
   - Extracts and validates vehicle position data
   - Independent of data sources and storage

3. **TripTransitionDetector** (`trip_detector.py`)
   - Detects when vehicles change trips
   - Compares current vs previous trip IDs
   - Manages trip lifecycle

4. **DataPublisher** (`publisher.py`)
   - Publishes data to Redis structures
   - Coordinates all write operations
   - Handles cleanup of inactive vehicles

5. **RedisClient** (`redis_client.py`)
   - Encapsulates all Redis operations
   - Provides clean async interface
   - Manages three data structures

6. **IngestionService** (`ingestion_service.py`)
   - Orchestrates the entire pipeline
   - Runs background polling task
   - Coordinates all components

### Data Models (`models.py`)

- `VehiclePosition`: Normalized vehicle location data
- `VehicleState`: Redis HASH representation
- `TripPosition`: Stream entry for trip tracks
- `TripTransition`: Trip change events

### Configuration (`config.py`)

- Environment-based settings using Pydantic
- Redis connection configuration
- API endpoints and polling intervals

## 🗄️ Redis Data Structures

### 1. Latest Vehicle State (HASH)
```
Key: vehicle:{vehicle_id}
Type: HASH
Purpose: Store current state of each vehicle
Fields:
  - vehicle_id
  - trip_id
  - route_id
  - latitude, longitude
  - bearing, speed
  - timestamp
  - current_status
  - stop_id
  - service_date (YYYYMMDD format)
  - last_updated
```

### 2. Active Trip Track (STREAM)
```
Key: trip:{trip_id}:{service_date}:track
Type: STREAM
Purpose: Append-only log of positions for each trip instance
Note: service_date is in YYYYMMDD format (e.g., 20251204)
      This ensures trips are unique per day and handles midnight crossings
Entries:
  - vehicle_id
  - lat, lon
  - bearing, speed
  - timestamp
  - status
  - stop_id
  - service_date
```

### 3. Activity Index (SET)
```
Key: active_vehicles
Type: SET
Purpose: Track which vehicles are currently active
Members: vehicle_id strings
```

### 4. Trip Status (STRING with TTL)
```
Key: trip:{trip_id}:{service_date}:status
Type: STRING
Purpose: Track trip lifecycle (active/completed)
Note: service_date ensures each trip instance is tracked separately
TTL: 1 hour (active), 24 hours (completed)
```

## 📅 Service Date Handling

The service uses a `service_date` field (YYYYMMDD format) to uniquely identify trip instances per day. This solves the problem that GTFS `trip_id` values repeat daily.

**Key Behaviors:**

1. **Derivation**: Service date is derived from the vehicle's timestamp in Lisbon timezone when a trip starts
2. **Persistence**: Once assigned, the service date is stored in Redis vehicle state and persists throughout the trip
3. **Midnight Crossing**: Trips that cross midnight maintain their original service date
   - Example: A trip starting at 11:30 PM on Dec 4 keeps `service_date=20251204` even after midnight
4. **Unique Keys**: Each trip instance gets unique Redis keys: `trip:{trip_id}:20251204:track`

**Benefits:**
- Prevents data collisions between same trip on different days
- Handles trips crossing midnight correctly
- Enables day-based queries and retention policies
- Aligns with transit industry conventions

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- Redis 7+
- Docker & Docker Compose (optional)

### Local Development

1. **Clone and setup**
```bash
cd ingestion-layer
cp .env.example .env
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Start Redis**
```bash
# Using Docker
docker run -d -p 6379:6379 redis:7-alpine

# Or install locally
brew install redis  # macOS
redis-server
```

4. **Run the application**
```bash
# Development mode with auto-reload and custom logging
uvicorn src.main:app --reload --log-config logging_config.json

# Production mode
uvicorn src.main:app --host 0.0.0.0 --port 8000 --log-config logging_config.json
```

### Docker Deployment

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f ingestion

# Stop services
docker-compose down
```

## 📡 API Endpoints

### Health & Status

- `GET /` - Service information
- `GET /health` - Health check (Redis + GTFS API connectivity)
- `GET /stats` - Storage statistics

### Vehicle Operations

- `GET /vehicles` - List all active vehicles
- `GET /vehicles/{vehicle_id}` - Get vehicle state

### Trip Operations

- `GET /trips/{trip_id}/track?count=100` - Get trip position history
- `GET /trips/{trip_id}/status` - Get trip status

### Manual Control

- `POST /trigger` - Manually trigger ingestion cycle

## 🔧 Configuration

Environment variables (`.env`):

```env
# GTFS API
GTFS_API_URL=https://gateway.carris.pt/gateway/gtfs/api/v2.11/GTFS/realtime/vehiclepositions
POLL_INTERVAL_SECONDS=30

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_PASSWORD=
REDIS_DB=0

# Application
APP_NAME=Carris Vehicle Ingestion
LOG_LEVEL=INFO
```

## 🧪 Testing

```bash
# Test health check
curl http://localhost:8000/health

# Get statistics
curl http://localhost:8000/stats

# Trigger manual ingestion
curl -X POST http://localhost:8000/trigger

# Get active vehicles
curl http://localhost:8000/vehicles

# Get specific vehicle
curl http://localhost:8000/vehicles/1234

# Get trip track
curl http://localhost:8000/trips/trip123/track?count=50
```

## 📊 Monitoring

The service provides several endpoints for monitoring:

1. **Health Status**: `/health` - Checks Redis and GTFS API connectivity
2. **Statistics**: `/stats` - Active vehicles, total states, trip tracks
3. **Active Vehicles**: `/vehicles` - Current active vehicle count and IDs

## 🔄 Data Flow

1. **Every 30 seconds** (configurable):
   - Fetch vehicle positions from GTFS API (protobuf)
   - Decode and normalize to Pydantic models
   - Detect trip transitions for each vehicle
   - Update vehicle state HASH
   - Append positions to trip STREAM
   - Update active vehicles SET
   - Set/update trip status keys
   - Clean up inactive vehicles

2. **On trip transition**:
   - Mark previous trip as completed
   - Trim old trip track to save space
   - Start tracking new trip

## 🛠️ Development

### Project Structure

```
ingestion-layer/
├── src/
│   ├── __init__.py
│   ├── config.py              # Settings and configuration
│   ├── models.py              # Pydantic data models
│   ├── redis_client.py        # Redis operations
│   ├── gtfs_fetcher.py        # GTFS API client
│   ├── normalizer.py          # Data transformation
│   ├── trip_detector.py       # Trip transition logic
│   ├── publisher.py           # Data publishing
│   ├── ingestion_service.py   # Main orchestrator
│   └── main.py                # FastAPI application
├── requirements.txt           # Python dependencies
├── Dockerfile                 # Container image
├── docker-compose.yml         # Multi-container setup
├── .env.example               # Environment template
└── README.md                  # This file
```

### Adding New Features

The modular architecture makes it easy to extend:

- **New data sources**: Create a new fetcher class
- **Different storage**: Implement a new client interface
- **Additional processing**: Add modules to the pipeline
- **New endpoints**: Add routes to `main.py`

## 📝 License

MIT

## 🤝 Contributing

Contributions welcome! The modular design makes it easy to:
- Add new features
- Improve individual components
- Enhance error handling
- Add tests

## 📞 Support

For issues or questions, please check the logs:

```bash
# Docker
docker-compose logs -f ingestion

# Local
# Logs are output to console with timestamps
```
