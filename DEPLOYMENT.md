# Deployment Guide

## Quick Start - Build and Deploy

### 1. Build the Docker Image

```bash
./build.sh
```

Or manually:
```bash
docker build -t carris-ingestion:latest .
```

### 2. Run with Docker

```bash
docker run -d \
  --name carris-ingestion \
  -p 8000:8000 \
  -e GTFS_API_URL='https://gateway.carris.pt/gateway/gtfs/api/v2.11/GTFS/realtime/vehiclepositions' \
  -e POLL_INTERVAL_SECONDS=30 \
  -e REDIS_HOST='blogs.berzuk.com' \
  -e REDIS_PORT=6379 \
  -e REDIS_PASSWORD='' \
  -e REDIS_DB=0 \
  -e LOG_LEVEL=INFO \
  carris-ingestion:latest
```

### 3. Or use Docker Compose

```bash
# Start
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

## Transfer to Production Server

### Option 1: Docker Registry (Recommended)

```bash
# Tag for registry
docker tag carris-ingestion:latest your-registry.com/carris-ingestion:latest

# Push to registry
docker push your-registry.com/carris-ingestion:latest

# On production server, pull and run
docker pull your-registry.com/carris-ingestion:latest
docker run -d --name carris-ingestion -p 8000:8000 \
  -e REDIS_HOST='blogs.berzuk.com' \
  your-registry.com/carris-ingestion:latest
```

### Option 2: Save and Transfer Image

```bash
# On build machine
docker save carris-ingestion:latest | gzip > carris-ingestion.tar.gz

# Transfer to production server (using scp)
scp carris-ingestion.tar.gz user@blogs.berzuk.com:/tmp/

# On production server
gunzip -c /tmp/carris-ingestion.tar.gz | docker load
docker run -d --name carris-ingestion -p 8000:8000 \
  -e REDIS_HOST='localhost' \
  carris-ingestion:latest
```

## Environment Variables

Configure via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `GTFS_API_URL` | Carris GTFS API endpoint | https://gateway.carris.pt/.../vehiclepositions |
| `POLL_INTERVAL_SECONDS` | How often to fetch data | 30 |
| `REDIS_HOST` | Redis server hostname | localhost |
| `REDIS_PORT` | Redis server port | 6379 |
| `REDIS_PASSWORD` | Redis password (if required) | (empty) |
| `REDIS_DB` | Redis database number | 0 |
| `LOG_LEVEL` | Logging level | INFO |

## Monitoring

### Health Check
```bash
curl http://localhost:8000/health
```

### Statistics
```bash
curl http://localhost:8000/stats
```

### Active Vehicles
```bash
curl http://localhost:8000/vehicles
```

### Container Logs
```bash
# Docker
docker logs -f carris-ingestion

# Docker Compose
docker-compose logs -f ingestion
```

## Performance Testing on Production

1. **Deploy the container**
2. **Monitor the logs** to verify ingestion cycle times
3. **Check Redis performance** with timing info in logs
4. **Test API endpoints** for response times

Expected performance:
- **Ingestion cycle**: < 3 seconds (with pipeline optimization)
- **API response**: < 100ms for most endpoints
- **Memory usage**: ~100-200MB

## Troubleshooting

### Container won't start
```bash
docker logs carris-ingestion
```

### Redis connection issues
```bash
# Test Redis connectivity from container
docker exec carris-ingestion python -c "
import redis.asyncio as aioredis
import asyncio
async def test():
    r = await aioredis.from_url('redis://blogs.berzuk.com:6379')
    print(await r.ping())
asyncio.run(test())
"
```

### Performance issues
- Check Redis network latency
- Verify GTFS API response times
- Monitor container resources: `docker stats carris-ingestion`

## Updating

```bash
# Stop old container
docker stop carris-ingestion
docker rm carris-ingestion

# Pull/load new image
docker load < carris-ingestion-new.tar.gz

# Start new container
docker run -d --name carris-ingestion ...
```

## Security Notes

- Container runs as non-root user (uid 1000)
- No sensitive data in image
- Redis password should be set via environment variable
- Use Docker secrets in production for sensitive values
