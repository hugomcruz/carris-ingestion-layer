#!/bin/bash

# Carris Vehicle Ingestion - Build & Deploy Script

set -e

# Configuration
IMAGE_NAME="carris-ingestion"
IMAGE_TAG="${1:-latest}"
FULL_IMAGE_NAME="${IMAGE_NAME}:${IMAGE_TAG}"

echo "🏗️  Building Docker image: ${FULL_IMAGE_NAME}"

# Build the Docker image
docker build -t ${FULL_IMAGE_NAME} .

echo "✅ Build complete!"
echo ""
echo "📦 Image: ${FULL_IMAGE_NAME}"
echo ""
echo "To run the container, use:"
echo ""
echo "  docker run -d \\"
echo "    --name carris-ingestion \\"
echo "    -p 8000:8000 \\"
echo "    -e GTFS_API_URL='https://gateway.carris.pt/gateway/gtfs/api/v2.11/GTFS/realtime/vehiclepositions' \\"
echo "    -e POLL_INTERVAL_SECONDS=30 \\"
echo "    -e REDIS_HOST='blogs.berzuk.com' \\"
echo "    -e REDIS_PORT=6379 \\"
echo "    -e REDIS_PASSWORD='' \\"
echo "    -e REDIS_DB=0 \\"
echo "    -e LOG_LEVEL=INFO \\"
echo "    ${FULL_IMAGE_NAME}"
echo ""
echo "Or use docker-compose:"
echo "  docker-compose up -d"
echo ""
echo "To save and transfer to another server:"
echo "  docker save ${FULL_IMAGE_NAME} | gzip > ${IMAGE_NAME}-${IMAGE_TAG}.tar.gz"
echo ""
echo "On the target server, load with:"
echo "  gunzip -c ${IMAGE_NAME}-${IMAGE_TAG}.tar.gz | docker load"
