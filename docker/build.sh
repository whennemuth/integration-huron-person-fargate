#!/bin/bash
#
# Build Docker image for Huron Person processing
# Optimized for AWS Fargate with Graviton2 (ARM64) processors
#
# Uses Docker Compose for consistent builds
# Usage: ./build.sh [tag]
#

set -e

# Configuration
IMAGE_TAG="${1:-latest}"

echo "============================================"
echo "Building Huron Person Processor Docker Image"
echo "============================================"
echo "Tag: ${IMAGE_TAG}"
echo "Platform: linux/arm64"
echo ""

# Navigate to docker directory
cd "$(dirname "$0")"

# Build using Docker Compose
echo "Building via Docker Compose..."
docker-compose build

# Tag with custom tag if provided and not 'latest'
if [[ "$IMAGE_TAG" != "latest" ]]; then
  echo ""
  echo "Tagging image as ${IMAGE_TAG}..."
  docker tag huron-person-processor:latest huron-person-processor:${IMAGE_TAG}
fi

echo ""
echo "✓ Build complete!"
echo ""
echo "Image details:"
docker images huron-person-processor --filter "dangling=false"

echo ""
echo "To run locally:"
echo "  Normal mode (chunker):   docker-compose up chunker"
echo "  Normal mode (processor): docker-compose up processor"
echo "  Normal mode (merger):    docker-compose up merger"
echo "  Manual mode (chunker):   docker-compose -f docker-compose.yml -f docker-compose.manual.yml up -d chunker"
echo "                           docker exec -it huron-chunker sh"
echo ""
echo "Or use the run script:"
echo "  ./run.sh chunker"
echo "  ./run.sh processor"
echo "  ./run.sh merger"
echo "  ./run.sh chunker --manual"
