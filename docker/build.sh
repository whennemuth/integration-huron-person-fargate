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
BASE_IMAGE_NAME="huron-person-integration"

echo "================================================"
echo "Building Huron Person Integration Docker Image"
echo "================================================"
echo "Tag: ${IMAGE_TAG}"
echo "Platform: linux/arm64"
echo "Format: Docker v2 (ECS compatible)"
echo ""

# Navigate to docker directory
cd "$(dirname "$0")"

# Build using Docker BuildKit with v2 manifest format
# - Uses buildx for faster builds with parallel stage execution
# - --output type=docker creates Docker v2 manifests (ECS compatible, not OCI index)
# - --provenance=false disables attestations that require OCI index format
# - Works in GitHub Actions, local dev, and CI/CD environments
echo "Building via Docker BuildKit..."
docker buildx build \
  --platform linux/arm64 \
  --output type=docker \
  --provenance=false \
  --tag ${BASE_IMAGE_NAME}:latest \
  --file Dockerfile \
  ..

# Tag with custom tag if provided and not 'latest'
if [[ "$IMAGE_TAG" != "latest" ]]; then
  echo ""
  echo "Tagging image as ${IMAGE_TAG}..."
  docker tag ${BASE_IMAGE_NAME}:latest ${BASE_IMAGE_NAME}:${IMAGE_TAG}
fi

echo ""
echo "✓ Build complete!"
echo ""
echo "Image details:"
docker images ${BASE_IMAGE_NAME} --filter "dangling=false"

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
