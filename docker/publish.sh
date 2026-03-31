#!/bin/bash

# Publish Docker image to ECR
# Builds the image using build.sh, then pushes to ECR
# Usage: ./docker/publish.sh [tag]

set -e

# Configuration
REGION=${REGION:-us-east-2}
AWS_ACCOUNT_ID=${AWS_ACCOUNT_ID:-770203350335}
REPOSITORY_NAME=${REPOSITORY_NAME:-huron-person-processor}
IMAGE_TAG=${1:-latest}

# Get configuration from context.json using helper script
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GET_CONTEXT="$SCRIPT_DIR/../bin/get-context.js"
if [ -f "$GET_CONTEXT" ] && command -v node >/dev/null 2>&1; then
    REPOSITORY_NAME=$(node "$GET_CONTEXT" ECR.repositoryName 2>/dev/null || echo "$REPOSITORY_NAME")
    AWS_ACCOUNT_ID=$(node "$GET_CONTEXT" ACCOUNT 2>/dev/null || echo "$AWS_ACCOUNT_ID")
    REGION=$(node "$GET_CONTEXT" REGION 2>/dev/null || echo "$REGION")
fi

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Publishing Docker Image to ECR ===${NC}"
echo "Repository: $REPOSITORY_NAME"
echo "Tag: $IMAGE_TAG"
echo "Region: $REGION"
echo "Account: $AWS_ACCOUNT_ID"
echo ""

# Get ECR repository URI
REPOSITORY_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPOSITORY_NAME}"
echo -e "${YELLOW}Repository URI: $REPOSITORY_URI${NC}"
echo ""

# Step 1: Build the image using build.sh
echo -e "${GREEN}Step 1: Building Docker image...${NC}"
cd "$SCRIPT_DIR"
./build.sh "$IMAGE_TAG"
echo ""

# Step 2: Check if repository exists, create if it doesn't
echo -e "${GREEN}Step 2: Checking if ECR repository exists...${NC}"
if ! aws ecr describe-repositories --repository-names $REPOSITORY_NAME --region $REGION >/dev/null 2>&1; then
    echo -e "${YELLOW}Repository doesn't exist, creating...${NC}"
    aws ecr create-repository --repository-name $REPOSITORY_NAME --region $REGION
fi

# Step 3: Push to ECR (handles login, tagging, and pushing)
echo -e "${GREEN}Step 3: Pushing to ECR...${NC}"
./push.sh "$IMAGE_TAG"

echo ""
echo -e "${GREEN}=== Success! ===${NC}"
if [ -z "$IMAGE_TAG" ] || [ "$IMAGE_TAG" = "latest" ]; then
    echo "Image pushed to: $REPOSITORY_URI:latest"
else
    echo "Image pushed to: $REPOSITORY_URI:$IMAGE_TAG"
    echo "Latest tag: $REPOSITORY_URI:latest"
fi
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Deploy CDK stack: npm run deploy"
echo "2. Upload a JSON file to S3 to trigger processing"
