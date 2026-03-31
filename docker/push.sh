#!/bin/bash

# Push Docker image to ECR
# Handles ECR login, tagging, and pushing
# Usage: ./docker/push.sh [tag]

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

# Get ECR repository URI
REPOSITORY_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${REPOSITORY_NAME}"

echo -e "${GREEN}=== Pushing Docker Image to ECR ===${NC}"
echo "Repository: $REPOSITORY_NAME"
echo "Tag: $IMAGE_TAG"
echo "Region: $REGION"
echo "Repository URI: $REPOSITORY_URI"
echo ""

# Login to ECR
echo -e "${GREEN}Logging in to ECR...${NC}"
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $REPOSITORY_URI

# Tag and push logic
if [ -z "$IMAGE_TAG" ] || [ "$IMAGE_TAG" = "latest" ]; then
    # Only tag and push as 'latest' once
    echo -e "${GREEN}Tagging image for ECR...${NC}"
    docker tag $REPOSITORY_NAME:latest $REPOSITORY_URI:latest
    
    echo -e "${GREEN}Pushing image to ECR...${NC}"
    docker push $REPOSITORY_URI:latest
    
    echo "Image pushed to: $REPOSITORY_URI:latest"
else
    # Tag and push both the specified tag AND 'latest'
    echo -e "${GREEN}Tagging image for ECR...${NC}"
    docker tag $REPOSITORY_NAME:$IMAGE_TAG $REPOSITORY_URI:$IMAGE_TAG
    docker tag $REPOSITORY_NAME:latest $REPOSITORY_URI:latest
    
    echo -e "${GREEN}Pushing image to ECR...${NC}"
    docker push $REPOSITORY_URI:$IMAGE_TAG
    docker push $REPOSITORY_URI:latest
    
    echo "Image pushed to: $REPOSITORY_URI:$IMAGE_TAG"
    echo "Latest tag: $REPOSITORY_URI:latest"
fi
