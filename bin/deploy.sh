#!/bin/bash

# Deploy CDK stack
# Usage: ./bin/deploy.sh

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Deploying CDK Stack ===${NC}"
echo ""

# Change to project root
cd "$(dirname "$0")/.."

# Build TypeScript
echo -e "${GREEN}Step 1: Building TypeScript...${NC}"
npm run build

# Deploy stack (automatically synthesizes)
echo -e "${GREEN}Step 2: Deploying stack...${NC}"
cdk deploy --all --no-rollback --require-approval never

echo ""
echo -e "${GREEN}=== Deployment Complete! ===${NC}"
echo ""
echo -e "${YELLOW}Stack outputs:${NC}"
cdk output --all

echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Build and push Docker image: npm run docker-publish"
echo "2. Upload a JSON file to the input bucket to trigger processing"
echo "3. Monitor CloudWatch logs: /ecs/huron-person-chunker and /ecs/huron-person-processor"
echo "4. Check SQS queue depth for processing progress"
