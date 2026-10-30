#!/bin/bash

# Deploy CDK stacks
# Usage: ./bin/deploy.sh [all|fargate|dashboard]   (default: all)
# Writes each deployed stack's template to cdk.out/synth.output.<fargate|dashboard>.yaml

set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

target="${1:-all}"
case "$target" in
  all|fargate|dashboard) ;;
  *) echo "Usage: $0 [all|fargate|dashboard]" >&2; exit 1 ;;
esac

echo -e "${GREEN}=== Deploying CDK Stack(s): $target ===${NC}"
echo ""

# Change to project root
cd "$(dirname "$0")/.."

# Build TypeScript
echo -e "${GREEN}Step 1: Building TypeScript...${NC}"
npm run dashboard:build:templates
echo -e "${GREEN}Compiling TypeScript (tsc) - prints nothing until it finishes, about a minute...${NC}"
npx tsc

fargate_stack="$(npm run -s get-stack-name)"
dashboard_stack="$(npm run -s dashboard:get-stack-name)"
if [ "$target" = "dashboard" ] && [ -z "$dashboard_stack" ] ; then
  echo "The context has no DASHBOARD section - there is no dashboard stack to deploy." >&2
  exit 1
fi

# Synthesize once; the template dumps and the deploy below reuse the cloud assembly in cdk.out
echo -e "${GREEN}Step 2: Synthesizing...${NC}"
case "$target" in
  all)       cdk synth -q ;;
  fargate)   cdk synth -q "$fargate_stack" ;;
  dashboard) cdk synth -q "$dashboard_stack" ;;
esac

if [ "$target" != "dashboard" ] ; then
  cdk synth --app cdk.out -e "$fargate_stack" > cdk.out/synth.output.fargate.yaml
  echo "Template: cdk.out/synth.output.fargate.yaml"
fi
if [ "$target" != "fargate" ] && [ -n "$dashboard_stack" ] ; then
  cdk synth --app cdk.out -e "$dashboard_stack" > cdk.out/synth.output.dashboard.yaml
  echo "Template: cdk.out/synth.output.dashboard.yaml"
fi

# A named dashboard stack brings its us-east-1 edge-lambda stack (a dependency) along
echo -e "${GREEN}Step 3: Deploying...${NC}"
case "$target" in
  all)       cdk deploy --app cdk.out --all --no-rollback --require-approval never ;;
  fargate)   cdk deploy --app cdk.out "$fargate_stack" --no-rollback --require-approval never ;;
  dashboard) cdk deploy --app cdk.out "$dashboard_stack" --no-rollback --require-approval never ;;
esac

echo ""
echo -e "${GREEN}=== Deployment Complete! ===${NC}"
echo ""
echo -e "${YELLOW}Stack outputs:${NC}"

echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Build and push Docker image: npm run docker-publish"
echo "2. Trigger processing: Use src/runner/Runner.ts for source API method, or upload a JSON file to the input bucket if using the \"filedrop\" method"
echo "3. Monitor CloudWatch logs: /ecs/huron-person-chunker and /ecs/huron-person-integration"
echo "4. Check SQS queue depth for processing progress"
