#!/bin/bash

# Destroy CDK stacks
# Usage: ./bin/teardown.sh [all|fargate|dashboard]   (default: all)
#
# The dashboard is torn down in three steps, because CloudFront keeps replicas of the Lambda@Edge functions
# for hours after their distribution is deleted, and Lambda won't delete a function until they are gone:
#   1. Destroy the dashboard stack, which deletes the distribution (nothing can reach the dashboard after this).
#   2. Delete every edge function version, waiting for the replicas to go (can take a few hours).
#      Refuses while the distribution still exists: see dashboard/scripts/DeleteEdgeVersions.ts.
#   3. Destroy the edge-lambda stack.
# Safe to rerun after a failure: destroying a stack that is already gone does nothing.

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

target="${1:-all}"
case "$target" in
  all|fargate|dashboard) ;;
  *) echo "Usage: $0 [all|fargate|dashboard]" >&2; exit 1 ;;
esac

echo -e "${GREEN}=== Destroying CDK Stack(s): $target ===${NC}"
echo ""

# Change to project root
cd "$(dirname "$0")/.."

fargate_stack="$(npm run -s get-stack-name)"
dashboard_stack="$(npm run -s dashboard:get-stack-name)"
if [ "$target" = "dashboard" ] && [ -z "$dashboard_stack" ] ; then
  echo "The context has no DASHBOARD section - there is no dashboard stack to destroy." >&2
  exit 1
fi

# Synthesize once; the steps below reuse the cloud assembly in cdk.out
echo -e "${GREEN}Synthesizing...${NC}"
npm run dashboard:build:templates
cdk synth -q
# Only an authenticated dashboard has an edge-lambda stack
edge_stack="$(cdk list --app cdk.out 2>/dev/null | grep '^edge-lambda-stack-' || true)"

if [ "$target" != "dashboard" ] ; then
  echo -e "${GREEN}Destroying $fargate_stack...${NC}"
  cdk destroy --app cdk.out -f "$fargate_stack"
fi

if [ "$target" != "fargate" ] && [ -n "$dashboard_stack" ] ; then
  echo -e "${GREEN}Step 1: Destroying $dashboard_stack (and its CloudFront distribution)...${NC}"
  cdk destroy --app cdk.out -f "$dashboard_stack"

  if [ -n "$edge_stack" ] ; then
    echo -e "${GREEN}Step 2: Deleting the Lambda@Edge function versions...${NC}"
    if ! npm run -s dashboard:delete-edge-versions ; then
      echo -e "${YELLOW}Deleting the Lambda@Edge function versions failed, so $edge_stack was left in place." \
        "Rerun this teardown later.${NC}" >&2
      exit 1
    fi

    echo -e "${GREEN}Step 3: Destroying $edge_stack...${NC}"
    if ! cdk destroy --app cdk.out -f "$edge_stack" ; then
      echo -e "${YELLOW}$edge_stack could not be destroyed yet (CloudFront may still hold replicas of its functions)." \
        "Rerun this teardown later.${NC}" >&2
      exit 1
    fi
  fi
fi

echo ""
echo -e "${GREEN}=== Teardown Complete! ===${NC}"
