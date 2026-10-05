#!/bin/bash
#
# swap.sh - Swap between different landscape-specific context and .env configurations
#
# Purpose:
#   This script allows switching the active context.json and .env files between different landscapes
#   (dev, test, staging, prod, etc.) while preserving the current state of each landscape.
#
# How it works:
#   1. Reads TAGS.Landscape from the current context.json to determine the active landscape
#   2. Saves the current context.json to context.<current_landscape>.json (preserving current state)
#   3. Saves the current .env to context/<current_landscape>.env (preserving current state)
#   4. Loads context.<target_landscape>.json into context.json (activating target landscape)
#   5. Loads context/<target_landscape>.env into .env (activating target landscape)
#
# Non-destructive:
#   - No config file content is ever lost
#   - Each landscape's state is preserved in its landscape-specific backup files
#   - The active context.json and .env are always saved before switching
#
# Usage:
#   ./swap.sh <landscape>
#
# Examples:
#   ./swap.sh staging   # Switch to staging landscape
#   ./swap.sh dev       # Switch back to dev landscape
#   ./swap.sh prod      # Switch to prod landscape
#
# Prerequisites:
#   - jq must be installed for JSON parsing
#   - Target landscape file (context.<landscape>.json) must exist in context/ directory
#   - Target .env file (<landscape>.env) should exist in context/ directory
#   - context.json must have TAGS.Landscape field
#   - .env file should have LANDSCAPE variable matching the landscape
#
set -e

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# 1. Check for landscape parameter
if [ -z "$1" ]; then
    echo -e "${RED}Error: Landscape parameter required${NC}"
    echo "Usage: ./swap.sh <landscape>"
    echo "Example: ./swap.sh test"
    exit 1
fi

TARGET_LANDSCAPE="$1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CONTEXT_FILE="$SCRIPT_DIR/context.json"
ENV_FILE="$ROOT_DIR/.env"

# Check if context.json exists
if [ ! -f "$CONTEXT_FILE" ]; then
    echo -e "${RED}Error: context.json not found in $SCRIPT_DIR${NC}"
    exit 1
fi

# 2. Check current landscape in context.json
CURRENT_LANDSCAPE=$(jq -r '.TAGS.Landscape' "$CONTEXT_FILE" 2>/dev/null)

if [ $? -ne 0 ] || [ "$CURRENT_LANDSCAPE" = "null" ] || [ -z "$CURRENT_LANDSCAPE" ]; then
    echo -e "${RED}Error: Could not read TAGS.Landscape from context.json${NC}"
    exit 1
fi

# Check if .env file exists and read its LANDSCAPE variable
CURRENT_ENV_LANDSCAPE=""
if [ -f "$ENV_FILE" ]; then
    CURRENT_ENV_LANDSCAPE=$(grep -E "^LANDSCAPE=" "$ENV_FILE" 2>/dev/null | cut -d'=' -f2 | tr -d '"' | tr -d "'")
fi

# Only exit early if BOTH context.json and .env are already at target landscape
if [ "$CURRENT_LANDSCAPE" = "$TARGET_LANDSCAPE" ]; then
    if [ -z "$CURRENT_ENV_LANDSCAPE" ] || [ "$CURRENT_ENV_LANDSCAPE" = "$TARGET_LANDSCAPE" ]; then
        echo -e "${YELLOW}Already at landscape: $TARGET_LANDSCAPE${NC}"
        echo "No swap needed."
        exit 0
    else
        echo -e "${YELLOW}Note: context.json is already at $TARGET_LANDSCAPE, but .env has LANDSCAPE=$CURRENT_ENV_LANDSCAPE${NC}"
        echo "Will swap .env file only."
        echo ""
    fi
fi

# 3. Check if target context file exists
TARGET_CONTEXT_FILE="$SCRIPT_DIR/context.$TARGET_LANDSCAPE.json"
if [ ! -f "$TARGET_CONTEXT_FILE" ]; then
    echo -e "${RED}Error: Target file not found: context.$TARGET_LANDSCAPE.json${NC}"
    echo "Available context files:"
    ls -1 "$SCRIPT_DIR"/context.*.json 2>/dev/null || echo "  (none)"
    exit 1
fi

# Check if target .env file exists (warning only, not fatal)
TARGET_ENV_FILE="$SCRIPT_DIR/$TARGET_LANDSCAPE.env"
if [ ! -f "$TARGET_ENV_FILE" ]; then
    echo -e "${YELLOW}Warning: Target .env file not found: context/$TARGET_LANDSCAPE.env${NC}"
    echo "Only context.json will be swapped. .env file will remain unchanged."
    echo ""
fi

# 4. Perform the swap
BACKUP_CONTEXT_FILE="$SCRIPT_DIR/context.$CURRENT_LANDSCAPE.json"
BACKUP_ENV_FILE="$SCRIPT_DIR/$CURRENT_LANDSCAPE.env"

echo -e "${GREEN}Swapping landscape from $CURRENT_LANDSCAPE to $TARGET_LANDSCAPE${NC}"
echo ""

# Save current context.json to context.<current_landscape>.json (overwrite is correct behavior)
echo "Saving current context.json → context.$CURRENT_LANDSCAPE.json"
cp "$CONTEXT_FILE" "$BACKUP_CONTEXT_FILE"

# Save current .env to context/<current_landscape>.env if it exists
if [ -f "$ENV_FILE" ]; then
    echo "Saving current .env → context/$CURRENT_LANDSCAPE.env"
    cp "$ENV_FILE" "$BACKUP_ENV_FILE"
else
    echo -e "${YELLOW}Note: .env file not found at root, skipping .env backup${NC}"
fi

# Copy target context file to context.json
echo "Loading context.$TARGET_LANDSCAPE.json → context.json"
cp "$TARGET_CONTEXT_FILE" "$CONTEXT_FILE"

# Copy target .env file to .env if it exists
if [ -f "$TARGET_ENV_FILE" ]; then
    echo "Loading context/$TARGET_LANDSCAPE.env → .env"
    cp "$TARGET_ENV_FILE" "$ENV_FILE"
fi

echo ""
echo -e "${GREEN}✓ Successfully swapped landscape to: $TARGET_LANDSCAPE${NC}"
echo ""
echo "Files swapped:"
echo "  - context.json (now $TARGET_LANDSCAPE)"
if [ -f "$ENV_FILE" ]; then
    echo "  - .env (now $TARGET_LANDSCAPE with LANDSCAPE=$TARGET_LANDSCAPE)"
fi
echo ""
echo "Files preserved:"
echo "  - context.$CURRENT_LANDSCAPE.json (saved current state)"
if [ -f "$BACKUP_ENV_FILE" ]; then
    echo "  - context/$CURRENT_LANDSCAPE.env (saved current state)"
fi
echo "  - context.$TARGET_LANDSCAPE.json (preserved)"
if [ -f "$TARGET_ENV_FILE" ]; then
    echo "  - context/$TARGET_LANDSCAPE.env (preserved)"
fi
