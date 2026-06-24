#!/bin/bash
#
# swap_context.sh - Swap between different landscape-specific context configurations
#
# Purpose:
#   This script allows switching the active context.json file between different landscapes
#   (dev, test, prod, etc.) while preserving the current state of each landscape.
#
# How it works:
#   1. Reads TAGS.Landscape from the current context.json to determine the active landscape
#   2. Saves the current context.json to context.<current_landscape>.json (preserving current state)
#   3. Loads context.<target_landscape>.json into context.json (activating target landscape)
#
# Non-destructive:
#   - No context file content is ever lost
#   - Each landscape's state is preserved in its landscape-specific backup file
#   - The active context.json is always saved before switching
#
# Usage:
#   ./swap_context.sh <landscape>
#
# Examples:
#   ./swap_context.sh test   # Switch to test landscape
#   ./swap_context.sh dev    # Switch back to dev landscape
#   ./swap_context.sh prod   # Switch to prod landscape
#
# Prerequisites:
#   - jq must be installed for JSON parsing
#   - Target landscape file (context.<landscape>.json) must exist
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
    echo "Usage: ./swap_context.sh <landscape>"
    echo "Example: ./swap_context.sh test"
    exit 1
fi

TARGET_LANDSCAPE="$1"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONTEXT_FILE="$SCRIPT_DIR/context.json"

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

if [ "$CURRENT_LANDSCAPE" = "$TARGET_LANDSCAPE" ]; then
    echo -e "${YELLOW}Already at landscape: $TARGET_LANDSCAPE${NC}"
    echo "No swap needed."
    exit 0
fi

# 3. Check if target context file exists
TARGET_FILE="$SCRIPT_DIR/context.$TARGET_LANDSCAPE.json"
if [ ! -f "$TARGET_FILE" ]; then
    echo -e "${RED}Error: Target file not found: context.$TARGET_LANDSCAPE.json${NC}"
    echo "Available context files:"
    ls -1 "$SCRIPT_DIR"/context.*.json 2>/dev/null || echo "  (none)"
    exit 1
fi

# 4. Perform the swap
BACKUP_FILE="$SCRIPT_DIR/context.$CURRENT_LANDSCAPE.json"

echo -e "${GREEN}Swapping context from $CURRENT_LANDSCAPE to $TARGET_LANDSCAPE${NC}"
echo ""

# Save current context.json to context.<current_landscape>.json (overwrite is correct behavior)
echo "Saving current context.json → context.$CURRENT_LANDSCAPE.json"
cp "$CONTEXT_FILE" "$BACKUP_FILE"

# Copy target context file to context.json
echo "Loading context.$TARGET_LANDSCAPE.json → context.json"
cp "$TARGET_FILE" "$CONTEXT_FILE"

echo ""
echo -e "${GREEN}✓ Successfully swapped context to: $TARGET_LANDSCAPE${NC}"
echo ""
echo "Files:"
echo "  - context.json (now $TARGET_LANDSCAPE)"
echo "  - context.$CURRENT_LANDSCAPE.json (saved current state)"
echo "  - context.$TARGET_LANDSCAPE.json (preserved)"
