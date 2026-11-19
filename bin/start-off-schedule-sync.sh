#!/bin/bash

##
# Start an off-schedule person sync by invoking this landscape's chunker subscriber Lambda
# (chunker-subscriber-<landscape>), outside the regular EventBridge schedule.
#
# The landscape, region and account come from context/context.json (TAGS.Landscape, REGION), the same way
# docker/push.sh reads them, unless --landscape / --region are given.
#
# Usage:
#   ./start-off-schedule-sync.sh --populationType <person-full|person-delta> --bulkReset <true|false>
#                                [--landscape <name>] [--region <region>] [--profile <aws-profile>] [--dry-run]
#   ./start-off-schedule-sync.sh --populationType <person-full|person-delta> --bulkReset <true|false> <aws-profile>
#
# Parameters:
#   --populationType   Required. Type of sync: 'person-full' or 'person-delta'
#   --bulkReset        Required. 'true' ignores stored hashes (a reset); 'false' trusts them (what the schedule runs).
#                      trustPreviousStorage is always sent as the opposite of bulkReset
#                      (src/chunking/ScheduledRun.ts, previousStateFlags()): without it the subscriber treats the run
#                      as a reset.
#   --landscape        Optional. Landscape suffix of the function name (default: TAGS.Landscape in context.json)
#   --region           Optional. AWS region (default: REGION in context.json, else us-east-2)
#   --profile          Optional. AWS CLI profile to use (default: none)
#   --dry-run          Optional. Print the invocation without running it
#   <aws-profile>      Optional. AWS CLI profile as positional argument (alternative to --profile)
#
# Examples:
#   ./start-off-schedule-sync.sh --populationType person-full --bulkReset false
#   ./start-off-schedule-sync.sh --populationType person-delta --bulkReset true --profile bu-integration
#   ./start-off-schedule-sync.sh --populationType person-full --bulkReset false myprofile
##

set -e  # Exit on error

USAGE="Usage: $0 --populationType <person-full|person-delta> --bulkReset <true|false> [--landscape <name>] [--region <region>] [--profile <aws-profile>] [--dry-run]"

# Defaults from context/context.json (via bin/get-context.js), as docker/push.sh does
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
GET_CONTEXT="$SCRIPT_DIR/get-context.js"
FUNCTION_BASE_NAME="chunker-subscriber" # FROM src/chunking/fetch/ChunkerApiSubscriber.ts
LANDSCAPE=""
REGION=""
if [ -f "$GET_CONTEXT" ] && command -v node >/dev/null 2>&1; then
  LANDSCAPE=$(node "$GET_CONTEXT" TAGS.Landscape 2>/dev/null || true)
  REGION=$(node "$GET_CONTEXT" REGION 2>/dev/null || true)
fi

PROFILE=""
POPULATION_TYPE=""
BULK_RESET=""
DRY_RUN="false"
POSITIONAL_ARGS=()

# Parse command-line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --populationType)
      POPULATION_TYPE="$2"
      shift 2
      ;;
    --bulkReset)
      BULK_RESET="$2"
      shift 2
      ;;
    --landscape)
      LANDSCAPE="$2"
      shift 2
      ;;
    --region)
      REGION="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
    --*)
      echo "Unknown parameter: $1"
      echo "$USAGE"
      exit 1
      ;;
    *)
      # Positional argument (likely profile)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done

REGION="${REGION:-us-east-2}"

# If profile wasn't set via --profile flag, check positional arguments
if [[ -z "$PROFILE" ]] && [[ ${#POSITIONAL_ARGS[@]} -gt 0 ]]; then
  PROFILE="${POSITIONAL_ARGS[0]}"
fi

# Validate required parameters
if [[ -z "$POPULATION_TYPE" ]]; then
  echo "Error: --populationType is required"
  echo "$USAGE"
  exit 1
fi

if [[ -z "$BULK_RESET" ]]; then
  echo "Error: --bulkReset is required"
  echo "$USAGE"
  exit 1
fi

if [[ -z "$LANDSCAPE" ]]; then
  echo "Error: no landscape: TAGS.Landscape is missing from context/context.json and --landscape was not given"
  exit 1
fi

# Validate populationType
if [[ "$POPULATION_TYPE" != "person-full" && "$POPULATION_TYPE" != "person-delta" ]]; then
  echo "Error: --populationType must be 'person-full' or 'person-delta'"
  exit 1
fi

# Validate bulkReset, and derive trustPreviousStorage from it (the two flags only act together)
if [[ "$BULK_RESET" == "true" ]]; then
  TRUST_PREVIOUS_STORAGE="false"
elif [[ "$BULK_RESET" == "false" ]]; then
  TRUST_PREVIOUS_STORAGE="true"
else
  echo "Error: --bulkReset must be 'true' or 'false'"
  exit 1
fi

FUNCTION_NAME="${FUNCTION_BASE_NAME}-${LANDSCAPE}"

# Generate ISO 8601 timestamp
TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)

# Build the payload (using configured defaults for baseUrl and fetchPath)
PAYLOAD=$(cat <<JSON
{
  "baseUrl": "from_config",
  "fetchPath": "from_config",
  "populationType": "$POPULATION_TYPE",
  "bulkReset": $BULK_RESET,
  "trustPreviousStorage": $TRUST_PREVIOUS_STORAGE,
  "processingMetadata": {
    "processedAt": "$TIMESTAMP",
    "processorVersion": "1.0.0"
  }
}
JSON
)

# Build AWS CLI arguments. AWS CLI v2 reads --payload as base64 unless told the payload is raw JSON.
AWS_ARGS=(lambda invoke)
if [[ -n "$PROFILE" ]]; then
  AWS_ARGS+=(--profile "$PROFILE")
fi
AWS_ARGS+=(--function-name "$FUNCTION_NAME" --region "$REGION" --cli-binary-format raw-in-base64-out --payload "$PAYLOAD" response.json)

# Display invocation details
echo "=========================================="
echo "Starting Off-Schedule Person Sync"
echo "=========================================="
echo "Population Type:        $POPULATION_TYPE"
echo "Bulk Reset:             $BULK_RESET"
echo "Trust Previous Storage: $TRUST_PREVIOUS_STORAGE"
echo "Landscape:              $LANDSCAPE"
echo "AWS Profile:            ${PROFILE:-<default>}"
echo "Function:               $FUNCTION_NAME"
echo "Region:                 $REGION"
echo "Timestamp:              $TIMESTAMP"
echo "=========================================="
echo ""
echo "Payload: $PAYLOAD"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
  echo "Dry run: would run: aws ${AWS_ARGS[*]}"
  exit 0
fi

# Invoke the Lambda function
echo "Invoking Lambda function..."
if aws "${AWS_ARGS[@]}"; then
  echo ""
  if [[ -f response.json ]]; then
    echo "Lambda Response:"
    cat response.json
    echo ""
  fi
  echo ""
  echo "✅ Lambda invocation completed successfully"
  echo "Check CloudWatch Logs for details: /aws/lambda/$FUNCTION_NAME"
  exit 0
else
  echo ""
  echo "❌ Error: Lambda invocation failed"
  if [[ -f response.json ]]; then
    echo "Response:"
    cat response.json
    echo ""
  fi
  exit 1
fi
