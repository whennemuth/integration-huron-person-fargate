#!/bin/bash

##
# Start an off-schedule person sync by invoking the ChunkerSubscriber Lambda function.
# This script triggers the chunking process manually outside of the regular EventBridge schedule.
#
# Usage:
#   ./start-off-schedule-sync.sh --populationType <person-full|person-delta> --bulkReset <true|false> [--profile <aws-profile>]
#   ./start-off-schedule-sync.sh --populationType <person-full|person-delta> --bulkReset <true|false> <aws-profile>
#
# Parameters:
#   --populationType   Required. Type of sync: 'person-full' or 'person-delta'
#   --bulkReset        Required. Reset mode: 'true' or 'false'
#   --profile          Optional. AWS CLI profile to use (default: none)
#   <aws-profile>      Optional. AWS CLI profile as positional argument (alternative to --profile)
#
# Examples:
#   ./start-off-schedule-sync.sh --populationType person-full --bulkReset false
#   ./start-off-schedule-sync.sh --populationType person-delta --bulkReset true --profile bu-integration
#   ./start-off-schedule-sync.sh --populationType person-full --bulkReset false myprofile
##

set -e  # Exit on error

# Default values
FUNCTION_NAME="chunker-subscriber"
REGION="us-east-2"
PROFILE=""
POPULATION_TYPE=""
BULK_RESET=""
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
    --*)
      echo "Unknown parameter: $1"
      echo "Usage: $0 --populationType <person-full|person-delta> --bulkReset <true|false> [--profile <aws-profile>]"
      exit 1
      ;;
    *)
      # Positional argument (likely profile)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done

# If profile wasn't set via --profile flag, check positional arguments
if [[ -z "$PROFILE" ]] && [[ ${#POSITIONAL_ARGS[@]} -gt 0 ]]; then
  PROFILE="${POSITIONAL_ARGS[0]}"
fi

# Validate required parameters
if [[ -z "$POPULATION_TYPE" ]]; then
  echo "Error: --populationType is required"
  echo "Usage: $0 --populationType <person-full|person-delta> --bulkReset <true|false> [--profile <aws-profile>]"
  exit 1
fi

if [[ -z "$BULK_RESET" ]]; then
  echo "Error: --bulkReset is required"
  echo "Usage: $0 --populationType <person-full|person-delta> --bulkReset <true|false> [--profile <aws-profile>]"
  exit 1
fi

# Validate populationType
if [[ "$POPULATION_TYPE" != "person-full" && "$POPULATION_TYPE" != "person-delta" ]]; then
  echo "Error: --populationType must be 'person-full' or 'person-delta'"
  exit 1
fi

# Validate bulkReset
if [[ "$BULK_RESET" != "true" && "$BULK_RESET" != "false" ]]; then
  echo "Error: --bulkReset must be 'true' or 'false'"
  exit 1
fi

# Generate ISO 8601 timestamp
TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ)

# Build the payload (using configured defaults for baseUrl and fetchPath)
PAYLOAD=$(cat <<EOF
{
  "baseUrl": "from_config",
  "fetchPath": "from_config",
  "populationType": "$POPULATION_TYPE",
  "bulkReset": $BULK_RESET,
  "processingMetadata": {
    "processedAt": "$TIMESTAMP",
    "processorVersion": "1.0.0"
  }
}
EOF
)

# Build AWS CLI command
AWS_CMD="aws lambda invoke"

# Add profile if specified
if [[ -n "$PROFILE" ]]; then
  AWS_CMD="$AWS_CMD --profile $PROFILE"
fi

# Add function name, region, and payload
AWS_CMD="$AWS_CMD --function-name $FUNCTION_NAME --region $REGION --payload '$PAYLOAD' response.json"

# Display invocation details
echo "=========================================="
echo "Starting Off-Schedule Person Sync"
echo "=========================================="
echo "Population Type: $POPULATION_TYPE"
echo "Bulk Reset:      $BULK_RESET"
echo "AWS Profile:     ${PROFILE:-<default>}"
echo "Function:        $FUNCTION_NAME"
echo "Region:          $REGION"
echo "Timestamp:       $TIMESTAMP"
echo "=========================================="
echo ""
echo "Command: $AWS_CMD"
echo ""

# Invoke the Lambda function
echo "Invoking Lambda function..."
eval $AWS_CMD

# Check result
if [[ $? -eq 0 ]]; then
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
