#!/bin/bash
#
# Run Huron Person Processor Docker container using Docker Compose
# Usage: ./run.sh [chunker|processor|merger] [--manual]
#
# AWS Credentials:
#   Uses AWS_PROFILE environment variable (defaults to 'default')
#   Ensure your ~/.aws/credentials file contains the profile
#
# Examples:
#   AWS_PROFILE=infnprd INPUT_BUCKET=input-bucket INPUT_KEY=file.json CHUNKS_BUCKET=chunks-bucket ./run.sh chunker
#   AWS_PROFILE=infnprd CHUNKS_BUCKET=chunks-bucket CHUNK_KEY=client/chunks/chunk-0001.ndjson ./run.sh processor
#   AWS_PROFILE=infnprd CHUNKS_BUCKET=chunks-bucket INPUT_BUCKET=input-bucket ./run.sh merger
#   AWS_PROFILE=dev ./run.sh processor --manual
#

set -e

# Load environment variables from .env file
export $(grep -v '^#' .env | xargs)

# Parse arguments
MODE=""
MANUAL_MODE=false

for arg in "$@"; do
  case $arg in
    --manual)
      MANUAL_MODE=true
      shift
      ;;
    chunker|processor|merger)
      MODE="$arg"
      shift
      ;;
    *)
      # Unknown option
      ;;
  esac
done

# Default to chunker if not specified
MODE="${MODE:-chunker}"

if [[ "$MODE" != "chunker" && "$MODE" != "processor" && "$MODE" != "merger" ]]; then
  echo "Error: Invalid mode. Use 'chunker', 'processor', or 'merger'"
  echo "Usage: $0 [chunker|processor|merger] [--manual]"
  exit 1
fi

echo "============================================"
echo "Running Huron Person ${MODE^}"
if $MANUAL_MODE; then
  echo "Mode: Manual (debug)"
else
  echo "Mode: Normal"
fi
echo "============================================"
echo ""

# Navigate to docker directory
cd "$(dirname "$0")"

# Validate required environment variables (only in normal mode)
if ! $MANUAL_MODE; then
  if [[ "$MODE" == "chunker" ]]; then
    # Chunker needs INPUT_BUCKET, INPUT_KEY, and CHUNKS_BUCKET
    if [[ -z "${INPUT_BUCKET}" ]]; then
      echo "Error: INPUT_BUCKET environment variable is required for chunker mode"
      echo "Example: AWS_PROFILE=infnprd INPUT_BUCKET=input-bucket INPUT_KEY=data.json CHUNKS_BUCKET=chunks-bucket $0 chunker"
      exit 1
    fi
    
    if [[ -z "${INPUT_KEY}" ]]; then
      echo "Error: INPUT_KEY environment variable is required for chunker mode"
      echo "Example: AWS_PROFILE=infnprd INPUT_BUCKET=input-bucket INPUT_KEY=data.json CHUNKS_BUCKET=chunks-bucket $0 chunker"
      exit 1
    fi
    
    if [[ -z "${CHUNKS_BUCKET}" ]]; then
      echo "Error: CHUNKS_BUCKET environment variable is required for chunker mode"
      echo "Example: AWS_PROFILE=infnprd INPUT_BUCKET=input-bucket INPUT_KEY=data.json CHUNKS_BUCKET=chunks-bucket $0 chunker"
      exit 1
    fi
    
    echo "Input: s3://${INPUT_BUCKET}/${INPUT_KEY}"
    echo "Chunks output: s3://${CHUNKS_BUCKET}/"
  elif [[ "$MODE" == "processor" ]]; then
    # Processor needs CHUNKS_BUCKET and CHUNK_KEY
    if [[ -z "${CHUNKS_BUCKET}" ]]; then
      echo "Error: CHUNKS_BUCKET environment variable is required for processor mode"
      echo "Example: AWS_PROFILE=infnprd CHUNKS_BUCKET=chunks-bucket CHUNK_KEY=client/chunks/chunk-0001.ndjson $0 processor"
      exit 1
    fi
    
    if [[ -z "${CHUNK_KEY}" ]]; then
      echo "Error: CHUNK_KEY environment variable is required for processor mode"
      echo "Example: AWS_PROFILE=infnprd CHUNKS_BUCKET=chunks-bucket CHUNK_KEY=client/chunks/chunk-0001.ndjson $0 processor"
      exit 1
    fi
    
    echo "Processing chunk: s3://${CHUNKS_BUCKET}/${CHUNK_KEY}"
  elif [[ "$MODE" == "merger" ]]; then
    # Merger needs CHUNKS_BUCKET and INPUT_BUCKET
    if [[ -z "${CHUNKS_BUCKET}" ]]; then
      echo "Error: CHUNKS_BUCKET environment variable is required for merger mode"
      echo "Example: AWS_PROFILE=infnprd CHUNKS_BUCKET=chunks-bucket INPUT_BUCKET=input-bucket $0 merger"
      exit 1
    fi
    
    if [[ -z "${INPUT_BUCKET}" ]]; then
      echo "Error: INPUT_BUCKET environment variable is required for merger mode"
      echo "Example: AWS_PROFILE=infnprd CHUNKS_BUCKET=chunks-bucket INPUT_BUCKET=input-bucket $0 merger"
      exit 1
    fi
    
    echo "Merging chunks: s3://${CHUNKS_BUCKET}/${INPUT_BUCKET}/chunks/ -> s3://${CHUNKS_BUCKET}/${INPUT_BUCKET}/previous-input.ndjson"
  fi
  
  echo "AWS Profile: ${AWS_PROFILE:-default}"
  echo "Region: ${REGION:-us-east-2}"
  echo "Items per chunk: ${ITEMS_PER_CHUNK:-200}"
  echo ""
fi

# Build compose file list
COMPOSE_FILES="-f docker-compose.yml"
if $MANUAL_MODE; then
  COMPOSE_FILES="${COMPOSE_FILES} -f docker-compose.manual.yml"
fi

# Run the container using Docker Compose
if $MANUAL_MODE; then
  echo "Starting container in manual mode..."
  echo "Container will stay running. Use: docker exec -it huron-${MODE} sh"
  echo ""
  # docker-compose ${COMPOSE_FILES} up -d ${MODE}
  echo "docker-compose ${COMPOSE_FILES} up -d ${MODE}"
  
  echo ""
  echo "✓ Container started: huron-${MODE}"
  echo ""
  echo "To shell in:"
  echo "  docker exec -it huron-${MODE} sh"
  echo ""
  echo "Inside container, run manually:"
  echo "  node dist/docker/${MODE}.js"
  echo ""
  echo "To stop:"
  echo "  docker-compose down ${MODE}"
else
  echo "Running container..."
  # docker-compose ${COMPOSE_FILES} up ${MODE}
  echo "docker-compose ${COMPOSE_FILES} up ${MODE}"
  
  echo ""
  echo "✓ ${MODE^} completed"
fi
