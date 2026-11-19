#!/bin/bash
#
# Run one phase of the Huron Person pipeline locally in its container, with Docker Compose.
# Usage: ./run.sh [chunker|processor|post-processor] [--manual]
#   (from the repo root: npm run chunker | processor | post-processor, or the *-debug scripts for --manual)
#
# The container takes its variables only from the phase's env file in this directory (chunker.env,
# processor.env, post-processor.env; templates in example-env-s3.md / example-env-dynamodb.md), not from
# your shell. Set AWS_PROFILE there: compose mounts ~/.aws read-only at the image user's home
# (/home/nodejs/.aws).
#
# Normal mode runs the phase's entry point and exits with its exit code. --manual starts the container
# without running node (docker-compose.manual.yml), so you can shell in and run it yourself.
# Only the named phase starts (--no-deps), not the phases its compose service depends on.
#

set -e

# Work from this directory, where the compose and env files are
cd "$(dirname "$0")"

# Parse arguments
MODE=""
MANUAL_MODE=false

for arg in "$@"; do
  case $arg in
    --manual)
      MANUAL_MODE=true
      ;;
    chunker|processor|post-processor)
      MODE="$arg"
      ;;
    *)
      echo "Error: unknown argument: $arg"
      echo "Usage: $0 [chunker|processor|post-processor] [--manual]"
      exit 1
      ;;
  esac
done

# Default to chunker if not specified
MODE="${MODE:-chunker}"
ENV_FILE="${MODE}.env"

# docker compose (v2 plugin), or the standalone docker-compose
if docker compose version >/dev/null 2>&1; then
  COMPOSE="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE="docker-compose"
else
  echo "Error: neither 'docker compose' nor 'docker-compose' is available"
  exit 1
fi

# The container's variables come only from the env file: check it exists and names a storage mode and a profile.
# The entry point reports any other missing variable itself.
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Error: docker/${ENV_FILE} not found (templates: docker/example-env-s3.md, docker/example-env-dynamodb.md)"
  exit 1
fi

env_value() {
  # The last uncommented NAME=value in the env file (empty if absent)
  grep -E "^[[:space:]]*$1=" "$ENV_FILE" | tail -n 1 | cut -d= -f2- | tr -d '\r' || true
}

STORAGE_TYPE="$(env_value PREVIOUS_STORAGE_TYPE)"
PROFILE="$(env_value AWS_PROFILE)"
if [[ -z "$STORAGE_TYPE" ]]; then
  echo "Error: PREVIOUS_STORAGE_TYPE (s3 or dynamodb) is not set in docker/${ENV_FILE}"
  exit 1
fi
if [[ -z "$PROFILE" ]]; then
  echo "Warning: AWS_PROFILE is not set in docker/${ENV_FILE}; the SDK will use the default profile"
fi

echo "============================================"
echo "Running Huron Person ${MODE^}"
if $MANUAL_MODE; then
  echo "Mode: Manual (debug)"
else
  echo "Mode: Normal"
fi
echo "Env file: docker/${ENV_FILE}"
echo "Storage type: ${STORAGE_TYPE}"
echo "AWS Profile: ${PROFILE:-default}"
echo "============================================"
echo ""

# Build compose file list
COMPOSE_FILES="-f docker-compose.yml"
if $MANUAL_MODE; then
  COMPOSE_FILES="${COMPOSE_FILES} -f docker-compose.manual.yml"
fi

# Run the container using Docker Compose
if $MANUAL_MODE; then
  echo "Starting container in manual mode..."
  $COMPOSE ${COMPOSE_FILES} up -d --no-deps ${MODE}

  echo ""
  echo "✓ Container started: huron-${MODE}"
  echo ""
  echo "To shell in:"
  echo "  docker exec -it huron-${MODE} sh"
  echo ""
  echo "Inside container, run manually:"
  echo "  node dist/docker/${MODE}.js"
  echo ""
  echo "To stop (from docker/):"
  echo "  ${COMPOSE} ${COMPOSE_FILES} down"
else
  echo "Running container..."
  $COMPOSE ${COMPOSE_FILES} up --no-deps --exit-code-from ${MODE} ${MODE}

  echo ""
  echo "✓ ${MODE^} completed"
fi
