# Example Environment Files: S3 Storage Mode

This document provides complete example `.env` files for running the three-phase pipeline in **S3 storage mode** (default). 

## Storage Mode: S3

In S3 mode, all delta state and metadata is stored in S3:
- Metadata and flags files in S3
- Delta storage (previous-input.ndjson, hashes.ndjson) in S3
- DynamoDB used only for error tracking (statistics table) and chunk ID generation (atomic counter table)

**Key Indicator**: `DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME` and `DYNAMODB_PERSON_HISTORY_TABLE_NAME` are **omitted**.

---

## chunker.env (S3 Mode)

```bash
# ============================================================================
# AWS and Environment Parameters
# ============================================================================
AWS_PROFILE=infnprd                    # AWS profile from ~/.aws/credentials
REGION=us-east-2                       # AWS region
NODE_ENV=production                    # Node environment

# ============================================================================
# Source Configuration
# ============================================================================
# Chunker supports two source modes: 'api' or 's3'

# Source fetch type: 's3' or 'api'
SOURCE_FETCH_TYPE=api

# --- S3 Source Mode (when SOURCE_FETCH_TYPE=s3) ---
# INPUT_BUCKET=huron-person-file-drop-dev
# INPUT_KEY=person-full/2026-03-03T19:58:41.277Z-people.json

# --- API Source Mode (when SOURCE_FETCH_TYPE=api) ---
# Config loaded via ConfigManager chain:
#   1. HURON_PERSON_CONFIG_JSON (from ECS TaskDef secrets in Fargate)
#   2. SECRET_ARN (fallback to Secrets Manager)
#   3. HURON_PERSON_CONFIG_PATH (local dev only)
SECRET_ARN=arn:aws:secretsmanager:us-east-2:770203350335:secret:huron-person-fargate-chunker/integration/_config/dev-xug4Og

# ============================================================================
# Output Configuration
# ============================================================================
CHUNKS_BUCKET=huron-person-chunks-dev              # Destination for chunk NDJSON files
SHARED_DELTA_STORAGE_DIR=delta-storage             # S3 prefix for baseline delta files

# ============================================================================
# Chunking Configuration
# ============================================================================
ITEMS_PER_CHUNK=200                                # Records per chunk file
PERSON_ID_FIELD=personid                           # Person identifier field name

# ============================================================================
# DynamoDB Tables (S3 Mode)
# ============================================================================
# Required for chunk ID generation (used by all storage modes)
DYNAMODB_ATOMIC_COUNTER_TABLE_NAME={STACK_ID}-atomic-counter-{landscape}

# Note: DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME and DYNAMODB_PERSON_HISTORY_TABLE_NAME
# are OMITTED in S3 mode. Their absence signals S3 storage backend.

# ============================================================================
# ECS Infrastructure (for service scaling and task coordination)
# ============================================================================
STACK_ID=huron-person-fargate                      # CDK stack identifier
LANDSCAPE=dev                                      # Environment: dev, staging, prod
ECS_CLUSTER_NAME={STACK_ID}-cluster-{landscape}
ECS_SERVICE_NAME={STACK_ID}-chunker-service-{landscape}
MAX_SCALING_CAPACITY=10                            # Maximum concurrent chunker tasks

# ============================================================================
# SQS Queue (ECS Fargate mode)
# ============================================================================
# SQS_QUEUE_URL=https://sqs.us-east-2.amazonaws.com/770203350335/{STACK_ID}-chunker-queue-{landscape}

# ============================================================================
# Task Protection (for local testing of scale-in protection)
# ============================================================================
# ECS_AGENT_URI is auto-provided in ECS Fargate
# Uncomment for local testing:
# ECS_AGENT_URI=http://169.254.170.2/api

# ============================================================================
# Cache Configuration (for PersonCache testing)
# ============================================================================
CACHE_ENABLED=true                                 # Enable/disable person cache
# CACHE_PATH=/tmp/cache                            # Local cache directory (optional)

# ============================================================================
# Testing Modes (for local development and debugging)
# ============================================================================
# POPULATION_SCOPE=test-population                 # Limit chunking to specific population subset
# POPULATION_TYPE=person-full                      # Override sync population type
# CHUNK_DIRECTORY=chunks/person-full/2026-03-03T19:58:41.277Z  # Explicit chunk directory path
# SINGLE_PERSON_BUID=U12345678                     # Test with single person by BUID

# ============================================================================
# API Retry Strategy (optional - JSON string from ECS context)
# ============================================================================
# RETRY_STRATEGY={"retryStrategyType":"exponential","retryStrategyOptions":{"maxRetries":3,"baseDelay":1000}}

# ============================================================================
# Sync Flags
# ============================================================================
# BULK_RESET=false                                 # Force target system lookups
# TRUST_PREVIOUS_STORAGE=true                      # Skip validation of baseline delta files

# ============================================================================
# Execution Context
# ============================================================================
IS_ECS_TASK=false                                  # Auto-set by ECS, override for local testing
# PAUSE_BEFORE_EARLY_EXIT=true                     # Pause before container exits (for log review)

# ============================================================================
# Dry Run Mode
# ============================================================================
DRY_RUN=true                                       # Set to true to avoid S3 writes (logs only)
```

---

## processor.env (S3 Mode)

```bash
# ============================================================================
# AWS and Environment Parameters
# ============================================================================
AWS_PROFILE=infnprd                    # AWS profile from ~/.aws/credentials
REGION=us-east-2                       # AWS region
NODE_ENV=production                    # Node environment

# ============================================================================
# Data Mapper Configuration
# ============================================================================
STATIC_MAP_USAGE='{ "orgMap": true, "stateMap": true, "countryMap": true }'

# ============================================================================
# Input Configuration
# ============================================================================
INPUT_BUCKET=huron-person-file-drop-dev            # Source bucket (for context)
SHARED_DELTA_STORAGE_DIR=delta-storage             # S3 prefix for baseline delta files

# ============================================================================
# Configuration Loading
# ============================================================================
# Config loaded via ConfigManager chain:
#   1. HURON_PERSON_CONFIG_JSON (from ECS TaskDef secrets in Fargate)
#   2. SECRET_ARN (fallback to Secrets Manager)
#   3. HURON_PERSON_CONFIG_PATH (local dev only)

# Inline config (example - uncomment and populate in production):
# HURON_PERSON_CONFIG_JSON='{"dataSource":{...},"dataTarget":{...},"storage":{"type":"s3","config":{...}}}'

SECRET_ARN=arn:aws:secretsmanager:us-east-2:770203350335:secret:huron-person-fargate-processor/integration/_config/dev-xug4Og

# ============================================================================
# Chunk Details
# ============================================================================
CHUNKS_BUCKET=huron-person-chunks-dev              # Bucket containing chunk files

# Local mode: explicit chunk key
CHUNK_KEY=chunks/person-full/2026-04-26T19:06:54.811Z/chunk-0000.ndjson

# ECS Fargate mode: SQS queue
# SQS_QUEUE_URL=https://sqs.us-east-2.amazonaws.com/770203350335/{STACK_ID}-processor-queue-{landscape}

# ============================================================================
# DynamoDB Tables (S3 Mode)
# ============================================================================
# Required for error tracking (used by all storage modes)
DYNAMODB_STATISTICS_TABLE_NAME={STACK_ID}-statistics-{landscape}

# OMIT these variables to use S3 storage mode (default):
# DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME - Omitted (S3 mode)
# DYNAMODB_PERSON_HISTORY_TABLE_NAME - Omitted (S3 mode)

# ============================================================================
# Task Protection (for local testing of scale-in protection)
# ============================================================================
# ECS_AGENT_URI is auto-provided in ECS Fargate
# Uncomment for local testing:
# ECS_AGENT_URI=http://169.254.170.2/api

# ============================================================================
# Cache Configuration (for PersonCache testing)
# ============================================================================
CACHE_ENABLED=true                                 # Enable/disable person cache
# CACHE_PATH=/tmp/cache                            # Local cache directory (optional)

# ============================================================================
# API Retry Strategy (optional - JSON string from ECS context)
# ============================================================================
# RETRY_STRATEGY={"retryStrategyType":"exponential","retryStrategyOptions":{"maxRetries":3,"baseDelay":1000}}

# ============================================================================
# Execution Context
# ============================================================================
IS_ECS_TASK=false                                  # Auto-set by ECS, override for local testing

# ============================================================================
# Dry Run Mode
# ============================================================================
DRY_RUN=false                                      # Set to true to avoid target API writes
```

---

## merger.env (S3 Mode)

```bash
# ============================================================================
# AWS and Environment Parameters
# ============================================================================
AWS_PROFILE=infnprd                    # AWS profile from ~/.aws/credentials
REGION=us-east-2                       # AWS region
NODE_ENV=production                    # Node environment

# ============================================================================
# Configuration Loading
# ============================================================================
# Config loaded via ConfigManager chain:
#   1. HURON_PERSON_CONFIG_JSON (from ECS TaskDef secrets in Fargate)
#   2. SECRET_ARN (fallback to Secrets Manager)
#   3. HURON_PERSON_CONFIG_PATH (local dev only)

# Inline config (example - uncomment and populate in production):
# HURON_PERSON_CONFIG_JSON='{"dataSource":{...},"dataTarget":{...},"storage":{"type":"s3","config":{...}}}'

SECRET_ARN=arn:aws:secretsmanager:us-east-2:770203350335:secret:huron-person-fargate-processor/integration/_config/dev-xug4Og

# ============================================================================
# Input Configuration
# ============================================================================
INPUT_BUCKET=huron-person-file-drop-dev            # Original input bucket (for context)
CHUNKS_BUCKET=huron-person-chunks-dev              # Bucket containing chunk files

# ============================================================================
# Shared Delta Storage
# ============================================================================
SHARED_DELTA_STORAGE_DIR=delta-storage             # S3 prefix for baseline delta files

# ============================================================================
# DynamoDB Tables (S3 Mode)
# ============================================================================
# Required for statistics tracking (used by all storage modes)
DYNAMODB_STATISTICS_TABLE_NAME={STACK_ID}-statistics-{landscape}

# Note: Person state tables are NOT used by merger in any mode

# ============================================================================
# Deletion Handling
# ============================================================================
PERSON_DELETE_TYPE=deferred                        # Deletion strategy: immediate, deferred, or disabled

# ============================================================================
# Task Protection (for local testing of scale-in protection)
# ============================================================================
# ECS_AGENT_URI is auto-provided in ECS Fargate
# Uncomment for local testing:
# ECS_AGENT_URI=http://169.254.170.2/api

# ============================================================================
# Cache Configuration (for PersonCache testing)
# ============================================================================
CACHE_ENABLED=true                                 # Enable/disable person cache
# CACHE_PATH=/tmp/cache                            # Local cache directory (optional)

# ============================================================================
# Execution Context
# ============================================================================
IS_ECS_TASK=false                                  # Auto-set by ECS, override for local testing

# ============================================================================
# SQS Queue (ECS Fargate mode)
# ============================================================================
# SQS_QUEUE_URL=https://sqs.us-east-2.amazonaws.com/770203350335/{STACK_ID}-merger-queue-{landscape}

# ============================================================================
# Local Mode (explicit chunk directory)
# ============================================================================
# CHUNK_DIRECTORY=chunks/person-full/2026-03-03T19:58:41.277Z

# ============================================================================
# Dry Run Mode
# ============================================================================
DRY_RUN=true                                       # Set to true to avoid S3 writes and deletes
```

---

## Placeholder Patterns

Replace the following placeholders with actual values:

- `{STACK_ID}` → `huron-person-fargate` (or your CDK stack identifier)
- `{landscape}` → `dev`, `staging`, or `prod`
- Account IDs in ARNs → Your AWS account ID (e.g., `770203350335`)
- Bucket names → Your actual S3 bucket names
- Secret ARNs → Your actual Secrets Manager ARNs

## Verification

To verify S3 mode is active:

1. **Check environment variables:**
   - `DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME` is **not set**
   - `DYNAMODB_PERSON_HISTORY_TABLE_NAME` is **not set**
   - `DYNAMODB_STATISTICS_TABLE_NAME` **is set** (required for both modes)

2. **Check processor logs:**
   ```
   Using S3 metadata storage (default)
   ```

3. **Check S3 for state files:**
   ```bash
   aws s3 ls s3://huron-person-chunks-dev/delta-storage/person-full/
   # Should see: previous-input.ndjson, hashes.ndjson
   ```

4. **No DynamoDB person state tables should exist** (or should be empty if they exist)
