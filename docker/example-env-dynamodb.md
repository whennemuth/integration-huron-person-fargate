# Example Environment Files: DynamoDB Storage Mode

This document provides complete example `.env` files for running the three-phase pipeline in **DynamoDB storage mode**.

## Storage Mode: DynamoDB

In DynamoDB mode, person state and history are stored in DynamoDB tables:
- **PersonCurrentStateTable** - Current hash state per person (PK: personId)
- **PersonHistoryTable** - Historical change audit trail (PK: personId, SK: syncRunId)
- **StatisticsTable** - Metadata, flags, error events
- Chunk NDJSON files remain in S3 (required for parallel processing)

**Key Indicator**: `DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME` and `DYNAMODB_PERSON_HISTORY_TABLE_NAME` are **set**.

---

## chunker.env (DynamoDB Mode)

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
# DynamoDB Tables (DynamoDB Mode)
# ============================================================================
# Required for chunk ID generation (used by all storage modes)
DYNAMODB_ATOMIC_COUNTER_TABLE_NAME={STACK_ID}-atomic-counter-{landscape}

# Note: Chunker does NOT use person state tables in any mode.
# PersonCurrentStateTable and PersonHistoryTable are only used by processor.

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

**Note:** chunker.env is identical in both S3 and DynamoDB modes. Chunker does not use person state tables.

---

## processor.env (DynamoDB Mode)

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
# HURON_PERSON_CONFIG_JSON='{"dataSource":{...},"dataTarget":{...},"storage":{"type":"dynamodb","config":{...}}}'

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
# DynamoDB Tables (DynamoDB Mode)
# ============================================================================
# Required for error tracking and metadata (used by all storage modes)
DYNAMODB_STATISTICS_TABLE_NAME={STACK_ID}-statistics-{landscape}

# ** DynamoDB Mode: INCLUDE these variables **
# These tables signal DynamoDB storage mode to ProcessorMetadataFactory
DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME={STACK_ID}-person-current-state-{landscape}
DYNAMODB_PERSON_HISTORY_TABLE_NAME={STACK_ID}-person-history-{landscape}

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

**Key Difference from S3 Mode:**
- ✅ **DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME** is **set** (enables DynamoDB mode)
- ✅ **DYNAMODB_PERSON_HISTORY_TABLE_NAME** is **set** (enables DynamoDB mode)

---

## merger.env (DynamoDB Mode)

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
# HURON_PERSON_CONFIG_JSON='{"dataSource":{...},"dataTarget":{...},"storage":{"type":"dynamodb","config":{...}}}'

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
# DynamoDB Tables (DynamoDB Mode)
# ============================================================================
# Required for statistics tracking and metadata (used by all storage modes)
DYNAMODB_STATISTICS_TABLE_NAME={STACK_ID}-statistics-{landscape}

# Note: Merger does NOT use person state tables in any mode.
# Merger consolidates results but doesn't track individual person changes.

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

**Note:** merger.env is identical in both S3 and DynamoDB modes. Merger does not use person state tables.

---

## Key Differences: DynamoDB Mode vs S3 Mode

### processor.env ONLY

**S3 Mode (default):**
```bash
DYNAMODB_STATISTICS_TABLE_NAME={STACK_ID}-statistics-{landscape}
# OMIT these:
# DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME
# DYNAMODB_PERSON_HISTORY_TABLE_NAME
```

**DynamoDB Mode:**
```bash
DYNAMODB_STATISTICS_TABLE_NAME={STACK_ID}-statistics-{landscape}
DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME={STACK_ID}-person-current-state-{landscape}
DYNAMODB_PERSON_HISTORY_TABLE_NAME={STACK_ID}-person-history-{landscape}
```

### chunker.env and merger.env

**Identical in both modes.** Neither chunker nor merger use person state tables.

## Placeholder Patterns

Replace the following placeholders with actual values:

- `{STACK_ID}` → `huron-person-fargate` (or your CDK stack identifier)
- `{landscape}` → `dev`, `staging`, or `prod`
- Account IDs in ARNs → Your AWS account ID (e.g., `770203350335`)
- Bucket names → Your actual S3 bucket names
- Secret ARNs → Your actual Secrets Manager ARNs

## Table Naming Convention

DynamoDB tables follow this pattern:
```
{STACK_ID}-{table-type}-{landscape}
```

Examples:
- `huron-person-fargate-statistics-dev`
- `huron-person-fargate-atomic-counter-dev`
- `huron-person-fargate-person-current-state-dev`
- `huron-person-fargate-person-history-dev`

## Verification

To verify DynamoDB mode is active:

1. **Check environment variables in processor:**
   - `DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME` is **set**
   - `DYNAMODB_PERSON_HISTORY_TABLE_NAME` is **set**
   - `DYNAMODB_STATISTICS_TABLE_NAME` is **set**

2. **Check processor logs:**
   ```
   Using DynamoDB metadata storage (DynamoDB-mode-specific tables detected)
   ```

3. **Check DynamoDB for person state:**
   ```bash
   aws dynamodb scan --table-name huron-person-fargate-person-current-state-dev --limit 10
   # Should see person records with personId, hash, syncRunId
   ```

4. **Check DynamoDB for change history:**
   ```bash
   aws dynamodb query \
     --table-name huron-person-fargate-person-history-dev \
     --key-condition-expression "personId = :pid" \
     --expression-attribute-values '{":pid":{"S":"U12345678"}}'
   # Should see historical changes for that person
   ```

5. **Chunk NDJSON files still exist in S3** (required for parallel processing):
   ```bash
   aws s3 ls s3://huron-person-chunks-dev/chunks/person-full/2026-04-26T19:06:54.811Z/
   # Should see: chunk-0000.ndjson, chunk-0001.ndjson, etc.
   ```

## IAM Permissions Required

In DynamoDB mode, ECS task roles need additional permissions:

```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:GetItem",
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:Query",
    "dynamodb:Scan"
  ],
  "Resource": [
    "arn:aws:dynamodb:us-east-2:770203350335:table/huron-person-fargate-person-current-state-dev",
    "arn:aws:dynamodb:us-east-2:770203350335:table/huron-person-fargate-person-current-state-dev/index/*",
    "arn:aws:dynamodb:us-east-2:770203350335:table/huron-person-fargate-person-history-dev"
  ]
}
```

These permissions are automatically granted by CDK when `context.PREVIOUS_STORAGE_TYPE === 'dynamodb'` or when `context.PREVIOUS_STORAGE_TYPE` is undefined (defaults to 'dynamodb').
