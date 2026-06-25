# Example Environment Variables

Reference configuration for test harnesses in `integration-huron-person-fargate`. Copy these values to your `.env` file and customize as needed for your environment.

**Note**: This file contains sanitized example values. Replace placeholder values (e.g., `<your-api-key>`, `<SECRET_ARN>`) with actual credentials appropriate for your environment.

```env
# Basic launch configuration settings for CDK and SDK operations.
AWS_PROFILE=infnprd
REGION=us-east-2
LANDSCAPE=dev
DEBUG=true

# Test mode (filesystem or s3)
MODE=filesystem

# Number of person records per chunk (default: 200)
ITEMS_PER_CHUNK=200

# Base path for filesystem storage
FILE_BASE_PATH=./test-data
FILE_NAME=20260303111609986.json

# S3 buckets and keys for testing
INPUT_BUCKET=huron-person-file-drop-dev
INPUT_KEY=person-full/2026-03-03T19:58:41.277Z-people.json
CHUNKS_BUCKET=huron-person-chunks-dev
CHUNK_KEY=chunks/person-full/2026-04-09T15:28:18.703Z/chunk-0000.ndjson

ECR_REGISTRY_ID=<account-id>
ECR_REPOSITORY_NAME=huron-person-integration
DRY_RUN=false

# Huron Person Integration Environment Variables
HURON_PERSON_CONFIG_PATH=../integration-huron-person/config.json

# Person DataSource Configuration (API Key Authentication)
DATASOURCE_ENDPOINTCONFIG_PERSON_BASE_URL=https://prod-buprod-cloudultra-fm.snaplogic.io
DATASOURCE_ENDPOINTCONFIG_PERSON_API_KEY=<your-person-api-key>
DATASOURCE_ENDPOINTCONFIG_PERSON_PATH=/api/1/rest/feed-master/queue/BUProd/Admin-Integration-Services/CommonServiceWrappers/huronIRBPerson

# People DataSource Configuration (API Key Authentication)
DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL=https://prod-buprod-fm.snaplogic.io
DATASOURCE_ENDPOINTCONFIG_PEOPLE_API_KEY=<your-people-api-key>
DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH=/api/1/rest/feed/run/task/BUProd/Admin-Integration-Services/GenericGets/huronIRBgetPersonByPopulation
DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT=10

# Current Terms DataSource Configuration (API Key Authentication)
DATASOURCE_ENDPOINTCONFIG_CURRENT_TERMS_BASE_URL=https://prod-buprod-fm.snaplogic.io
DATASOURCE_ENDPOINTCONFIG_CURRENT_TERMS_API_KEY=<your-current-terms-api-key>
DATASOURCE_ENDPOINTCONFIG_CURRENT_TERMS_PATH=/api/1/rest/feed/run/task/BUProd/Admin-Integration-Services/GenericGets/getCurrentTermFromCS

# DataTarget Configuration (JWT Authentication)
DATATARGET_ENDPOINTCONFIG_BASE_URL=https://bu.hrs-staging.com
DATATARGET_ENDPOINTCONFIG_USERNAME=<target-system-username>
DATATARGET_ENDPOINTCONFIG_PASSWORD=<target-system-password>
DATATARGET_ENDPOINTCONFIG_LOGIN_SVC_PATH=/loginsvc/api/v1/token/
DATATARGET_ENDPOINTCONFIG_LOGIN_USERID=<user-email@bu.edu>
DATATARGET_ENDPOINTCONFIG_EXTERNAL_TOKEN=<external-jwt-token>

# Integration Configuration
CLIENT_ID=huron-person-integration-dev
SECRET_ARN=<arn:aws:secretsmanager:region:account:secret:path-xxxxx>
CHUNKER_QUEUE_URL=https://sqs.us-east-2.amazonaws.com/<account-id>/huron-person-chunker-queue
POPULATION_TYPE=person-full
BULK_RESET=false

# Cache Configuration
CACHE_ENABLED=true
CACHE_PATH=.

# --------- Use these for src\Runner.ts ---------- #
RUNNER_CHUNKER_QUEUE_URL=https://sqs.us-east-2.amazonaws.com/770203350335/huron-person-chunker-queue-dev
RUNNER_DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT=10
RUNNER_POPULATION_TYPE=person-full
RUNNER_POPULATION_SCOPE=standard
RUNNER_BULK_RESET=false
RUNNER_TRUST_PREVIOUS_STORAGE=true
RUNNER_SECRET_ARN=arn:aws:secretsmanager:us-east-2:770203350335:secret:huron-person-fargate-processor/integration/_config/dev-xug4Og
RUNNER_STACK_ID=huron-person-fargate-processor
RUNNER_REGION=us-east-2
# For seeding the queue.
RUNNER_MESSAGES_TO_PREPOPULATE=15
RUNNER_ECS_CLUSTER_NAME=huron-person-cluster-dev
RUNNER_ECS_SERVICE_NAME=Chunker
RUNNER_DESIRED_COUNT=12
RUNNER_STACK_ID=huron-person-fargate-processor

# ------- Harness Groups for src\chunking\fetch\ChunkFromAPI.ts ------- #
CHUNK_FROM_API_SINGLE_PERSON_BUID=U15364542
CHUNK_FROM_API_CHUNKER_QUEUE_URL=https://sqs.us-east-2.amazonaws.com/<account-id>/huron-person-chunker-queue
CHUNK_FROM_API_POPULATION_TYPE=person-full
CHUNK_FROM_API_POPULATION_SCOPE=single
CHUNK_FROM_API_BULK_RESET=false
CHUNK_FROM_API_TRUST_PREVIOUS_STORAGE=false
CHUNK_FROM_API_PERSON_ID_FIELD=personid
CHUNK_FROM_API_CHUNKS_BUCKET=huron-person-chunks-dev
CHUNK_FROM_API_SECRET_ARN=<arn:aws:secretsmanager:region:account:secret:path-xxxxx>
CHUNK_FROM_API_REGION=us-east-2

# ------- Harness Groups for src\chunking\fetch\QueueSeeder.ts ------- #
QUEUE_SEEDER_BASE_URL=https://prod-buprod-fm.snaplogic.io
QUEUE_SEEDER_FETCH_PATH=/api/1/rest/feed/run/task/BUProd/Admin-Integration-Services/GenericGets/huronIRBgetPersonByPopulation
QUEUE_SEEDER_POPULATION_TYPE=person-full
QUEUE_SEEDER_LIMIT=100
QUEUE_SEEDER_MESSAGES_TO_SEED=15
QUEUE_SEEDER_QUEUE_URL=https://sqs.us-east-2.amazonaws.com/<account-id>/huron-person-chunker-queue
QUEUE_SEEDER_REGION=us-east-2
QUEUE_SEEDER_BULK_RESET=false
QUEUE_SEEDER_TRUST_PREVIOUS_STORAGE=false
QUEUE_SEEDER_DRY_RUN=true

# ------- Harness Groups for src\merging\DeferredDeleteHandler.ts ------- #
MERGED_NDJSON_KEY=previous-input-testing.ndjson
BASELINE_NDJSON_KEY=delta-storage/previous-input.ndjson
PERSON_DELETE_TYPE=soft

# ------- Harness Groups for src\PersonCache.ts ------- #
PERSON_CACHE_BUCKET_NAME=huron-person-chunks-dev
PERSON_CACHE_KEY=personCache.txt

# ------- Harness Groups for src\statistics\StatisticsTable.ts ------- #
STATISTICS_TABLE_TASK=chunks
STATISTICS_TABLE_INTEGRATION_TIMESTAMP=2026-05-13T17:23:38.978Z

# ------- Harness Groups for src\storage\S3StorageAdapter.ts ------- #
S3_STORAGE_ADAPTER_TEST_TASK=write-file
S3_STORAGE_ADAPTER_TEST_BUCKET_NAME=huron-person-chunks-dev
S3_STORAGE_ADAPTER_TEST_KEY=chunks/person-full/2026-05-14T20:04:31.425Z/write_test.txt
S3_STORAGE_ADAPTER_TEST_REGION=us-east-2

# ------- Harness Groups for docker/chunker.ts ------- #
DOCKER_CHUNKER_CHUNKS_BUCKET=huron-person-chunks-dev
DOCKER_CHUNKER_STACK_ID=huron-person-fargate-processor
DOCKER_CHUNKER_REGION=us-east-2
DOCKER_CHUNKER_ITEMS_PER_CHUNK=200
DOCKER_CHUNKER_PERSON_ID_FIELD=personid
DOCKER_CHUNKER_DRY_RUN=false
DOCKER_CHUNKER_SQS_QUEUE_URL=https://sqs.us-east-2.amazonaws.com/770203350335/huron-person-chunker-queue-dev
DOCKER_CHUNKER_SHARED_DELTA_STORAGE_DIR=delta-storage
DOCKER_CHUNKER_IS_ECS_TASK=false
DOCKER_CHUNKER_ECS_AGENT_URI=http://169.254.170.2
DOCKER_CHUNKER_HURON_PERSON_CONFIG_PATH=../integration-huron-person/config.json
DOCKER_CHUNKER_SECRET_ARN=arn:aws:secretsmanager:us-east-2:770203350335:secret:huron-person-fargate-processor/integration/_config/dev-xug4Og
DOCKER_CHUNKER_HURON_PERSON_CONFIG_JSON=
DOCKER_CHUNKER_POPULATION_SCOPE=single
DOCKER_CHUNKER_POPULATION_TYPE=person-full
DOCKER_CHUNKER_CHUNK_DIRECTORY=
DOCKER_CHUNKER_SINGLE_PERSON_BUID=U15364542
DOCKER_CHUNKER_INPUT_BUCKET=huron-person-file-drop-dev
DOCKER_CHUNKER_INPUT_KEY=person-full/2026-03-03T19:58:41.277Z-people.json
DOCKER_CHUNKER_BULK_RESET=false
DOCKER_CHUNKER_TRUST_PREVIOUS_STORAGE=false
DOCKER_CHUNKER_MAX_SCALING_CAPACITY=1
DOCKER_CHUNKER_ECS_CLUSTER_NAME=huron-person-fargate-cluster-dev
DOCKER_CHUNKER_CACHE_ENABLED=true
DOCKER_CHUNKER_CACHE_PATH=.

# ------- Harness Groups for docker/processor.ts ------- #
DOCKER_PROCESSOR_REGION=us-east-2
DOCKER_PROCESSOR_CHUNKS_BUCKET=huron-person-chunks-dev
DOCKER_PROCESSOR_CHUNK_KEY=chunks/person-full/2026-04-09T15:28:18.703Z/chunk-0000.ndjson
DOCKER_PROCESSOR_SQS_QUEUE_URL=
DOCKER_PROCESSOR_HURON_PERSON_CONFIG_JSON=
DOCKER_PROCESSOR_STATIC_MAP_USAGE={"orgMap":true,"stateMap":true,"countryMap":true}
DOCKER_PROCESSOR_DRY_RUN=false
DOCKER_PROCESSOR_BULK_RESET=false
DOCKER_PROCESSOR_DYNAMODB_STATISTICS_TABLE_NAME=huron-person-fargate-statistics
DOCKER_PROCESSOR_RETRY_STRATEGY=retry-throttle-and-5xx
DOCKER_PROCESSOR_SHARED_DELTA_STORAGE_DIR=delta-storage
DOCKER_PROCESSOR_IS_ECS_TASK=false
DOCKER_PROCESSOR_ECS_AGENT_URI=http://169.254.170.2
DOCKER_PROCESSOR_HURON_PERSON_CONFIG_PATH=../integration-huron-person/config.json
DOCKER_PROCESSOR_SECRET_ARN=arn:aws:secretsmanager:us-east-2:770203350335:secret:huron-person-fargate-processor/integration/_config/dev-xug4Og
DOCKER_PROCESSOR_CACHE_ENABLED=true
DOCKER_PROCESSOR_CACHE_PATH=.

# ------- Harness Groups for docker/merger.ts ------- #
DOCKER_MERGER_SQS_QUEUE_URL=
DOCKER_MERGER_CHUNKS_BUCKET=huron-person-chunks-dev
DOCKER_MERGER_CHUNK_DIRECTORY=chunks/person-full/2026-05-26T15:04:22.699Z
DOCKER_MERGER_INPUT_KEY=person-full/2026-03-03T19:58:41.277Z-people.json
DOCKER_MERGER_REGION=us-east-2
DOCKER_MERGER_SHARED_DELTA_STORAGE_DIR=delta-storage
DOCKER_MERGER_DRY_RUN=false
DOCKER_MERGER_IS_ECS_TASK=false
DOCKER_MERGER_ECS_AGENT_URI=http://169.254.170.2
DOCKER_MERGER_PERSON_DELETE_TYPE=soft
DOCKER_MERGER_HURON_PERSON_CONFIG_PATH=../integration-huron-person/config.json
DOCKER_MERGER_SECRET_ARN=arn:aws:secretsmanager:us-east-2:770203350335:secret:huron-person-fargate-processor/integration/_config/dev-xug4Og
DOCKER_MERGER_HURON_PERSON_CONFIG_JSON=
DOCKER_MERGER_DYNAMODB_STATISTICS_TABLE_NAME=huron-person-fargate-statistics
DOCKER_MERGER_CACHE_ENABLED=true
DOCKER_MERGER_CACHE_PATH=.

# ------- Harness Groups for src\chunking\filedrop\ChunkFromS3.ts ------- #
CHUNK_FROM_S3_CHUNKS_BUCKET=huron-person-chunks-dev
CHUNK_FROM_S3_REGION=us-east-2
CHUNK_FROM_S3_ITEMS_PER_CHUNK=200
CHUNK_FROM_S3_PERSON_ID_FIELD=personid
CHUNK_FROM_S3_DRY_RUN=false
CHUNK_FROM_S3_INPUT_BUCKET=huron-person-file-drop-dev
CHUNK_FROM_S3_INPUT_KEY=person-full/2026-03-03T19:58:41.277Z-people.json
CHUNK_FROM_S3_BULK_RESET=false
CHUNK_FROM_S3_TRUST_PREVIOUS_STORAGE=false
CHUNK_FROM_S3_SQS_QUEUE_URL=https://sqs.us-east-2.amazonaws.com/<account-id>/huron-person-chunker-queue

# ------- Harness Groups for src\chunking\fetch\BigJsonFetch.ts ------- #
BIG_JSON_FETCH_MODE=filesystem
BIG_JSON_FETCH_ITEMS_PER_CHUNK=200
BIG_JSON_FETCH_DRY_RUN=false
BIG_JSON_FETCH_SECRET_ARN=<arn:aws:secretsmanager:region:account:secret:path-xxxxx>
BIG_JSON_FETCH_SYNC_TYPE=person-full
BIG_JSON_FETCH_FILE_BASE_PATH=./test-data
BIG_JSON_FETCH_CHUNKS_BUCKET=huron-person-chunks-dev
BIG_JSON_FETCH_REGION=us-east-2
BIG_JSON_FETCH_CACHE_ENABLED=true
BIG_JSON_FETCH_CACHE_PATH=.

# ------- Harness Groups for src\chunking\filedrop\BigJsonFile.ts ------- #
BIG_JSON_FILE_MODE=filesystem
BIG_JSON_FILE_ITEMS_PER_CHUNK=200
BIG_JSON_FILE_FILE_BASE_PATH=./test-data
BIG_JSON_FILE_FILE_NAME=20260303111609986.json
BIG_JSON_FILE_INPUT_BUCKET=huron-person-file-drop-dev
BIG_JSON_FILE_INPUT_KEY=person-full/2026-03-03T19:58:41.277Z-people.json
BIG_JSON_FILE_REGION=us-east-2

# ------- Harness Groups for src\chunking\PersonArrayWrapper.ts ------- #
PERSON_ARRAY_WRAPPER_MODE=filesystem
PERSON_ARRAY_WRAPPER_PERSON_ID_FIELD=personid
PERSON_ARRAY_WRAPPER_MAX_DETECTION_BYTES=10240
PERSON_ARRAY_WRAPPER_FILE_BASE_PATH=./test-data
PERSON_ARRAY_WRAPPER_FILE_NAME=20260303111609986.json
PERSON_ARRAY_WRAPPER_INPUT_BUCKET=huron-person-file-drop-dev
PERSON_ARRAY_WRAPPER_INPUT_KEY=person-full/2026-03-03T19:58:41.277Z-people.json
PERSON_ARRAY_WRAPPER_REGION=us-east-2

# ------- Harness Groups for src\merging\DeferredDeleteHandler.ts ------- #
DEFERRED_DELETE_CHUNKS_BUCKET=huron-person-chunks-dev
DEFERRED_DELETE_REGION=us-east-2
DEFERRED_DELETE_MERGED_NDJSON_KEY=previous-input-testing.ndjson
DEFERRED_DELETE_BASELINE_NDJSON_KEY=delta-storage/previous-input.ndjson
DEFERRED_DELETE_PERSON_DELETE_TYPE=soft
DEFERRED_DELETE_HURON_PERSON_CONFIG_PATH=../integration-huron-person/config.json
DEFERRED_DELETE_SECRET_ARN=<arn:aws:secretsmanager:region:account:secret:path-xxxxx>
DEFERRED_DELETE_DYNAMODB_STATISTICS_TABLE_NAME=huron-person-fargate-statistics
DEFERRED_DELETE_CACHE_ENABLED=true
DEFERRED_DELETE_CACHE_PATH=.

# ------- Harness Groups for src\PersonCache.ts ------- #
PERSON_CACHE_PERSON_CACHE_BUCKET_NAME=huron-person-chunks-dev
PERSON_CACHE_PERSON_CACHE_KEY=personCache.txt
PERSON_CACHE_REGION=us-east-2
PERSON_CACHE_SECRET_ARN=<arn:aws:secretsmanager:region:account:secret:path-xxxxx>
PERSON_CACHE_HURON_PERSON_CONFIG_PATH=../integration-huron-person/config.json
PERSON_CACHE_CACHE_ENABLED=true
PERSON_CACHE_CACHE_PATH=.

# ------- Harness Groups for src\processing\ApiErrorTracking.ts ------- #
API_ERROR_TRACKING_HURON_PERSON_CONFIG_PATH=../integration-huron-person/config.json
API_ERROR_TRACKING_SECRET_ARN=<arn:aws:secretsmanager:region:account:secret:path-xxxxx>
API_ERROR_TRACKING_CACHE_ENABLED=true
API_ERROR_TRACKING_CACHE_PATH=.

# ------- Harness Groups for src\statistics\StatisticsTable.ts ------- #
STATISTICS_TABLE_STATISTICS_TABLE_TASK=chunks
STATISTICS_TABLE_STATISTICS_TABLE_INTEGRATION_TIMESTAMP=2026-05-13T17:23:38.978Z
STATISTICS_TABLE_TRUNCATE_CHUNK_SIZE=25

# ------- Harness Groups for scrap\EcrChecker.ts ------- #
ECR_CHECKER_AWS_PROFILE=infnprd
ECR_CHECKER_REGION=us-east-2
ECR_CHECKER_ECR_REGISTRY_ID=<account-id>
ECR_CHECKER_ECR_REPOSITORY_NAME=huron-person-integration
```
