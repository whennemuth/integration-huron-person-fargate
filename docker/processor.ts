/**
 * Processor Entry Point Router (Phase 2)
 * 
 * This module acts as a switchboard that routes to the appropriate processor
 * implementation based on storage mode detected from environment variables.
 * 
 * ## Storage Mode Detection
 * 
 * Uses the same logic as MetadataFactoryForBootstrap:
 * - If DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME or DYNAMODB_PERSON_HISTORY_TABLE_NAME is set → DynamoDB mode
 * - Otherwise → S3 mode (default)
 * 
 * ## Architecture
 * 
 * ### S3 Mode (processor-s3.ts)
 * - Writes mini-deltas to S3 for each chunk
 * - Maintains coordination via marker files
 * - Merger consolidates all chunk deltas into single previous-input.ndjson
 * 
 * ### DynamoDB Mode (processor-dynamodb.ts)
 * - Writes directly to PersonCurrentState and PersonHistory tables
 * - No mini-deltas or marker files needed
 * - Merger only handles deletion detection
 * 
 * ## Deployment
 * 
 * This single entry point is used for both modes:
 * - Docker image: Built once, contains both implementations
 * - ECS task: CMD="node dist/docker/processor.js"
 * - Environment variables determine which implementation runs
 * 
 * ## Environment Variables
 * 
 * Mode Detection:
 * - DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME: If set, uses DynamoDB mode
 * - DYNAMODB_PERSON_HISTORY_TABLE_NAME: If set, uses DynamoDB mode
 * 
 * Common Variables (both modes):
 * - REGION: AWS region
 * - CHUNKS_BUCKET: S3 bucket with chunk files
 * - CHUNK_KEY or SQS_QUEUE_URL: Chunk location
 * - STATIC_MAP_USAGE: Static map configuration
 * - BULK_RESET: Full upsert flag
 * - DRY_RUN: No-op mode
 * 
 * S3 Mode Specific:
 * - SHARED_DELTA_STORAGE_DIR: Path for merged previous-input.ndjson
 * 
 * DynamoDB Mode Specific:
 * - PERSON_CURRENT_STATE_TABLE_NAME: Current state table
 * - PERSON_HISTORY_TABLE_NAME: History audit table
 * - DYNAMODB_STATISTICS_TABLE_NAME: Error tracking table
 */

import { TestEnvironment } from 'integration-core';
import { QueueReader } from '../src/Queue';

// Import both processor implementations
import { main as mainS3 } from '../src/processing/ProcessorForS3';
import { main as mainDynamoDb } from '../src/processing/ProcessorForDynamoDb';

/**
 * Route to appropriate processor based on storage mode.
 * 
 * Detection logic matches MetadataFactoryForBootstrap:
 * - Checks for DynamoDB-mode-specific table names
 * - Falls back to S3 mode if not found
 */
async function main(queueReader: QueueReader) {
  const { 
    DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME, 
    DYNAMODB_PERSON_HISTORY_TABLE_NAME 
  } = process.env;

  // Detect storage mode from environment
  if (DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME || DYNAMODB_PERSON_HISTORY_TABLE_NAME) {
    console.log('🔀 Router: Detected DynamoDB mode (DynamoDB-mode-specific tables present)');
    console.log('   Routing to processor-dynamodb.ts\n');
    await mainDynamoDb(queueReader);
  } else {
    console.log('🔀 Router: Detected S3 mode (default - no DynamoDB tables configured)');
    console.log('   Routing to processor-s3.ts\n');
    await mainS3(queueReader);
  }
}

// Run if executed directly
if (require.main === module) {
  const testEnvironment = TestEnvironment('DOCKER_PROCESSOR');

  // Declare all environment variables used across both implementations
  [
    // Common variables
    'REGION',
    'CHUNKS_BUCKET',
    'CHUNK_KEY',
    'SQS_QUEUE_URL',
    'HURON_PERSON_CONFIG_JSON',
    'STATIC_MAP_USAGE',
    'DRY_RUN',
    'BULK_RESET',
    'RETRY_STRATEGY',
    'IS_ECS_TASK',
    'ECS_AGENT_URI',
    'HURON_PERSON_CONFIG_PATH',
    'SECRET_ARN',
    'CACHE_ENABLED',
    'CACHE_PATH',
    
    // S3 mode specific
    'SHARED_DELTA_STORAGE_DIR',
    
    // DynamoDB mode specific
    'DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME',
    'DYNAMODB_PERSON_HISTORY_TABLE_NAME',
    'DYNAMODB_STATISTICS_TABLE_NAME'
  ].forEach(testEnvironment.getVar);

  main(QueueReader.getInstance());
}
