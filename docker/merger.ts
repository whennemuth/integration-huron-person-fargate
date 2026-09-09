/**
 * Merger Entry Point Router (Phase 3)
 * 
 * This module acts as a switchboard that routes to the appropriate merger
 * implementation based on storage mode detected from environment variables.
 * 
 * ## Storage Mode Detection
 * 
 * Uses the same logic as processor.ts router:
 * - If DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME or DYNAMODB_PERSON_HISTORY_TABLE_NAME is set → DynamoDB mode
 * - Otherwise → S3 mode (default)
 * 
 * ## Architecture
 * 
 * ### S3 Mode (MergerForS3)
 * - Consolidates delta chunk files from deltas/{population}/{timestamp}/
 * - Merges with existing baseline (previous-input.ndjson)
 * - Writes merged result to delta-storage/previous-input.ndjson
 * - Cleans up temporary delta chunk files
 * 
 * ### DynamoDB Mode (MergerForDynamoDB)
 * - No file consolidation needed (processors wrote directly to DynamoDB tables)
 * - No baseline merging needed (state already in PersonCurrentStateTable)
 * - Still runs DeferredDeleteHandler for soft-deletion of removed records
 * 
 * ## Critical Design Note
 * 
 * BOTH modes need the merger service:
 * - S3 mode: File consolidation + deletion handling
 * - DynamoDB mode: Deletion handling only
 * 
 * The comment in lib/AppConstruct.ts stating "Only create merger service for file-based
 * delta storage" is INCORRECT. DynamoDB mode also needs merger for DeferredDeleteHandler.
 * 
 * ## Deployment
 * 
 * This single entry point is used for both modes:
 * - Docker image: Built once, contains both implementations
 * - ECS task: CMD="node dist/docker/merger.js"
 * - Environment variables determine which implementation runs
 * 
 * ## Environment Variables
 * 
 * Mode Detection:
 * - DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME: If set, uses DynamoDB mode
 * - DYNAMODB_PERSON_HISTORY_TABLE_NAME: If set, uses DynamoDB mode
 * 
 * Common Variables (both modes):
 * - SQS_QUEUE_URL: SQS queue URL for task parameters (ECS mode)
 * - CHUNKS_BUCKET: Bucket containing chunk files (local mode)
 * - CHUNK_DIRECTORY or INPUT_KEY: Chunk location (local mode)
 * - REGION: AWS region
 * - SHARED_DELTA_STORAGE_DIR: Path for merged output (default: 'delta-storage')
 * - DRY_RUN: If "true", runs without writing output or deleting chunks
 * - IS_ECS_TASK: ECS task indicator
 * - ECS_AGENT_URI: ECS agent endpoint
 * - PERSON_DELETE_TYPE: Deletion strategy
 * - HURON_PERSON_CONFIG_PATH or SECRET_ARN or HURON_PERSON_CONFIG_JSON: Config source
 * - CACHE_ENABLED: Cache configuration
 * - CACHE_PATH: Cache file path
 * 
 * S3 Mode Specific:
 * - Uses SHARED_DELTA_STORAGE_DIR for merged output location
 * 
 * DynamoDB Mode Specific:
 * - DYNAMODB_STATISTICS_TABLE_NAME: Statistics table (optional)
 */

import { TestEnvironment } from 'integration-core';
import { IContext } from '../context/IContext';
import { MergerForDynamoDB } from '../src/merging/MergerForDynamoDB';
import { MergerForS3 } from '../src/merging/MergerForS3';

/**
 * Route to appropriate merger based on storage mode.
 * 
 * Detection logic matches processor.ts router:
 * - Checks for DynamoDB-mode-specific table names
 * - Falls back to S3 mode if not found
 */
async function main() {
  const { 
    PREVIOUS_STORAGE_TYPE,
    DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME, 
    DYNAMODB_PERSON_HISTORY_TABLE_NAME 
  } = process.env;

  if(!PREVIOUS_STORAGE_TYPE) {
    throw new Error('PREVIOUS_STORAGE_TYPE environment variable is not set.');
  }

  // Detect storage mode from environment
  const previousStorageType = PREVIOUS_STORAGE_TYPE.toLowerCase() as IContext['PREVIOUS_STORAGE_TYPE'];
  switch(previousStorageType) {
    case 's3':
      console.log('🔀 Router: Detected S3 mode (no DynamoDB tables configured)');
      console.log('   Routing to MergerForS3\n');
      await new MergerForS3().main();
      break;
    case 'dynamodb':
      if (!DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME || !DYNAMODB_PERSON_HISTORY_TABLE_NAME) {
        throw new Error('DynamoDB mode requires both DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME and DYNAMODB_PERSON_HISTORY_TABLE_NAME to be set.');
      }
      console.log('🔀 Router: Detected DynamoDB mode (DynamoDB-mode-specific tables present)');
      console.log('   Routing to MergerForDynamoDB\n');
      await new MergerForDynamoDB().main();
      break;
    default:
      throw new Error(`Unsupported storage type: ${previousStorageType}`);
  }
}

// Run if executed directly
if (require.main === module) {
  const testEnvironment = TestEnvironment('DOCKER_MERGER');

  // Declare all environment variables needed by either merger implementation
  [
    // Common variables (both modes)
    'SQS_QUEUE_URL',
    'CHUNKS_BUCKET',
    'CHUNK_DIRECTORY',
    'INPUT_KEY',
    'REGION',
    'PREVIOUS_STORAGE_TYPE',
    'DRY_RUN',
    'IS_ECS_TASK',
    'ECS_AGENT_URI',
    'PERSON_DELETE_TYPE',
    'HURON_PERSON_CONFIG_PATH',
    'SECRET_ARN',
    'HURON_PERSON_CONFIG_JSON',
    'CACHE_ENABLED',
    'CACHE_PATH',
    
    // S3 mode specific
    'SHARED_DELTA_STORAGE_DIR',
    
    // Mode detection (DynamoDB mode)
    'DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME',
    'DYNAMODB_PERSON_HISTORY_TABLE_NAME',
    'DYNAMODB_STATISTICS_TABLE_NAME',
  ].forEach(testEnvironment.getVar);

  main().catch(error => {
    console.error('Fatal error in merger router:', error);
    process.exit(1);
  });
}
