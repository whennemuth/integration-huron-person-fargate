/**
 * Processor Entry Point (Phase 2)
 * 
 * This module runs in Fargate tasks that are triggered by SQS messages.
 * Each message contains an S3 key pointing to an NDJSON chunk file.
 * The processor uses HuronPersonIntegration from huron-person project,
 * treating each chunk as a "mini full sync" with S3DataSourceConfig.
 * 
 * Architecture:
 * - Reuses HuronPersonIntegration.run() completely
 * - Each chunk is processed as a bulk sync operation (1 API call for all persons)
 * - Aggregate of all chunk syncs = complete full sync
 * - Zero code duplication from SyncPeople.ts
 * 
 * Environment Variables:
 * - REGION: AWS region (e.g., 'us-east-2')
 * - SECRET_ARN: Name of the Secrets Manager secret containing huron-person config. 
 *   Gets the secret as an alternative to the secrets injection of 'HURON_PERSON_CONFIG_JSON' 
 *   environment variable.
 * - CHUNKS_BUCKET: Bucket containing the chunk file (or from SQS message when running in ECS)
 * - CHUNK_KEY: Key of the NDJSON chunk file to process (or from SQS message when running in ECS)
 * - SQS_QUEUE_URL: URL of the SQS queue to read chunk messages from (if not using env vars for CHUNKS_BUCKET and CHUNK_KEY)
 * - STATIC_MAP_USAGE: JSON string specifying which static maps to load (e.g., '{ "orgMap": true, "stateMap": true, "countryMap": true }')
 * - BULK_RESET: If "true", will "upsert" all persons in the chunk, ignoring previous delta state. SEE: src\UpsertDeltaStrategy.ts
 * - STACK_ID: Unique identifier for this stack, used in SSM parameter naming for bulk reset flag
 * - DRY_RUN: If "true", runs the sync without making API calls (default: false)
 * - All huron-person config env vars (HURON_API_ENDPOINT, JWT credentials, storage config, etc.)
 * 
 * Input:
 * - NDJSON file with one person record per line
 * 
 * Output:
 * - Logs processing results
 * - Syncs all persons in chunk to Huron API via bulk sync
 * 
 * Example Usage:
 * ```bash
 * CHUNKS_BUCKET=my-bucket CHUNK_KEY=data/people/chunk-0000.ndjson node dist/processor.js
 * ```
 */

import {
  Config,
  ConfigManager,
  HuronPersonIntegration,
  S3DataSourceConfig,
  BasicCache
} from 'integration-huron-person';
import { SSMClient, GetParameterCommand } from '@aws-sdk/client-ssm';
import { NextChunk, QueueReader } from '../src/Queue';
import { StaticMapUsage } from 'integration-huron-person/dist/types/src/data-mapper/DataMapper';
import { pathUpTo } from '../src/Utils';

/**
 * Create a config with S3 data source for the chunk
 * Builds base config from environment/filesystem, then injects S3 chunk details
 * Also derives delta storage path from chunk key to organize delta outputs by run
 */
export const buildChunkConfig = async (bucketName: string, s3Key: string, region?: string): Promise<Config> => {
  // Load base configuration from environment/filesystem
  const { HURON_PERSON_CONFIG_PATH, SECRET_ARN } = process.env;
  const configManager = ConfigManager.getInstance();
  const baseConfig = await configManager
    .reset()
    .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← Check JSON first
    .fromSecretManager(SECRET_ARN)                // ← Then check Secrets Manager if SECRET_ARN is provided
    .fromEnvironment()                            // ← Then individual overrides
    .fromFileSystem(HURON_PERSON_CONFIG_PATH)     // ← Then file-based config
    .getConfigAsync('people');

  // Create S3 data source config for this chunk
  const baseRegion = baseConfig.dataSource.people && 'region' in baseConfig.dataSource.people 
    ? baseConfig.dataSource.people.region 
    : 'us-east-1';
  
  const s3DataSource: S3DataSourceConfig = {
    bucketName,
    key: s3Key,
    region: region || baseRegion,
    fieldsOfInterest: baseConfig.dataSource.people?.fieldsOfInterest
  };

  // Derive delta storage paths from chunk key
  // Chunked path: "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0124.ndjson" 
  //            -> "deltas/person-full/2026-03-03T19:58:41.277Z" (for chunk-specific delta writes)
  // Integrated path: "deltas" (for reading previous-input.ndjson created by merger)
  const chunkDir = s3Key.substring(0, s3Key.lastIndexOf('/'));
  const chunkedDeltaStoragePath = chunkDir.replace(/^chunks\//, 'deltas/');
  const integratedDeltaStoragePath = pathUpTo({ fullPath: chunkedDeltaStoragePath, segment: 'deltas' });

  console.log(`Chunked delta storage path: ${chunkedDeltaStoragePath}`);
  console.log(`Integrated delta storage path: ${integratedDeltaStoragePath}`);

  // Return config with S3 data source and overridden storage/clientId for delta storage
  // IMPORTANT: 
  // - storage.config.bucketName: Use chunks bucket (not input bucket)
  // - integration.clientId: Chunked path for writing chunk-specific deltas
  // - integratedDeltaClientId: Integrated path for reading merged previous-input.ndjson
  return {
    ...baseConfig,
    dataSource: {
      ...baseConfig.dataSource,
      people: s3DataSource
    },
    integration: {
      ...baseConfig.integration,
      clientId: chunkedDeltaStoragePath // For writing chunk-specific deltas
    },
    integratedDeltaClientId: integratedDeltaStoragePath, // For reading integrated previous-input.ndjson
    storage: {
      ...baseConfig.storage,
      config: {
        ...(baseConfig.storage.config as any),
        bucketName: bucketName,     // Use chunks bucket, not input bucket from base config
        keyPrefix: chunkedDeltaStoragePath + '/'  // Organize deltas by run timestamp
      }
    }
  } as Config;
}

/**
 * Extract chunk ID from S3 key
 * @param s3Key - S3 key like "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0124.ndjson"
 * @returns chunk ID like "0124" or undefined if not present
 */
export const extractChunkId = (s3Key: string): string | undefined => {
  const match = s3Key.match(/chunk-(\d+)\.ndjson$/);
  return match ? match[1] : undefined;
}

export const validateChunk = (chunk: NextChunk | undefined) => {
  if (!chunk) {
    throw new Error('No chunk information provided in SQS message or environment variables');
  }
  const { bucketName, s3Key } = chunk;
  if (!bucketName) {
    console.error('ERROR: CHUNKS_BUCKET environment variable or queue message required');
    process.exit(1);
  }

  if (!s3Key) {
    console.error('ERROR: CHUNK_KEY environment variable or queue message required');
    process.exit(1);
  }
}

/**
 * Get BULK_RESET flag from environment variable or SSM Parameter Store.
 * Fallback chain: ENV VAR → SSM Parameter → false
 * 
 * @param region - AWS region for SSM client
 * @param stackId - Stack ID used in parameter name
 * @returns Promise<boolean> - The bulk reset flag value
 */
export const getBulkResetFlag = async (region: string, stackId: string): Promise<boolean> => {
  // First check environment variable (for docker-compose or manual override)
  const { BULK_RESET } = process.env;
  if (BULK_RESET !== undefined) {
    const value = BULK_RESET.trim().toLowerCase() === 'true';
    console.log(`Bulk Reset (from ENV): ${value}`);
    return value;
  }

  // Try to read from SSM Parameter Store
  try {
    const ssmClient = new SSMClient({ region });
    const parameterName = `/huron-person-integration/${stackId}/bulk-reset`;
    
    const command = new GetParameterCommand({ Name: parameterName });
    const response = await ssmClient.send(command);
    
    const value = response.Parameter?.Value?.trim().toLowerCase() === 'true';
    console.log(`Bulk Reset (from SSM ${parameterName}): ${value}`);
    return value;
  } catch (error: any) {
    // If parameter doesn't exist or read fails, log warning and default to false
    if (error.name === 'ParameterNotFound') {
      console.warn('Bulk Reset parameter not found in SSM, defaulting to false');
    } else {
      console.warn(`Failed to read Bulk Reset from SSM: ${error.message}, defaulting to false`);
    }
    return false;
  }
}

export async function main(queueReader: QueueReader) {
  // Check for expected environment variables
  const { 
    REGION:region, 
    CHUNKS_BUCKET: chunksBucket,
    CHUNK_KEY: chunkKey,
    SQS_QUEUE_URL: queueUrl,
    HURON_PERSON_CONFIG_JSON,
    STATIC_MAP_USAGE,
    DRY_RUN,
    STACK_ID
  } = process.env;  
  const dryRun = `${DRY_RUN}`.trim().toLowerCase() === 'true';
  const staticMapUsage: StaticMapUsage | undefined = STATIC_MAP_USAGE ? JSON.parse(STATIC_MAP_USAGE) : undefined;

  console.log(`=== ${dryRun ? 'DRY RUN: ' : ''}Phase 2: Processor (using HuronPersonIntegration) ===\n`);
  console.log(`Chunks bucket: ${chunksBucket || 'from SQS messages'}`);
  console.log(`Chunk key: ${chunkKey || 'from SQS messages'}`);
  console.log(`SQS queue URL: ${queueUrl || 'not set, using environment variables for bucket/key'}`);
  console.log(`Huron person config json: ${HURON_PERSON_CONFIG_JSON?.substring(0, 10)}...`);
  console.log(`Static map usage: ${JSON.stringify(staticMapUsage ?? {})}`);
  
  // Get bulk reset flag with fallback chain: ENV → SSM → false
  const bulkReset = await getBulkResetFlag(region || 'us-east-2', STACK_ID || 'huron-person-fargate-processor');
  
  // Read chunk information from queue or environment
  let nextChunk: NextChunk | undefined;
  if(chunksBucket && chunkKey) {
    nextChunk = {bucketName: chunksBucket, s3Key: chunkKey};
  } 
  else if(queueUrl) {
    console.log('Reading chunk information from SQS queue...');
    nextChunk = await queueReader.receiveMessage() as NextChunk;
  } 
  else {
    console.error('ERROR: Either CHUNKS_BUCKET and CHUNK_KEY environment variables or SQS_QUEUE_URL must be provided');
    process.exit(1);
  }
  const { bucketName, s3Key } = nextChunk || {};

  // Validate required information
  validateChunk(nextChunk);

  // Extract chunk ID from S3 key (e.g., "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0029.ndjson" -> "0029")
  const chunkId = extractChunkId(s3Key!);

  console.log(`Processing chunk: s3://${bucketName}/${s3Key}`);
  if (chunkId) {
    console.log(`Chunk ID: ${chunkId}`);
  }
  console.log(`Region: ${region || 'default (us-east-1)'}\n`);

  try {
    // Build config with S3 data source pointing to this chunk
    const config = await buildChunkConfig(bucketName!, s3Key!, region);

    // Create shared cache instance for JWT tokens to avoid repeated authentication
    // This cache will be shared across all API client instances (organizations, person lookups, person updates)
    const cache = BasicCache.getInstance();
    console.log(`Cache instance created: ${cache.constructor.name}`);

    // Create and run integration using HuronPersonIntegration
    const integration = new HuronPersonIntegration({ 
      config,  // Pass pre-built config with S3 data source
      staticMapUsage, // Pass through static map usage from environment variable
      bulkReset, // Pass through bulk reset flag from environment variable
      cache // Shared cache for JWT tokens
    });
    
    // Pass chunkId to enable chunked storage output. 
    // If in dry run mode, this won't actually write deltas but allows us to see the intended storage paths in logs.
    await integration.run(`Processing chunk: s3://${bucketName}/${s3Key}`, chunkId);

    process.exit(0);

  } catch (error: any) {
    console.error(`\n✗ Processing chunk: s3://${bucketName}/${s3Key} failed:`, error.message);
    console.error(error.stack);
    process.exit(1);
  }
}

// Run if executed directly
if (require.main === module) {
  main(QueueReader.getInstance());
}
