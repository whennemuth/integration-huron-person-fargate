import { AxiosResponseStreamFilter, Config, ConfigManager, DataSourceConfig } from "integration-huron-person";
import { extractChunkBasePath } from "../src/chunking/filedrop/ChunkPathUtils";
import { getLocalConfig } from "../src/Utils";
import { S3StorageAdapter } from "../src/storage/S3StorageAdapter";
import { BigJsonFetch, BigJsonFetchConfig } from "../src/chunking/fetch/BigJsonFetch";
import { ChunkFromParams, IChunkFromSource, writeMetadata } from "./chunker";
import { PersonArrayWrapper } from "../src/PersonArrayWrapper";

/**
 * Chunker Entry Point (Phase 1)
 * 
 * This module runs in a Fargate task caused by SQS messages created by ChunkerSubscriber Lambda.
 * It fetches a large JSON file *** FROM AN API ENDPOINT *** containing person records and 
 * breaks it up into smaller NDJSON chunk files for parallel processing.
 * 
 * Two modes of operation:
 * 1. ECS Fargate (production): Reads INPUT_BUCKET and INPUT_KEY from SQS message
 * 2. Local development: Reads INPUT_BUCKET and INPUT_KEY from environment variables (fallback)
 * 
 * Environment Variables:
 * - Config:
 *   - SECRET_ARN: Secrets Manager ARN containing config (fallback)
 *     and/or...
 *   - HURON_PERSON_CONFIG_PATH: Path to config.json (local dev only)
 *     and/or...
 *   - HURON_PERSON_CONFIG_JSON: Full config as JSON string from TaskDef secrets
 * - SYNC_TYPE: 'person-full' or 'person-delta'
 * - SQS_QUEUE_URL: SQS queue URL to read task parameters from (if running in ECS context)
 * - CHUNKS_BUCKET: Destination bucket for chunk files
 * - REGION: AWS region (e.g., 'us-east-2')
 * - ITEMS_PER_CHUNK: Number of persons per chunk (default: 200)
 * - PERSON_ID_FIELD: Field name for person IDs (default: 'personid')
 * - DRY_RUN: If 'true', performs a dry run without writing chunks (default: 'false')
 * 
 * Output:
 * - Creates NDJSON chunk files in S3 at: {inputBucket}/chunks/chunk-0000.ndjson, chunk-0001.ndjson, etc.
 *   (Note: inputBucket name becomes the top-level folder prefix in chunks bucket)
 * - Each chunk triggers an S3 event notification that feeds into the processor SQS queue
 * 
 * Example Local Usage:
 * ```bash
 * INPUT_BUCKET=input-bucket INPUT_KEY=data.json CHUNKS_BUCKET=chunks-bucket node dist/docker/chunker.js
 * ```
 */
export class ChunkFromAPI implements IChunkFromSource {
  constructor(private config: Config) { }

  private validateConfig = () => {
    const { config } = this;
    const { dataSource: { people } = {} } = config ?? {};
    const { endpointConfig: { apiKey, baseUrl } = {}, fetchPath } = people as DataSourceConfig;
    if( ! baseUrl) {
      console.error('ERROR: Missing required API config.dataSource.people.endpointConfig.baseUrl in config');
      process.exit(1);
    };
    if( ! fetchPath) {
      console.error('ERROR: Missing required API config.dataSource.people.fetchPath in config');
      process.exit(1);
    }
    if( ! apiKey) {
      console.error('ERROR: Missing required API config.dataSource.people.endpointConfig.apiKey in config');
      process.exit(1);
    }
  }

  /**
   * Create synthetic input key with timestamp. Mimics S3 file structure, as if the
   * source file were coming from S3 (it does not - it comes from an API endpoint fetch) 
   * as would be the case with the ChunkFromS3 class - this allows us to reuse the same 
   * chunk base path extraction logic and chunk storage structure in S3 for both API and 
   * S3 sources.
   * @param syncType 
   * @returns 
   */
  private getSyntheticInputKey(): string {
    const { SYNC_TYPE: syncType = 'person-full' } = process.env;
    if (syncType !== 'person-full' && syncType !== 'person-delta') {
      console.error(`ERROR: Invalid SYNC_TYPE: ${syncType} (must be 'person-full' or 'person-delta')`);
      process.exit(1);
    }
    console.log(`Sync type: ${syncType}`);
    const timestamp = new Date().toISOString();
    return `${syncType}/${timestamp}.json`;
  }


  public runChunking = async (params: ChunkFromParams) => {

    this.validateConfig();

    const { chunksBucket, region, itemsPerChunk, personIdField, dryRun } = params;
    
    // Extract chunk base path (creates: chunks/person-full/2026-04-09T15:28:18.703Z)
    const chunkBasePath = extractChunkBasePath(this.getSyntheticInputKey());
    
    console.log(`Chunks: s3://${chunksBucket}/${chunkBasePath}/`);
    console.log(`Region: ${region || 'default'}`);
    console.log(`Items per chunk: ${itemsPerChunk}`);
    console.log(`Person ID field: ${personIdField}\n`);

    try {

      // Create storage adapter for output chunks
      const chunksStorage = new S3StorageAdapter({ bucketName: chunksBucket, region });
      const personArrayWrapper = new PersonArrayWrapper(chunksStorage, personIdField);
      const responseFilter = new AxiosResponseStreamFilter({ fieldsOfInterest: [personIdField] });

      // Configure fetcher
      const fetchConfig: BigJsonFetchConfig = {
        itemsPerChunk,
        config: this.config,
        responseFilter,
        outputStorage: chunksStorage,
        clientId: chunkBasePath, // Derived from synthetic input key with timestamp
        personIdField,
        personArrayWrapper,
        dryRun: dryRun.toLowerCase() === 'true'
      };

      // Run fetch and chunk operation
      const fetcher = new BigJsonFetch(fetchConfig);
      const result = await fetcher.fetchAndChunk();

      // Write metadata and log results
      await writeMetadata(
        chunksStorage,
        chunksBucket,
        chunkBasePath,
        result,
        itemsPerChunk,
        `api://config`,
        fetchConfig.dryRun || false
      );

      // Exit with success
      process.exit(0);

    } catch (error: any) {
      console.error('\n✗ API fetch and chunk failed:', error.message);
      console.error(error.stack);
      process.exit(1);
    }
  }  
}


if(require.main === module) {
  // Read additional configuration from environment
  const {
    HURON_PERSON_CONFIG_PATH, 
    SECRET_ARN,
    CHUNKS_BUCKET: chunksBucket = '',
    REGION: region,
    ITEMS_PER_CHUNK: itemsPerChunkStr = '200',
    PERSON_ID_FIELD: personIdField = 'personid',
    DRY_RUN: dryRun = 'false'
  } = process.env;

  // Validate bucket name required for output is provided.
  if (!chunksBucket) {
    console.error('ERROR: CHUNKS_BUCKET environment variable is required');
    process.exit(1);
  }

  // Validate items per chunk is a positive integer
  const itemsPerChunk = parseInt(itemsPerChunkStr, 10);
  if (isNaN(itemsPerChunk) || itemsPerChunk <= 0) {
    console.error(`ERROR: Invalid ITEMS_PER_CHUNK: ${itemsPerChunkStr}`);
    process.exit(1);
  }

  (async () => {
    // Load configuration.
    const configManager = ConfigManager.getInstance();
    const localConfigPath = HURON_PERSON_CONFIG_PATH || getLocalConfig();
    const config = await configManager
      .reset()
      .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← TaskDef secret injection
      .fromSecretManager(SECRET_ARN)                // ← Fallback to Secrets Manager
      .fromEnvironment()                            // ← Fallback to individual env var overrides
      .fromFileSystem(localConfigPath)              // ← Local dev only
      .getConfigAsync('people');

    // Run API fetch and chunking operation.
    await new ChunkFromAPI(config).runChunking({
      chunksBucket, region, itemsPerChunk, personIdField, dryRun
    });

  })();
}