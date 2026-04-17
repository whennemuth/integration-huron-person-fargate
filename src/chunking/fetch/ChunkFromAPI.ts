import { AxiosResponseStreamFilter, Config, ConfigManager, DataSourceConfig } from "integration-huron-person";
import { extractChunkBasePath } from "../filedrop/ChunkPathUtils";
import { getLocalConfig } from "../../Utils";
import { S3StorageAdapter } from "../../storage/S3StorageAdapter";
import { BigJsonFetch, BigJsonFetchConfig } from "./BigJsonFetch";
import { ChunkFromParams, grabMessageBodyFromQueue, IChunkFromSource, writeMetadata } from "../../../docker/chunker";
import { PersonArrayWrapper } from "../PersonArrayWrapper";
import { SyncPopulation } from "../../../docker/chunkTypes";

export type TaskParameters = {
  baseUrl: string,
  fetchPath: string,
  populationType: SyncPopulation,
  bulkReset: boolean
};

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
 * - BULK_RESET: Used to tag the chunk files (will be referenced by processor.ts when processing the chunk files)
 * - DRY_RUN: If 'true', performs a dry run without writing chunks (default: 'false')
 * 
 * Output:
 * - Creates NDJSON chunk files in S3 at: {inputBucket}/chunks/chunk-0000.ndjson, chunk-0001.ndjson, etc.
 *   (Note: inputBucket name becomes the top-level folder prefix in chunks bucket)
 * - Each chunk triggers an S3 event notification that feeds into the processor SQS queue
 * 
 * Example Local Usage:
 * ```bash
 * INPUT_BUCKET=input-bucket INPUT_KEY=data.json CHUNKS_BUCKET=chunks-bucket BULK_RESET=false node dist/docker/chunker.js
 * ```
 */
export class ChunkFromAPI implements IChunkFromSource {
  private taskParameters: TaskParameters;

  public static defaultPopulationType = SyncPopulation.PersonFull;

  /**
   * On instantiation, attempt to read task parameters from environment variables 
   * (local dev/docker-compose mode)
   * @param config 
   */
  constructor(private config: Config) {
    this.setTaskParametersFromEnvironment();
  }

  /**
   * Set task parameters from environment variables (local dev/docker-compose mode).
   */
  private setTaskParametersFromEnvironment = (): void => {
    let { 
      BASE_URL: baseUrl, 
      FETCH_PATH: fetchPath, 
      POPULATION_TYPE: populationType 
    } = process.env;

    if (!Object.values(SyncPopulation).includes(populationType as SyncPopulation)) {
      console.log(`Invalid or missing POPULATION_TYPE (${populationType}) in environment, defaulting to ${SyncPopulation.PersonFull}`);
      populationType = ChunkFromAPI.defaultPopulationType;
    }

    if (baseUrl && fetchPath && populationType) {
      console.log('Running in local context - task parameters come from environment variables');
      this.taskParameters = {
        baseUrl: baseUrl,
        fetchPath: fetchPath,
        populationType: populationType as SyncPopulation,
        bulkReset: process.env.BULK_RESET?.toLowerCase() === 'true'
      };
    }
  }

  /**
   * Set task parameters from SQS message body (ECS mode). 
   * This will be called by the main chunker entry point after reading a message from the queue.
   * @param messageBody 
   */
  public setTaskParametersFromQueueMessageBody = (messageBody: any) => {
    let { 
      BASE_URL: baseUrl='from_config', 
      FETCH_PATH: fetchPath='from_config', 
      POPULATION_TYPE: populationType=SyncPopulation.PersonFull,
      BULK_RESET: bulkReset='false'
    } = messageBody || {}; 

    if (!Object.values(SyncPopulation).includes(populationType)) {
      console.warn(`Invalid or missing POPULATION_TYPE (${populationType}) in message parameters, defaulting to ${SyncPopulation.PersonFull}`);
      populationType = ChunkFromAPI.defaultPopulationType;
    }

    this.taskParameters = { 
      baseUrl, 
      fetchPath, 
      populationType, 
      bulkReset: bulkReset.toLowerCase() === 'true' 
    };
  }

  private setTaskParametersFromConfig = (): void => {
    const { 
      baseUrl: msgBaseUrl='from_config', 
      fetchPath: msgFetchPath='from_config', 
    } = this.taskParameters || {};
    
    const { config } = this;
    const { dataSource: { people } = {} } = config ?? {};
    
    // Guard against undefined people
    if (!people) {
      return;
    }
    
    const { endpointConfig: { baseUrl } = {}, fetchPath } = people as DataSourceConfig;

    if( ! baseUrl) {
      if(msgBaseUrl && msgBaseUrl !== 'from_config') {
        (this.config.dataSource.people as DataSourceConfig).endpointConfig.baseUrl = msgBaseUrl;
      } 
    };

    if( ! fetchPath) {
      if(msgFetchPath && msgFetchPath !== 'from_config') {
        (this.config.dataSource.people as DataSourceConfig).fetchPath = msgFetchPath;
      } 
    };
  }

  public hasSufficientTaskInfo = (logToConsole: boolean = false): boolean => {
    const { baseUrl, fetchPath } = this.taskParameters || {};
    if( ! baseUrl && logToConsole) {
      console.log('Missing required baseUrl for API source');
    }
    if( ! fetchPath && logToConsole) {
      console.log('Missing required fetchPath for API source');
    }
    return !!baseUrl && !!fetchPath;
  }

  public hasSufficientConfig = (logToConsole: boolean = false): boolean => {
    // First, try to populate task parameters from config if not already set
    if (!this.taskParameters) {
      const { dataSource: { people } = {} } = this.config ?? {};
      if (people) {
        const { endpointConfig: { baseUrl } = {}, fetchPath } = people as DataSourceConfig;
        if (baseUrl && fetchPath) {
          this.taskParameters = {
            baseUrl,
            fetchPath,
            populationType: ChunkFromAPI.defaultPopulationType,
            bulkReset: false
          };
        }
      }
    } else {
      // Task parameters exist, try to fill in missing values from config
      this.setTaskParametersFromConfig();
    }
    
    const { dataSource: { people } = {} } = this.config ?? {};
    const { endpointConfig: { apiKey } = {} } = (people as DataSourceConfig) || {};

    let sufficient = true;
    if( ! apiKey) {
      sufficient = false;
      if(logToConsole) {
        console.log('Missing required API key in config.dataSource.people.endpointConfig.apiKey');
      }
    }
    
    if( ! this.hasSufficientTaskInfo(logToConsole)) {
      sufficient = false;
    }
    return sufficient;
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
    const { populationType } = this.taskParameters || {};
    const timestamp = new Date().toISOString();
    const key = `${populationType}/${timestamp}.json`;
    console.log(`Generated synthetic input key for API source: ${key}`);
    return key;
  }

  /**
   * Perform the fetch from the API endpoint and chunking operation.
   * @param params 
   */
  public runChunking = async (params: ChunkFromParams) => {

    if (!this.hasSufficientConfig(true)) {
      process.exit(1);
    }

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

    const chunkFromAPI = new ChunkFromAPI(config);

    if (!chunkFromAPI.hasSufficientTaskInfo()) {
      const msgBody = await grabMessageBodyFromQueue();
      chunkFromAPI.setTaskParametersFromQueueMessageBody(msgBody);
    }

    // Run API fetch and chunking operation.
    await new ChunkFromAPI(config).runChunking({
      chunksBucket, region, itemsPerChunk, personIdField, dryRun
    });

  })();
}