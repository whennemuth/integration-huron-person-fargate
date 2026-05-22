import { AxiosResponseStreamFilter, Config, ConfigManager, DataSourceConfig, error, ResponseProcessor } from "integration-huron-person";
import { IContext } from "../../../context/IContext";
import { SyncPopulation } from "../../../docker/chunkTypes";
import { ChunkFromParams, grabMessageBodyFromQueue, IChunkFromSource, writeChunkMetadata } from "../../../docker/chunker";
import { getLocalConfig } from "../../Utils";
import { S3StorageAdapter } from "../../storage/S3StorageAdapter";
import { ApiChunkerEvent } from '../ChunkerSubscriber';
import { WriteMetadataParams } from "../Metadata";
import { PersonArrayWrapper } from "../PersonArrayWrapper";
import { extractChunkDirectory } from "../filedrop/ChunkPathUtils";
import { BigJsonFetch, BigJsonFetchConfig } from "./BigJsonFetch";
import { handleApiEvent } from './ChunkerApiSubscriber';
import { ChunkConfigOverride } from "./ChunkConfigOverride";

export type TaskParameters = {
  baseUrl: string,
  fetchPath: string,
  populationType: SyncPopulation,
  bulkReset: boolean,
  trustPreviousStorage: boolean,
  offset?: number;
  limit?: number;
  chunkDirectory?: string;
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
  private chunkDirectory: string;
  private context?: IContext;

  public static defaultPopulationType = SyncPopulation.PersonFull;

  /**
   * On instantiation, attempt to read task parameters from environment variables 
   * (local dev/docker-compose mode)
   * @param config 
   * @param context Optional context configuration for maxScalingCapacity checks
   */
  constructor(private config: Config, context?: IContext) {
    this.context = context;
    this.setTaskParametersFromEnvironment();
  }

  /**
   * Set task parameters from environment variables (local dev/docker-compose mode).
   */
  private setTaskParametersFromEnvironment = (): void => {
    let {
      POPULATION_SCOPE:scope = 'standard',
      POPULATION_TYPE: populationType,
      CHUNK_DIRECTORY: chunkDirectory,
      DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL: baseUrl,
      DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH: fetchPath,
      DATASOURCE_ENDPOINTCONFIG_PEOPLE_OFFSET: offset = '0',
      DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT: limit = '0',
      TRUST_PREVIOUS_STORAGE = 'false',
      BULK_RESET
    } = process.env;

    scope = (scope ?? 'standard').toLowerCase();
    switch(scope) {
      case 'single':
        return this.setTaskParametersFromTestEnvironment();
      case 'standard':
        break;
      default:
        console.warn(`Unrecognized POPULATION_SCOPE value: ${scope}, defaulting to 'standard'`);
        scope = 'standard';
    }

    // Set taskParameters if we have baseUrl and fetchPath
    // populationType can default to person-full if not provided
    if (baseUrl && fetchPath) {
      console.log('Running in local context - task parameters come from environment variables');
      this.taskParameters = {
        baseUrl: baseUrl,
        fetchPath: fetchPath,
        populationType: (populationType as SyncPopulation) || ChunkFromAPI.defaultPopulationType,
        bulkReset: BULK_RESET?.toLowerCase() === 'true',
        trustPreviousStorage: TRUST_PREVIOUS_STORAGE?.toLowerCase() === 'true',
        offset: offset ? parseInt(offset, 10) : undefined,
        limit: limit ? parseInt(limit, 10) : undefined,
        chunkDirectory: chunkDirectory || undefined
      };
    }
  }
  /**
   * To avoid having to acquire parameters from the SQS message for local testing, this method allows 
   * us to set parameters from a specific set of environment variables that mimic the message body 
   * structure. This is only used when POPULATION_SCOPE is set to "single" and allows us to easily 
   * test against an artificial siutation in which the fetch produces a single person result, and we 
   * can pretend it is a full population fetch by setting the SINGLE_PERSON_BUID environment variable
   * and getting only one person (this keeps the logic consistent with a full population fetch but with
   * the brevity of a single person).
   */
  private setTaskParametersFromTestEnvironment = (): void => {
    let {
      POPULATION_TYPE: populationType,
      CHUNK_DIRECTORY: chunkDirectory,
      DATASOURCE_ENDPOINTCONFIG_PERSON_BASE_URL: baseUrl,
      DATASOURCE_ENDPOINTCONFIG_PERSON_PATH: fetchPath,
      DATASOURCE_ENDPOINTCONFIG_PEOPLE_OFFSET: offset = '0',
      DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT: limit = '0',
      SINGLE_PERSON_BUID: buid,
      TRUST_PREVIOUS_STORAGE = 'false',
      BULK_RESET
    } = process.env;

    if(!buid) {
      throw new Error('SINGLE_PERSON_BUID environment variable is required when POPULATION_SCOPE is set to "single"');
    }
    fetchPath = `${fetchPath}?buid=${buid}`;

    if (baseUrl && fetchPath && populationType) {
      console.log('Running in local context - task parameters come from environment variables');
      // Fake a situation in which we pretend the values we set in the environment variables came from
      // an SQS message body, as if we had received a message from the queue.
      this.setTaskParametersFromQueueMessageBody({
        baseUrl: baseUrl,
        fetchPath: fetchPath,
        populationType: populationType as SyncPopulation,
        bulkReset: BULK_RESET?.toLowerCase() === 'true',
        trustPreviousStorage: TRUST_PREVIOUS_STORAGE?.toLowerCase() === 'true',
        offset: offset ? parseInt(offset, 10) : undefined,
        limit: limit ? parseInt(limit, 10) : undefined,
        chunkDirectory: chunkDirectory || undefined
      });
    }
  }

  /**
   * Set task parameters from SQS message body (ECS mode). 
   * This will be called by the main chunker entry point after reading a message from the queue.
   * @param messageBody 
   */
  public setTaskParametersFromQueueMessageBody = (messageBody: any) => {
    // Handle both camelCase (baseUrl) and environment variable style (DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL) property names
    // camelCase takes precedence over env var style for backwards compatibility
    let { 
      baseUrl = messageBody?.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL || 'from_config',
      fetchPath = messageBody?.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH || 'from_config',
      populationType = messageBody?.POPULATION_TYPE || SyncPopulation.PersonFull,
      offset: camelCaseOffset = messageBody?.offset,
      limit: camelCaseLimit = messageBody?.limit,
      chunkDirectory: camelCaseChunkDirectory = messageBody?.chunkDirectory,
      bulkReset = messageBody?.BULK_RESET || false,
      trustPreviousStorage: camelCaseTrustPreviousStorage = messageBody?.trustPreviousStorage
    } = messageBody || {};

    // Extract offset and limit with camelCase taking precedence over env var names
    const offset = camelCaseOffset !== undefined 
      ? camelCaseOffset 
      : (messageBody?.DATASOURCE_ENDPOINTCONFIG_PEOPLE_OFFSET || '0');
    const limit = camelCaseLimit !== undefined 
      ? camelCaseLimit 
      : (messageBody?.DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT || '0');
    const chunkDirectory = camelCaseChunkDirectory !== undefined
      ? camelCaseChunkDirectory
      : (messageBody?.DATASOURCE_ENDPOINTCONFIG_PEOPLE_CHUNK_DIRECTORY || undefined);
    const trustPreviousStorage = camelCaseTrustPreviousStorage !== undefined
      ? camelCaseTrustPreviousStorage
      : (messageBody?.TRUST_PREVIOUS_STORAGE || false);

    this.taskParameters = { 
      baseUrl, 
      fetchPath, 
      populationType, 
      offset: offset ? parseInt(offset, 10) : undefined,
      limit: limit ? parseInt(limit, 10) : undefined,
      chunkDirectory,
      bulkReset: typeof bulkReset === 'boolean' ? bulkReset : bulkReset === 'true',
      trustPreviousStorage: typeof trustPreviousStorage === 'boolean'
        ? trustPreviousStorage
        : trustPreviousStorage === 'true'
    };

    // The baseUrl and/or fetchPath values of the message may be different from the ones in the 
    // config file. If so, we need to override the config values with the ones from the message 
    // parameters so that the chunking process will use the correct endpoint and apiKey.
    const chunkConfigOverride = new ChunkConfigOverride(this.config, this.taskParameters);
    this.config = chunkConfigOverride.getOverridenConfig();
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
    
    // Special case: if BOTH are 'from_config', it means the SQS queue was empty and we got only
    // the fallback default values, so this is NOT sufficient task info (empty queue scenario)
    if (baseUrl === 'from_config' && fetchPath === 'from_config') {
      if (logToConsole) {
        console.log('Insufficient task info - queue message was empty (both baseUrl and fetchPath are default from_config placeholders)');
      }
      return false;
    }
    
    if( ! baseUrl && logToConsole) {
      console.log('Missing required baseUrl for API source');
    }
    if( ! fetchPath && logToConsole) {
      console.log('Missing required fetchPath for API source');
    }
    return !!baseUrl && !!fetchPath;
  }

  public hasSufficientConfig = (logToConsole: boolean = false): boolean => {
    const { dataSource: { people: configPeople } = {} } = this.config ?? {};
    const { endpointConfig: { baseUrl: configBaseUrl, apiKey } = {}, fetchPath: configFetchPath } = (configPeople as DataSourceConfig) || {};
    
    // Check if config has the required values
    let sufficient = !!(configBaseUrl && configFetchPath && apiKey);
    
    if (!apiKey && logToConsole) {
      console.log('Missing required API key in config.dataSource.people.endpointConfig.apiKey');
    }
    if (!configBaseUrl && logToConsole) {
      console.log('Missing required baseUrl in config');
    }
    if (!configFetchPath && logToConsole) {
      console.log('Missing required fetchPath in config');
    }
    
    // If we don't have sufficient config, try to fill in missing values from taskParameters
    if (!sufficient && this.taskParameters) {
      const { baseUrl: taskBaseUrl, fetchPath: taskFetchPath } = this.taskParameters;
      // Only use task parameters if they're not 'from_config' defaults
      const effectiveBaseUrl = (taskBaseUrl && taskBaseUrl !== 'from_config') ? taskBaseUrl : configBaseUrl;
      const effectiveFetchPath = (taskFetchPath && taskFetchPath !== 'from_config') ? taskFetchPath : configFetchPath;
      sufficient = !!(effectiveBaseUrl && effectiveFetchPath && apiKey);
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

  public getChunkDirectory = (): string => {
    const { chunkDirectory } = this.taskParameters || {};
    if(chunkDirectory) {
      return chunkDirectory;
    }
    if(!this.chunkDirectory) {
      this.chunkDirectory = extractChunkDirectory(this.getSyntheticInputKey());
    }
    return this.chunkDirectory;
  }

  /**
   * Get the bulkReset flag from task parameters.
   * Returns true if bulkReset was specified in SQS message or environment, false otherwise.
   */
  public getBulkResetFlag = (): boolean => {
    return this.taskParameters?.bulkReset || false;
  }

  /**
   * Get the trustPreviousStorage flag from task parameters.
   * Returns true if previous storage should be trusted, false otherwise.
   */
  public getTrustPreviousStorageFlag = (): boolean => {
    return this.taskParameters?.trustPreviousStorage || false;
  }

  /**
   * Get the syncPopulation value from task parameters.
   * Returns the population type (PersonFull or PersonDelta).
   */
  public getSyncPopulation = (): SyncPopulation => {
    return (this.taskParameters?.populationType as SyncPopulation) || ChunkFromAPI.defaultPopulationType;
  }

  /**
   * Create and send the next SQS message for parallel chunking.
   * Calculates the next offset (currentOffset + limit) and sends a message to the chunker queue.
   * This is called BEFORE starting the current chunking task to enable true parallelism.
   * @param currentOffset Current offset in population
   * @param limit Number of batches to process per task
   * @param queueUrl SQS queue URL
   * @returns true if message sent successfully, false if skipped
   */
  public sendNextChunkingMessage = async (
    queueUrl: string | undefined,
    dryRun?: boolean
  ): Promise<boolean> => {

    const { limit, offset: currentOffset } = this.getLimitAndOffset(); 

    // Don't create next message if limit is 0 (process all)
    if (limit === 0) {
      console.log('ℹ️  limit=0 (process all). Not creating next message.');
      return false;
    }

    if (!queueUrl) {
      console.warn('⚠️  SQS_QUEUE_URL not available. Unable to send next chunking message.');
      return false;
    }

    try {

      const { baseUrl, fetchPath, populationType, bulkReset, trustPreviousStorage } = this.taskParameters;
      const nextOffset = currentOffset + limit;
      if (dryRun) {
        console.log(`[DRY RUN] Would send next chunking message to SQS: offset=${nextOffset}, limit=${limit}`);
        return true;
      }

      const apiChunkerEvent = {
        baseUrl, fetchPath, populationType, bulkReset, trustPreviousStorage, limit, offset: nextOffset, 
        chunkDirectory: this.getChunkDirectory()
      } as ApiChunkerEvent;

      // Send the SQS message
      await handleApiEvent(apiChunkerEvent, queueUrl);

      return true;
    } catch (error) {
      return false;
    }
  }

  /**
   * Get the limit and offset for the current chunking task, taking into account the 
   * maxScalingCapacity for parallelism control. If maxScalingCapacity is 1, it overrides any
   * provided limit and offset to process the full population in a single task (no parallelism).
   * @returns An object containing the limit and offset for the current chunking task.
   */
  public getLimitAndOffset = (): { limit: number; offset: number } => {
    const envMaxScalingCapacity = process.env.MAX_SCALING_CAPACITY
      ? parseInt(process.env.MAX_SCALING_CAPACITY, 10)
      : undefined;
    const maxScalingCapacity = this.context?.ECS.chunkerService?.maxScalingCapacity ?? envMaxScalingCapacity ?? -1;
    if(!this.taskParameters) {
      console.warn('Task parameters not set, defaulting limit and offset to 0 (process full population in single task)');
      return { limit: 0, offset: 0 };
    }
    const { offset = 0, limit = 0 } = this.taskParameters;
    if (maxScalingCapacity === 1 && (limit !== 0 || offset !== 0)) {
      console.log(`ℹ️  maxScalingCapacity is 1 (parallelism disabled). Overriding offset=0, limit=0 to process full population.`);
      this.taskParameters.offset = 0;
      this.taskParameters.limit = 0;
      return this.getLimitAndOffset();
    }
    return { limit, offset };
  }

  /**
   * Bring the desired count of the service to zero to prevent further tasks from starting. This is 
   * used when there is nothing left to chunk and we need prevent any new tasks from being launched.
   * The service would eventually scale down on its own after some time, but many new extraneous
   * tasks will be launched in the meantime if we don't do this.
   */
  private setDesiredCountToZero = async (): Promise<void> => {
    const { ECS_CHUNKER_SERVICE_NAME: serviceName } = process.env;
    const clusterName = this.context?.ECS.clusterName || process.env.ECS_CLUSTER_NAME;
    const region = process.env.REGION;

    // Validate required environment variables
    if (!serviceName) {
      console.warn('⚠️  ECS_CHUNKER_SERVICE_NAME environment variable not set. Unable to scale down service.');
      return;
    }

    if (!clusterName) {
      console.warn('⚠️  Cluster name not available. Unable to scale down service.');
      return;
    }

    if (!region) {
      console.warn('⚠️  REGION environment variable not set. Unable to scale down service.');
      return;
    }

    try {
      const { ECSClient, UpdateServiceCommand } = await import('@aws-sdk/client-ecs');
      const ecsClient = new ECSClient({ region });

      const command = new UpdateServiceCommand({
        cluster: clusterName,
        service: serviceName,
        desiredCount: 0,
      });

      if (process.env.DRY_RUN?.toLowerCase() === 'true') {
        console.log(`[DRY RUN] Would scale down ECS service: cluster=${clusterName}, service=${serviceName}, desiredCount=0`);
        return;
      }

      await ecsClient.send(command);
      console.log(`✓ Successfully scaled down chunker service to desiredCount=0`);
    } catch (error: any) {
      console.error(`⚠️  Failed to scale down chunker service: ${error.message}`);
      // Non-fatal error - don't fail the chunking job
    }
  }

  /**
   * Perform the fetch from the API endpoint and chunking operation.
   * @param params 
   */
  public runChunking = async (params: ChunkFromParams) => {

    const {
      chunksBucket,
      region,
      itemsPerChunk,
      personIdField,
      bulkReset: bulkResetOverride = false,
      trustPreviousStorage: trustPreviousStorageOverride = false,
      dryRun
    } = params;

    try {
      let { bulkReset, trustPreviousStorage, populationType } = this.taskParameters || {
        bulkReset: false,
        trustPreviousStorage: false
      };
      const { limit, offset } = this.getLimitAndOffset();
      if(bulkReset !== bulkResetOverride && bulkResetOverride === true) {
        console.warn(`Overriding bulkReset flag in task parameters from ${bulkReset} to ${bulkResetOverride} based on message parameters`);
      }
      bulkReset = bulkReset || bulkResetOverride; // Allow override of bulkReset flag from message parameters if needed
      trustPreviousStorage = trustPreviousStorage || trustPreviousStorageOverride;
      
      if( ! populationType) {
        console.warn(`Population type not specified either POPULATION_TYPE environment variable or message parameters, defaulting to ${ChunkFromAPI.defaultPopulationType}`);
        this.taskParameters.populationType = ChunkFromAPI.defaultPopulationType;
      }

      if (!this.hasSufficientConfig(true)) {
        process.exit(1);
      }
      
      // Extract chunk base path (creates: chunks/person-full/2026-04-09T15:28:18.703Z)
      const chunkDirectory = this.getChunkDirectory();

      console.log(`Chunks: s3://${chunksBucket}/${chunkDirectory}/`);
      console.log(`Region: ${region || 'default'}`);
      console.log(`Items per chunk: ${itemsPerChunk}`);
      console.log(`Bulk reset flag: ${bulkReset}`);
      console.log(`Trust previous storage: ${trustPreviousStorage}`);
      console.log(`Offset: ${offset}`);
      console.log(`Limit: ${limit}`);
      console.log(`Person ID field: ${personIdField}`);
      console.log(`Population type: ${this.taskParameters.populationType}\n`);
      console.log(`Final task parameters: ${JSON.stringify(this.taskParameters)}`);

      // Create storage adapter for output chunks
      const chunksStorage = new S3StorageAdapter({ bucketName: chunksBucket, region });
      const personArrayWrapper = new PersonArrayWrapper(chunksStorage, personIdField);

      // MEMORY OPTIMIZATION 3: Create response filter to force streaming mode
      // This prevents ApiClientForApiKey from buffering entire API response in memory
      let responseFilter: ResponseProcessor | undefined;
      const people = this.config.dataSource.people;
      const fieldsOfInterest = people && 'fieldsOfInterest' in people ? people.fieldsOfInterest : undefined;

      if (fieldsOfInterest && fieldsOfInterest.length > 0) {
        // Create stream filter to prevent buffering entire API response
        // This forces responseType: 'stream' in ApiClientForApiKey and filters fields in-flight
        responseFilter = new AxiosResponseStreamFilter({ 
          fieldsOfInterest,
          maxBatchSize: 500 // Limit to 500 objects per batch to prevent unbounded accumulation
        });
        console.log(`Created response filter with ${fieldsOfInterest.length} fields of interest`);
      } else {
        console.warn('WARNING: No fieldsOfInterest configured in dataSource.people - API will buffer entire response in memory! Configure fieldsOfInterest to prevent OOM errors.');
      }

      // Configure fetcher
      const fetchConfig: BigJsonFetchConfig = {
        itemsPerChunk,
        config: this.config,
        responseFilter, // MEMORY OPTIMIZATION 3: Enable streaming mode
        outputStorage: chunksStorage,
        clientId: chunkDirectory, // Derived from synthetic input key with timestamp
        personIdField,
        personArrayWrapper,
        sourcePath: undefined, // Not used for API source, as the wrapper will detect the person array path from the API response stream directly
        offset, // indicates the "nth" chunk in from the start of the overall sync population. Used in the context of chunking "in parallel".
        limit, // indicates how many chunks to "chunk out" before stopping. Used in the context of chunking "in parallel".
        dryRun: dryRun.toLowerCase() === 'true'
      };

      // Run fetch and chunk operation
      const fetcher = new BigJsonFetch(fetchConfig);
      const result = await fetcher.fetchAndChunk();

      // Build source and target URLs for metadata
      const { baseUrl, fetchPath } = this.taskParameters;
      const sourceUrl = `${baseUrl}${fetchPath}`;

      if(result.reachedTheEndOfRecords) {
        // Build target URL from config if available
        let targetUrl: string | undefined;
        try {
          const targetBaseUrl = this.config.dataTarget?.endpointConfig?.baseUrl;
          const personsPath = this.config.dataTarget?.personsPath;
          if (targetBaseUrl && personsPath) {
            targetUrl = `${targetBaseUrl}${personsPath}`;
          }
        } catch (error) {
          // Target URL is optional, don't fail if not available
          console.log('Target URL not available in config');
        }

        // Write metadata and log results
        await writeChunkMetadata({
          storage: chunksStorage,
          bucketName: chunksBucket,
          chunkDirectory,
          chunkCount: result.chunkCount,
          totalRecords: result.totalRecords,
          itemsPerChunk,
          source: sourceUrl,
          target: targetUrl,
          dryRun: fetchConfig.dryRun || false,
          bulkReset,
          trustPreviousStorage,
          syncPopulation: this.taskParameters.populationType as SyncPopulation,
          region,
          chunkKeys: result.chunkKeys
        } satisfies WriteMetadataParams);

        await this.setDesiredCountToZero();
      }
      
      // Exit with success
      process.exit(0);

    } catch (e: any) {
      error({ msg: '\n✗ API fetch and chunk failed', o: e, flat: true });
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
    await chunkFromAPI.runChunking({
      chunksBucket, region, itemsPerChunk, personIdField, dryRun
    });

  })();
}