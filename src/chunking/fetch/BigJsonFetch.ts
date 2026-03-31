import { Timer } from 'integration-core';
import { 
  Config, 
  BuCdmPeopleDataSource, 
  BuCdmPeopleDataSourceBatch,
  ResponseProcessor,
  ConfigManager,
  AxiosResponseStreamFilter
} from 'integration-huron-person';
import { IPersonArrayWrapper, PersonArrayWrapper } from "../../PersonArrayWrapper";
import { FileSystemStorageAdapter, IStorageAdapter, S3StorageAdapter } from "../../storage";
import { extractChunkBasePath } from '../filedrop/ChunkPathUtils';

/**
 * Configuration for BigJsonFetch chunking operations
 */
export interface BigJsonFetchConfig {
  /** Number of records to include in each chunk file */
  itemsPerChunk: number;
  
  /** Configuration object from integration-huron-person (contains API endpoint details) */
  config: Config;
  
  /** Optional response filter for selective field streaming */
  responseFilter?: ResponseProcessor;
  
  /** Storage adapter for writing output chunks */
  outputStorage: IStorageAdapter;
  
  /** Client ID used as top-level folder for chunk organization (required) */
  clientId: string;
  
  /** Field name to identify person objects (default: 'personid') */
  personIdField?: string;
  
  /**
   * OPTIONAL: Custom wrapper for detecting person array paths.
   * 
   * If not provided, defaults to returning '0.response' which works for
   * the BU API response structure: [{response_code: 200, response: [persons]}]
   * 
   * Provide a custom PersonArrayWrapper if you need:
   * - Auto-detection of unknown structures
   * - Different array paths
   * - Custom detection logic
   * 
   * @example
   * ```typescript
   * // Auto-detect structure
   * personArrayWrapper: new PersonArrayWrapper(storage, 'personid')
   * 
   * // Custom path
   * personArrayWrapper: { 
   *   async detectPersonArrayPath() { return '0.data.persons'; }
   * }
   * ```
   */
  personArrayWrapper?: IPersonArrayWrapper;

  /** Optional dry run mode (default: false), if true, no files will be written */
  dryRun?: boolean;
}

/**
 * Result of a chunking operation
 */
export interface ChunkResult {
  /** S3 keys of created chunk files */
  chunkKeys: string[];
  
  /** Total number of records across all chunks */
  totalRecords: number;
  
  /** Number of chunk files created */
  chunkCount: number;
}

/**
 * Handles fetching person records from an API endpoint and chunking them into NDJSON files.
 * 
 * This class uses BuCdmPeopleDataSourceBatch from the integration-huron-person package
 * to handle batched API fetching, delegating Huron-specific API logic to that project.
 * This class focuses on extracting persons from the batched responses and writing chunks.
 * 
 * Chunks are written to storage using the clientId to create the directory structure:
 * chunks/{syncType}/{timestamp}/chunk-0000.ndjson
 * 
 * @example
 * ```typescript
 * const fetcher = new BigJsonFetch({
 *   itemsPerChunk: 500,
 *   config: huronPersonConfig, // Config from integration-huron-person
 *   outputStorage: new S3StorageAdapter({ bucketName: 'my-bucket' }),
 *   clientId: 'chunks/person-full/2026-04-09T15:28:18.703Z'
 * });
 * 
 * const result = await fetcher.fetchAndChunk();
 * ```
 */
export class BigJsonFetch {
  private readonly itemsPerChunk: number;
  private readonly config: Config;
  private readonly responseFilter?: ResponseProcessor;
  private readonly outputStorage: IStorageAdapter;
  private readonly clientId: string;
  private readonly personIdField: string;
  private readonly personArrayWrapper: IPersonArrayWrapper;
  private readonly dryRun: boolean;

  constructor(config: BigJsonFetchConfig) {
    this.itemsPerChunk = config.itemsPerChunk;
    this.config = config.config;
    this.responseFilter = config.responseFilter;
    this.outputStorage = config.outputStorage;
    this.clientId = config.clientId;
    this.personIdField = config.personIdField || 'personid';
    this.dryRun = config.dryRun || false;
    
    // Use provided wrapper or default to '0.response' (BU API structure)
    this.personArrayWrapper = config.personArrayWrapper ?? {
      async detectPersonArrayPath(_key: string): Promise<string | undefined> {
        return '0.response';
      }
    };
  }

  /**
   * Fetches person records from the API endpoint and chunks them into NDJSON files.
   * 
   * This method uses BuCdmPeopleDataSourceBatch to make batched API calls,
   * extracts person records, and writes chunks to storage using the clientId-based
   * directory structure.
   * 
   * Process:
   * 1. Uses clientId to determine chunk directory (e.g., chunks/person-full/2026-04-09T15:28:18.703Z/)
   * 2. Creates BuCdmPeopleDataSource and BuCdmPeopleDataSourceBatch
   * 3. Batch processor calls API with recordCount and offset parameters
   * 4. For each batch, extracts person records and writes chunk file
   * 5. Returns metadata about created chunks
   * 
   * @returns ChunkResult with chunk keys, counts, and metadata
   * @throws Error if API call fails or no person records are found
   */
  public async fetchAndChunk(): Promise<ChunkResult> {
    console.log(`Starting fetch and chunk operation`);
    const timer = new Timer();
    timer.start();

    // Use CLIENT_ID for chunk organization (same pattern as BigJsonFile)
    // If clientId already contains 'chunks/', use as-is; otherwise append '/chunks/'
    const chunkDir = this.clientId.includes('/chunks/') || this.clientId.startsWith('chunks/')
      ? `${this.clientId}/`
      : `${this.clientId}/chunks/`;
    
    console.log(`Using chunk directory: ${chunkDir}`);

    const chunkKeys: string[] = [];
    let chunkNumber = 0;

    // Create data source for API communication
    const dataSource = new BuCdmPeopleDataSource({ 
      config: this.config, 
      responseFilter: this.responseFilter 
    });

    // Create batch processor with custom process implementation
    const batchProcessor = new class extends BuCdmPeopleDataSourceBatch {
      constructor(
        dataSource: BuCdmPeopleDataSource,
        batchSize: number,
        private parent: BigJsonFetch,
        private chunkDir: string,
        private chunkKeys: string[],
        private chunkNumber: { value: number }
      ) {
        super(dataSource, batchSize);
      }

      protected process = async (response: any[]): Promise<void> => {
        // Extract persons from response
        const persons = await this.parent.extractPersonsFromResponse(response);
        
        if (persons.length === 0) {
          console.log('No persons found in batch response');
          return;
        }

        console.log(`Received ${persons.length} persons from batch ${this.chunkNumber.value + 1}`);

        // Write chunk with the fetched persons
        const chunkKey = `${this.chunkDir}chunk-${this.parent.padChunkNumber(this.chunkNumber.value)}.ndjson`;
        await this.parent.writeChunk(chunkKey, persons);
        this.chunkKeys.push(chunkKey);
        
        if (!this.parent.dryRun) {
          console.log(`Wrote chunk ${this.chunkNumber.value + 1}: ${chunkKey} (${persons.length} records)`);
        }

        this.chunkNumber.value++;
      };
    }(dataSource, this.itemsPerChunk, this, chunkDir, chunkKeys, { value: chunkNumber });

    // Process all batches
    try {
      await batchProcessor.processBatch();
    } catch (error: any) {
      throw new Error(`Failed to fetch and chunk from API: ${error.message}`);
    }

    const totalRecords = batchProcessor.recordsProcessed();

    if (totalRecords === 0) {
      throw new Error('No person records found from API calls');
    }

    timer.stop();
    timer.logElapsed(`Completed fetch and chunk into ${chunkKeys.length} chunks with ${totalRecords} records`);
    
    return {
      chunkKeys,
      totalRecords,
      chunkCount: chunkKeys.length
    };
  }

  /**
   * Extracts person objects from the API response.
   * 
   * Uses the personArrayWrapper to detect where persons are located in the response,
   * then extracts all person objects that have the personIdField.
   * 
   * @param responseData - The data returned from the API (may be array or wrapped structure)
   * @returns Array of person objects
   */async extractPersonsFromResponse(responseData: any): Promise<any[]> {
    // Use wrapper to detect person array path
    const arrayPath = await this.personArrayWrapper.detectPersonArrayPath('api-response');
    
    if (!arrayPath) {
      // No specific path - try to find persons recursively
      return this.extractPersons(responseData);
    }

    // Navigate to the specified path
    const pathParts = arrayPath.split('.');
    let current = responseData;
    
    for (const part of pathParts) {
      if (current == null) {
        return [];
      }
      current = current[part];
    }

    // Extract persons from the located array
    if (Array.isArray(current)) {
      return current.filter(item => this.isPerson(item));
    }

    return [];
  }

  /**
   * Recursively extracts all person objects from a value.
   * 
   * @param value - The value to search (object, array, or primitive)
   * @returns Array of person objects found (empty if none)
   */
  private extractPersons(value: any): any[] {
    const persons: any[] = [];
    
    if (this.isPerson(value)) {
      persons.push(value);
    } else if (Array.isArray(value)) {
      for (const item of value) {
        persons.push(...this.extractPersons(item));
      }
    } else if (value && typeof value === 'object') {
      for (const key in value) {
        if (value.hasOwnProperty(key)) {
          persons.push(...this.extractPersons(value[key]));
        }
      }
    }
    
    return persons;
  }

  /**
   * Helper function to identify if an object is a person record.
   * 
   * @param obj - The object to check
   * @returns true if object has the personid field, false otherwise
   */
  private isPerson(obj: any): boolean {
    return (
      obj != null &&
      typeof obj === 'object' &&
      !Array.isArray(obj) &&
      this.personIdField in obj &&
      obj[this.personIdField] != null
    );
  }

  /**
   * Writes a single chunk to storage as NDJSON (newline-delimited JSON).
   * Each record is on its own line, with a trailing newline at the end.
   * 
   * In dry run mode, logs what would be written instead of actually writing.
   * 
   * Made public to allow access from nested batch processor class.
   */
  public async writeChunk(chunkKey: string, records: any[]): Promise<void> {
    if (this.dryRun) {
      console.log(`[DRY RUN] Would write chunk to: ${chunkKey} (${records.length} records)`);
      return;
    }
    
    const ndjson = records.map(record => JSON.stringify(record)).join('\n') + '\n';
    await this.outputStorage.writeFile(chunkKey, ndjson, 'application/x-ndjson');
  }

  /**
   * Pads chunk number with leading zeros to ensure proper sorting.
   * Example: 0 -> "0000", 42 -> "0042", 9999 -> "9999"
   * 
   * Made public to allow access from nested batch processor class.
   */
  public padChunkNumber(chunkNumber: number): string {
    return chunkNumber.toString().padStart(4, '0');
  }
}


/**
 * Example usage / test harness - can be run with `ts-node src/chunking/fetch/BigJsonFetch.ts` 
 * to test the fetch and chunk operation.
 * 
 * Requires a config.json file in the integration-huron-person project with people data source configuration.
 */
if (require.main === module) {
  (async () => {
    console.log('=== BigJsonFetch Test Harness ===\n');

    const { MODE, ITEMS_PER_CHUNK = '200', DRY_RUN = 'false' } = process.env;
    const itemsPerChunk = parseInt(ITEMS_PER_CHUNK, 10);
    const dryRun = DRY_RUN.toLowerCase() === 'true';

    if (!MODE || (MODE !== 'filesystem' && MODE !== 's3')) {
      console.log('No valid MODE specified ("filesystem" or "s3"), skipping test harness execution');
      return;
    }

    // Load configuration from integration-huron-person
    const workspaceFolder = process.argv[2];
    if (!workspaceFolder) {
      console.error('ERROR: Workspace folder path is required as first argument');
      return;
    }

    const configPath = require('path').resolve(workspaceFolder, '../integration-huron-person/config.json');
    
    // Load configuration
    const configManager = ConfigManager.getInstance();
    const cfg = configManager
      .reset()
      .fromSecretManager(process.env.SECRET_ARN) // Load from Secrets Manager first if SECRET_ARN is provided
      .fromEnvironment()
      .fromFileSystem(configPath)
      .getConfig('people');

    const { SYNC_TYPE: syncType = 'person-full' } = process.env;

    if (syncType !== 'person-full' && syncType !== 'person-delta') {
      console.error(`Invalid SYNC_TYPE: ${syncType} (must be 'person-full' or 'person-delta')`);
      return;
    }

    // Create synthetic input key with timestamp (mimics S3 file structure)
    const timestamp = new Date().toISOString();
    const syntheticInputKey = `${syncType}/${timestamp}.json`;
    
    // Extract chunk base path (creates: chunks/person-full/2026-04-10T15:28:18.703Z)
    const clientId = extractChunkBasePath(syntheticInputKey);

    switch (MODE) {
      case 'filesystem':
        console.log('Running in FILESYSTEM test mode');
        const { FILE_BASE_PATH: basePath = './test-data' } = process.env;

        // Instantiate fetcher and its parameters
        const fsOutputStorage = new FileSystemStorageAdapter({ basePath });
        const fsResponseFilter = new AxiosResponseStreamFilter({ fieldsOfInterest: ['personid'] });
        const fsFetcherConfig: BigJsonFetchConfig = {
          itemsPerChunk,
          config: cfg,
          responseFilter: fsResponseFilter,
          outputStorage: fsOutputStorage,
          clientId,
          personIdField: 'personid',
          personArrayWrapper: new PersonArrayWrapper(fsOutputStorage, 'personid'),
          dryRun
        };

        const fsFetcher = new BigJsonFetch(fsFetcherConfig);
        
        // Run the fetcher
        try {
          const result = await fsFetcher.fetchAndChunk();
          console.log(`✓ Created ${result.chunkCount} chunks with ${result.totalRecords} person records:`);
          result.chunkKeys.forEach(key => console.log(`  - ${key}`));
        } catch (error: any) {
          console.error(`✗ Error: ${error.message}`);
          console.error(error.stack);
        }
        break;

      case 's3':
        console.log('Running in S3 test mode');
        const { 
          CHUNKS_BUCKET: chunksBucket,
          REGION: region
        } = process.env;

        if (!chunksBucket) {
          console.error('CHUNKS_BUCKET environment variable is required for S3 test mode');
          return;
        }

        console.log(`Sync type: ${syncType}`);
        console.log(`Chunks bucket: ${chunksBucket}`);
        console.log(`Region: ${region || 'default'}\n`);

        // Instantiate fetcher and its parameters
        const s3OutputStorage = new S3StorageAdapter({ bucketName: chunksBucket, region });
        const s3ResponseFilter = new AxiosResponseStreamFilter({ fieldsOfInterest: ['personid'] });
        const s3FetcherConfig: BigJsonFetchConfig = {
          itemsPerChunk,
          config: cfg,
          responseFilter: s3ResponseFilter,
          outputStorage: s3OutputStorage,
          clientId,
          personIdField: 'personid',
          personArrayWrapper: new PersonArrayWrapper(s3OutputStorage, 'personid'),
          dryRun
        };

        const s3Fetcher = new BigJsonFetch(s3FetcherConfig);
        
        // Run the fetcher
        try {
          const result = await s3Fetcher.fetchAndChunk();
          console.log(`\n✓ Created ${result.chunkCount} chunks with ${result.totalRecords} person records:`);
          result.chunkKeys.forEach(key => console.log(`  - s3://${chunksBucket}/${key}`));
        } catch (error: any) {
          console.error(`✗ Error: ${error.message}`);
          console.error(error.stack);
        }
        break;
    }

    console.log('\n---\n');
  })();
}