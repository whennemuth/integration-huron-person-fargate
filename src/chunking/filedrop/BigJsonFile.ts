import { S3Client } from '@aws-sdk/client-s3';
import { parser } from 'stream-json';
import { pick } from 'stream-json/filters/Pick';
import { streamArray } from 'stream-json/streamers/StreamArray';
import { IStorageAdapter, S3StorageAdapter, FileSystemStorageAdapter } from '../../storage';
import { IPersonArrayWrapper, PersonArrayWrapper } from '../../PersonArrayWrapper';
import { Timer } from 'integration-core'

/**
 * Configuration for BigJsonFile chunking operations
 */
export interface BigJsonFileConfig {
  /** Number of records to include in each chunk file */
  itemsPerChunk: number;
  
  /** S3 bucket name where files are stored (required if storage not provided) */
  bucketName?: string;
  
  /** Optional S3 client (for testing with mocks) */
  s3Client?: S3Client;
  
  /** Field name to identify person objects (default: 'personid') */
  personIdField?: string;
  
  /** Client ID used as top-level folder for chunk organization (required) */
  clientId: string;
  
  /** Optional storage adapter (defaults to S3StorageAdapter if not provided) */
  storage?: IStorageAdapter;
  
  /** Optional separate storage adapter for output chunks (defaults to storage) */
  outputStorage?: IStorageAdapter;
  
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
 * Handles chunking of JSON files containing person records into smaller NDJSON chunk files.
 * 
 * This class streams JSON files from storage (S3, filesystem, or other implementations),
 * automatically identifies person objects by the presence of a personid field, and writes
 * them in batches as NDJSON files for memory-efficient processing.
 * 
 * The chunker works with ANY JSON structure - whether it's a plain array, deeply nested objects,
 * or wrapped API responses. It recursively finds all person objects as it streams.
 * 
 * Storage is abstracted via IStorageAdapter, allowing the same code to work with:
 * - AWS S3 (default via S3StorageAdapter)
 * - Local filesystem (via FileSystemStorageAdapter)
 * - Other storage backends (implement IStorageAdapter)
 * 
 * @example
 * ```typescript
 * // Using S3 (default behavior)
 * const chunker = new BigJsonFile({
 *   itemsPerChunk: 1000,
 *   bucketName: 'my-bucket'
 * });
 * await chunker.breakup('data/people.json');
 * 
 * // Using local filesystem
 * const localChunker = new BigJsonFile({
 *   itemsPerChunk: 1000,
 *   storage: new FileSystemStorageAdapter({ basePath: './data' })
 * });
 * await localChunker.breakup('people.json');
 * ```
 */
export class BigJsonFile {
  private readonly storage: IStorageAdapter;
  private readonly outputStorage: IStorageAdapter;
  private readonly itemsPerChunk: number;
  private readonly personIdField: string;
  private readonly clientId: string;
  private readonly personArrayWrapper: IPersonArrayWrapper;
  private readonly dryRun: boolean;

  constructor(config: BigJsonFileConfig) {
    this.itemsPerChunk = config.itemsPerChunk;
    this.personIdField = config.personIdField || 'personid';
    this.clientId = config.clientId;
    this.dryRun = config.dryRun || false;
    
    // Use provided storage adapter or default to S3
    if (config.storage) {
      this.storage = config.storage;
    } else {
      if (!config.bucketName) {
        throw new Error('bucketName is required when storage adapter is not provided');
      }
      this.storage = new S3StorageAdapter({
        bucketName: config.bucketName,
        s3Client: config.s3Client
      });
    }
    
    // Output storage defaults to main storage if not specified
    this.outputStorage = config.outputStorage || this.storage;
    
    // Use provided wrapper or default to '0.response' (BU API structure)
    this.personArrayWrapper = config.personArrayWrapper ?? {
      async detectPersonArrayPath(_key: string): Promise<string | undefined> {
        return '0.response';
      }
    };
  }

  /**
   * Breaks up a JSON file into smaller NDJSON chunk files by finding and chunking person records.
   * 
   * This method streams the JSON file once, identifies person objects at any nesting level,
   * and writes chunks immediately as they fill up (on-the-fly chunking) without loading
   * the entire file into memory.
   * 
   * Process:
   * 1. Streams JSON file from storage using stream-json for memory efficiency
   * 2. Identifies person objects by checking for personid field (works at any nesting level)
   * 3. Accumulates person records into batches of size itemsPerChunk
   * 4. Writes each batch as NDJSON to storage immediately (on-the-fly chunking)
   * 5. Naming pattern: {original-directory}/{original-name}/chunk-0000.ndjson
   * 6. Returns metadata about created chunks
   * 
   * Example: data/api-response.json -> data/api-response/chunk-0000.ndjson, chunk-0001.ndjson, etc.
   * 
   * @param key - Storage key of the original JSON file (any structure containing person objects)
   * @returns ChunkResult with chunk keys, counts, and metadata
   * @throws Error if file doesn't exist, isn't valid JSON, or contains no person records
   */
  public async breakup(key: string): Promise<ChunkResult> {
    console.log(`Starting breakup operation for: ${key}`);
    const timer = new Timer();
    timer.start();

    // Detect person array path using wrapper
    const arrayPath = await this.personArrayWrapper.detectPersonArrayPath(key);
    if (arrayPath) {
      console.log(`Using person array path: ${arrayPath}`);
    } else {
      console.log('No specific array path, using flexible discovery mode');
    }

    // Stream, identify persons, and chunk on-the-fly
    const { chunkKeys, totalRecords } = await this.streamAndChunk(key, arrayPath);
    
    if (totalRecords === 0) {
      throw new Error(`No person records found in: ${key}`);
    }

    timer.stop();
    timer.logElapsed(`Completed breakup of ${key} into ${chunkKeys.length} chunks with ${totalRecords} records`);
    
    return {
      chunkKeys,
      totalRecords,
      chunkCount: chunkKeys.length
    };
  }

  /**
   * Streams the JSON file and chunks person records on-the-fly.
   * 
   * Supports two modes based on arrayPath parameter:
   * 
   * **MODE 1: Direct path streaming (arrayPath provided)**
   * - Uses pick() + streamArray() to navigate directly to nested array
   * - Streams individual persons DURING JSON parsing
   * - Memory-efficient: Never loads full nested array into memory
   * - Example: arrayPath='0.response' for [{response: [persons]}]
   * 
   * **MODE 2: Flexible discovery (arrayPath not provided)**
   * - Uses streamArray() + extractPersons() to find persons at any level
   * - Works with any JSON structure
   * - **Tradeoff:** Parses nested arrays into memory first
   * 
   * @param key - Storage key of the file
   * @param arrayPath - Optional path to person array (auto-detected or configured)
   * @returns Chunk keys and total count
   */
  private async streamAndChunk(key: string, arrayPath?: string): Promise<{ chunkKeys: string[]; totalRecords: number }> {
    const stream = await this.storage.getReadStream(key);

    const chunkKeys: string[] = [];
    let currentChunk: any[] = [];
    let totalRecords = 0;
    let chunkNumber = 0;

    // Use CLIENT_ID for chunk organization
    // If clientId already contains 'chunks/', use as-is; otherwise append '/chunks/'
    const chunkDir = this.clientId.includes('/chunks/') || this.clientId.startsWith('chunks/')
      ? `${this.clientId}/`
      : `${this.clientId}/chunks/`;

    return new Promise((resolve, reject) => {
      // Build pipeline based on whether we have a nested path
      let pipeline = stream.pipe(parser());
      
      if (arrayPath) {
        // MODE 1: Direct path to nested array (memory-efficient for large datasets)
        // Uses pick to navigate to specific path, then streamArray emits each person
        pipeline = pipeline.pipe(pick({ filter: arrayPath })).pipe(streamArray());
      } else {
        // MODE 2: Flexible structure discovery (may accumulate nested arrays)
        // streamArray emits top-level elements, extractPersons finds persons recursively
        pipeline = pipeline.pipe(streamArray());
      }
      
      pipeline
        .on('data', async (data: { key: number; value: any }) => {
          // Extract persons from this element
          const persons = arrayPath 
            ? [data.value] // pick+streamArray already emits individual persons
            : this.extractPersons(data.value); // Recursively find persons
          
          if (persons.length === 0) {
            return; // Skip elements with no persons
          }

          // Pause stream to handle backpressure during writes
          pipeline.pause();

          // Add persons to current chunk
          for (const person of persons) {
            if (!this.isPerson(person)) {
              continue; // Skip non-person objects
            }
            
            currentChunk.push(person);
            totalRecords++;
            
            // When chunk is full, write it immediately
            if (currentChunk.length >= this.itemsPerChunk) {
              try {
                const chunkKey = `${chunkDir}chunk-${this.padChunkNumber(chunkNumber)}.ndjson`;
                await this.writeChunk(chunkKey, currentChunk);
                chunkKeys.push(chunkKey);
                if (!this.dryRun) {
                  console.log(`Wrote chunk ${chunkNumber + 1}: ${chunkKey} (${currentChunk.length} records)`);
                }
                
                chunkNumber++;
                currentChunk = []; // Start new chunk
              } catch (error: any) {
                pipeline.destroy();
                reject(new Error(`Failed to write chunk: ${error.message}`));
                return;
              }
            }
          }

          pipeline.resume();
        })
        .on('end', async () => {
          try {
            // Write the last partial chunk if it exists
            if (currentChunk.length > 0) {
              const chunkKey = `${chunkDir}chunk-${this.padChunkNumber(chunkNumber)}.ndjson`;
              await this.writeChunk(chunkKey, currentChunk);
              chunkKeys.push(chunkKey);
              if (!this.dryRun) {
                console.log(`Wrote final chunk ${chunkNumber + 1}: ${chunkKey} (${currentChunk.length} records)`);
              }
            }
            
            resolve({ chunkKeys, totalRecords });
          } catch (error: any) {
            reject(new Error(`Failed to write final chunk: ${error.message}`));
          }
        })
        .on('error', (error: Error) => {
          reject(new Error(`Failed to parse JSON from ${key}: ${error.message}`));
        });
    });
  }

  /**
   * Recursively extracts all person objects from a value.
   * 
   * This helper function is the "drill down" logic - it searches through
   * nested structures (objects, arrays) to find all person objects. This
   * allows the chunker to handle any JSON structure, whether persons are
   * at the top level or deeply nested.
   * 
   * @param value - The value to search (object, array, or primitive)
   * @returns Array of person objects found (empty if none)
   */
  private extractPersons(value: any): any[] {
    const persons: any[] = [];
    
    if (this.isPerson(value)) {
      // This value itself is a person
      persons.push(value);
    } else if (Array.isArray(value)) {
      // Recursively search array elements
      for (const item of value) {
        persons.push(...this.extractPersons(item));
      }
    } else if (value && typeof value === 'object') {
      // Recursively search object properties
      for (const key in value) {
        if (value.hasOwnProperty(key)) {
          persons.push(...this.extractPersons(value[key]));
        }
      }
    }
    // Primitives (string, number, etc.) - no persons
    
    return persons;
  }

  /**
   * Helper function to identify if an object is a person record.
   * 
   * Checks for the presence of the personIdField (default: 'personid').
   * This allows the chunker to work with any JSON structure by finding
   * person objects at any nesting level during streaming.
   * 
   * This is the "drill down" logic - it tests each value emitted by
   * streamArray() to see if it's a person object, making it easy to
   * test and reason about separately from the streaming logic.
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
   */
  private async writeChunk(chunkKey: string, records: any[]): Promise<void> {
    if (this.dryRun) {
      // Dry run mode: log what would be written without actually writing
      console.log(`[DRY RUN] Would write chunk to: ${chunkKey} (${records.length} records)`);
      return;
    }
    
    const ndjson = records.map(record => JSON.stringify(record)).join('\n') + '\n';
    await this.outputStorage.writeFile(chunkKey, ndjson, 'application/x-ndjson');
  }

  /**
   * Extracts the base name from a storage key (filename without extension).
   * Example: "data/people.json" -> "people"
   */
  private getBaseName(key: string): string {
    const parts = key.split('/');
    const filename = parts[parts.length - 1];
    return filename.replace(/\.[^.]+$/, ''); // Remove extension
  }

  /**
   * Extracts the directory from a storage key (everything before the filename).
   * Example: "data/people.json" -> "data/"
   * Example: "people.json" -> ""
   */
  private getDirectory(key: string): string {
    const parts = key.split('/');
    if (parts.length === 1) {
      return ''; // No directory, file is at root
    }
    return parts.slice(0, -1).join('/') + '/';
  }

  /**
   * Pads chunk number with leading zeros to ensure proper sorting.
   * Example: 0 -> "0000", 42 -> "0042", 9999 -> "9999"
   */
  private padChunkNumber(chunkNumber: number): string {
    return chunkNumber.toString().padStart(4, '0');
  }
}


/**
 * Example usage / test harness - can be run with `ts-node src/BigJsonFile.ts` 
 * to test the breakup operation with different storage backends.
 */
if (require.main === module) {
  (async () => {
    console.log('=== BigJsonFile Test Harness ===\n');

    const { MODE, ITEMS_PER_CHUNK='200' } = process.env;
    const itemsPerChunk = parseInt(ITEMS_PER_CHUNK, 10);
    const config = { itemsPerChunk } as BigJsonFileConfig;

    switch (MODE) {
      case 'filesystem':
        console.log('Running in FILESYSTEM test mode');
        const { FILE_BASE_PATH:basePath='./test-data', FILE_NAME:fileName } = process.env;
        if( ! fileName) {
          console.error('FILE_NAME environment variable is required for FILESYSTEM test mode');
          return;
        }

        // Instantiate chunker and its parameters
        config.storage = new FileSystemStorageAdapter({ basePath });
        config.personArrayWrapper = new PersonArrayWrapper(config.storage, 'personid');     
        const fsChunker = new BigJsonFile(config);
        
        // Run the chunker
        try {
          const result = await fsChunker.breakup(fileName);
          console.log(`✓ Created ${result.chunkCount} chunks with ${result.totalRecords} person records:`);
          result.chunkKeys.forEach(key => console.log(`  - ${key}`));
        } catch (error: any) {
          console.error(`✗ Error: ${error.message}`);
        }
        break;
      case 's3':
        console.log('Running in S3 test mode');
        const { INPUT_BUCKET:bucketName, INPUT_KEY:s3Key, REGION:region } = process.env;
        if( ! bucketName) {
          console.error('INPUT_BUCKET environment variable is required for S3 test mode');
          return;
        }
        if( ! s3Key) {
          console.error('INPUT_KEY environment variable is required for S3 test mode');
          return;
        }
        
        // Instantiate chunker and its parameters
        config.storage = new S3StorageAdapter({ bucketName, region });
        config.personArrayWrapper = new PersonArrayWrapper(config.storage, 'personid');
        const s3Chunker = new BigJsonFile(config);
        
        // Run the chunker
        try {
          const result = await s3Chunker.breakup(s3Key);
          console.log(`✓ Created ${result.chunkCount} chunks with ${result.totalRecords} person records:`);
          result.chunkKeys.forEach(key => console.log(`  - ${key}`));
        } catch (error: any) {
          console.error(`✗ Error: ${error.message}`);
        }
        break;
      default:
        console.log('No valid MODE specified ("filesystem" or "s3"), skipping test harness execution');
    }

    console.log('\n---\n');
  })();
}

