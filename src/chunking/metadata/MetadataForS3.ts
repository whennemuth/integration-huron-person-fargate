import { GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Config } from 'integration-huron-person';
import { S3StorageAdapter } from '../../storage/S3StorageAdapter';
import { objectExistsInS3 } from '../../Utils';
import {
  AbstractMetadata,
  ChunkMetadata,
  Flags,
  MarkRunFailedParams,
  ReadFlagsParams,
  ReadMetadataParams,
  ReadTerminalErrorParams,
  TerminalError,
  WriteFlagsParams,
  WriteMetadataParams
} from './AbstractMetadata';

/**
 * S3-based metadata management implementation.
 * 
 * Stores metadata as JSON files in S3:
 * - _metadata.json: Run manifest and configuration
 * - _flags.json: Sync configuration flags
 * - _terminal_error.json: Terminal error marker
 * 
 * ## File Locations
 * All files stored under: `s3://{bucket}/chunks/{populationType}/{timestamp}/`
 * 
 * ## Usage:
 * ```typescript
 * const metadata = new MetadataForS3({ config });
 * await metadata.writeFlags({ bucketName, chunkDirectory, ...flags });
 * await metadata.write({ bucketName, chunkDirectory, itemsPerChunk, source, ...flags });
 * ```
 */
export class MetadataForS3 extends AbstractMetadata {
  private storage?: S3StorageAdapter;

  constructor(params: { config: Config; storage?: S3StorageAdapter }) {
    super(params);
    this.storage = params.storage;
  }

  /**
   * Write chunk metadata to S3.
   * Persists only the run manifest (flags, paths, timestamps).
   * Parameters like chunkCount, totalRecords, chunkKeys are accepted for caller convenience but NOT persisted.
   * These values are now determined from contiguous marker files and S3 state, not from metadata.
   */
  public async write(params: WriteMetadataParams): Promise<void> {
    const { 
      bucketName, chunkDirectory, itemsPerChunk,
      source,
      target,
      bulkReset,
      trustPreviousStorage,
      syncPopulation,
      runFailed,
      runFailureMessage,
      runFailureTimestamp,
      dryRun = false,
      region,
      replace = false
    } = params;

    if (!bucketName) {
      throw new Error('bucketName is required for S3 metadata operations');
    }

    const metadataKey = AbstractMetadata.getMetadataKey(chunkDirectory);
    const deltaStoragePath = AbstractMetadata.deriveDeltaStoragePath(chunkDirectory);

    const metadata: ChunkMetadata = {
      itemsPerChunk, source, chunkDirectory,
      deltaStoragePath, bulkReset, trustPreviousStorage, syncPopulation, createdAt: new Date().toISOString()
    };

    // Add optional target field
    if (target) {
      metadata.target = target;
    }
    if (runFailed !== undefined) {
      metadata.runFailed = runFailed;
    }
    if (runFailureMessage !== undefined) {
      metadata.runFailureMessage = runFailureMessage;
    }
    if (runFailureTimestamp !== undefined) {
      metadata.runFailureTimestamp = runFailureTimestamp;
    }

    const metadataJson = JSON.stringify(metadata, null, 2);
    const metadataLog = `s3://${bucketName}/${metadataKey}`;

    if (dryRun) {
      console.log(`[DRY RUN] Would write metadata to: ${metadataLog}`);
      console.log(`[DRY RUN] Content: ${metadataJson}`);
    } 
    else {
      /**
       * Don't overwrite existing metadata file. If it exists, this means the first chunking run 
       * already wrote the metadata, so we should not overwrite it with subsequent runs because 
       * the metadata is the same for all chunks in the same chunking run and is only meant to be 
       * written once at the end of the first chunking run when all information is available.
       */
      let cancel: boolean = false;
      if ( ! replace) {
        cancel = await objectExistsInS3(bucketName, metadataKey, region);
        if (cancel) {
          console.warn(`Metadata file already exists at ${metadataLog}. Skipping write.`);
          return;
        }
      }

      if (this.storage) {
        // Use provided storage adapter or create S3Client
        await this.storage.writeFile(metadataKey, metadataJson, 'application/json');
      } 
      else {
        const s3Client = new S3Client({ region });
        await s3Client.send(new PutObjectCommand({
          Bucket: bucketName,
          Key: metadataKey,
          Body: metadataJson,
          ContentType: 'application/json'
        }));
      }
      console.log(`\n✓ Metadata written: ${metadataLog}`);
    }
  }

  /**
   * Write flags file to S3 before chunking starts.
   * This file contains only bulkReset and syncPopulation flags needed by processors.
   * Written early so processor tasks can read flags even before chunking completes.
   */
  public async writeFlags(params: WriteFlagsParams): Promise<void> {
    const {
      bucketName,
      chunkDirectory,
      bulkReset,
      trustPreviousStorage,
      syncPopulation,
      runFailed,
      runFailureMessage,
      runFailureTimestamp,
      dryRun = false,
      region,
      replace = false
    } = params;

    if (!bucketName) {
      throw new Error('bucketName is required for S3 metadata operations');
    }

    const flagsKey = AbstractMetadata.getFlagsKey(chunkDirectory);
    const flags: Flags = {
      bulkReset,
      trustPreviousStorage,
      syncPopulation
    };

    if (runFailed !== undefined) {
      flags.runFailed = runFailed;
    }
    if (runFailureMessage !== undefined) {
      flags.runFailureMessage = runFailureMessage;
    }
    if (runFailureTimestamp !== undefined) {
      flags.runFailureTimestamp = runFailureTimestamp;
    }

    const flagsJson = JSON.stringify(flags, null, 2);
    const flagsLog = `s3://${bucketName}/${flagsKey}`;

    if (dryRun) {
      console.log(`[DRY RUN] Would write flags to: ${flagsLog}`);
      console.log(`[DRY RUN] Content: ${flagsJson}`);
    } else {
      // Use provided storage adapter or create S3Client
      if (this.storage) {
        await this.storage.writeFile(flagsKey, flagsJson, 'application/json');
      } else {
        const s3Client = new S3Client({ region });
        await s3Client.send(new PutObjectCommand({
          Bucket: bucketName,
          Key: flagsKey,
          Body: flagsJson,
          ContentType: 'application/json'
        }));
      }
      console.log(`✓ Flags written: ${flagsLog}`);
    }
  }

  /**
   * Read chunk metadata from S3.
   * Replaces getChunkMetadata and readChunkMetadata with unified implementation.
   */
  public async read(params: ReadMetadataParams): Promise<Partial<ChunkMetadata>> {
    const { bucketName, chunkDirectory, region } = params;
    
    if (!bucketName) {
      console.error('Missing required bucket name');
      return {};
    }

    const metadataKey = AbstractMetadata.getMetadataKey(chunkDirectory);
    const s3Client = new S3Client({ region });

    try {
      console.log(`Reading metadata from: s3://${bucketName}/${metadataKey}`);
      
      const response = await s3Client.send(
        new GetObjectCommand({
          Bucket: bucketName,
          Key: metadataKey
        })
      );

      const body = await response.Body?.transformToString();
      if (!body) {
        console.log(`No metadata file found at s3://${bucketName}/${metadataKey}`);
        return {};
      }

      const metadata = JSON.parse(body) as ChunkMetadata;
      console.log(`✓ Metadata loaded: ${JSON.stringify(metadata)}`);
      
      // Log warnings for missing expected fields
      this.validateMetadata(metadata);
      
      return metadata;
    } catch (error: any) {
      if (error.name === 'NoSuchKey') {
        console.log(`No metadata file found at s3://${bucketName}/${metadataKey}`);
        return {};
      }
      console.warn(`Warning: Could not read metadata file: ${error.message}`);
      console.warn('Falling back to environment variables for configuration');
      return {};
    }
  }

  /**
   * Read flags file from S3.
   * Flags file contains bulkReset and syncPopulation needed by processors.
   */
  public async readFlags(params: ReadFlagsParams): Promise<Partial<Flags>> {
    const { bucketName, chunkDirectory, region } = params;
    
    if (!bucketName) {
      console.error('Missing required bucket name');
      return {};
    }

    const flagsKey = AbstractMetadata.getFlagsKey(chunkDirectory);
    const s3Client = new S3Client({ region });

    try {
      console.log(`Reading flags from: s3://${bucketName}/${flagsKey}`);
      
      const response = await s3Client.send(
        new GetObjectCommand({
          Bucket: bucketName,
          Key: flagsKey
        })
      );

      const body = await response.Body?.transformToString();
      if (!body) {
        console.log(`No flags file found at s3://${bucketName}/${flagsKey}`);
        return {};
      }

      const flags = JSON.parse(body) as Flags;
      console.log(`✓ Flags loaded: ${JSON.stringify(flags)}`);
      
      return flags;
    } catch (error: any) {
      if (error.name === 'NoSuchKey') {
        console.log(`No flags file found (chunking may not have started yet)`);
        return {};
      }
      console.warn(`Warning: Could not read flags file: ${error.message}`);
      console.warn('Falling back to environment variables for configuration');
      return {};
    }
  }

  /**
   * Mark the chunking run as failed.
   * This prevents merger orchestration from treating partial chunk output as successful completion.
   */
  public async markRunFailed(params: MarkRunFailedParams): Promise<void> {
    const { bucketName, chunkDirectory, region, errorMessage } = params;
    
    if (!bucketName) {
      throw new Error('bucketName is required for S3 metadata operations');
    }

    const markerKey = AbstractMetadata.getTerminalErrorKey(chunkDirectory);
    const marker: TerminalError = {
      stage: 'chunking',
      chunkDirectory,
      errorMessage,
      errorTimestamp: new Date().toISOString()
    };

    const s3Client = new S3Client({ region });
    await s3Client.send(new PutObjectCommand({
      Bucket: bucketName,
      Key: markerKey,
      Body: JSON.stringify(marker, null, 2),
      ContentType: 'application/json'
    }));

    console.error(`⛔ Terminal error marker written: s3://${bucketName}/${markerKey}`);
  }

  /**
   * Determine whether the run has been explicitly marked as failed.
   */
  public async isRunFailed(params: ReadFlagsParams): Promise<boolean> {
    const flags = await this.readFlags(params);
    return flags.runFailed === true;
  }

  /**
   * Check whether a terminal error marker exists for the run.
   */
  public async terminalErrorExists(params: ReadTerminalErrorParams): Promise<boolean> {
    const { bucketName, chunkDirectory, region } = params;
    
    if (!bucketName) {
      throw new Error('bucketName is required for S3 metadata operations');
    }

    const key = AbstractMetadata.getTerminalErrorKey(chunkDirectory);
    return objectExistsInS3(bucketName, key, region);
  }

  /**
   * Read terminal error marker details if present.
   */
  public async readTerminalError(params: ReadTerminalErrorParams): Promise<TerminalError | undefined> {
    const { bucketName, chunkDirectory, region } = params;
    
    if (!bucketName) {
      throw new Error('bucketName is required for S3 metadata operations');
    }

    const key = AbstractMetadata.getTerminalErrorKey(chunkDirectory);
    const s3Client = new S3Client({ region });

    try {
      const response = await s3Client.send(new GetObjectCommand({
        Bucket: bucketName,
        Key: key
      }));
      const body = await response.Body?.transformToString();
      if (!body) {
        return undefined;
      }
      return JSON.parse(body) as TerminalError;
    } catch (error: any) {
      if (error.name === 'NoSuchKey') {
        return undefined;
      }
      throw error;
    }
  }

  /**
   * Read flags from a chunk file S3 key by deriving the chunk directory
   * @param bucketName - S3 bucket name
   * @param chunkS3Key - Full S3 key to chunk file (e.g., "chunks/person-full/.../chunk-0000.ndjson")
   * @param region - AWS region
   */
  public async readFlagsFromChunkKey(
    bucketName: string | undefined,
    chunkS3Key: string,
    region?: string
  ): Promise<Partial<Flags>> {
    // Derive chunk directory from chunk key
    // "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0000.ndjson" 
    // -> "chunks/person-full/2026-03-03T19:58:41.277Z"
    const chunkDirectory = chunkS3Key.substring(0, chunkS3Key.lastIndexOf('/'));
    
    return this.readFlags({ bucketName, chunkDirectory, region });
  }

  /**
   * Read metadata from a chunk file S3 key by deriving the chunk directory
   * @param bucketName - S3 bucket name
   * @param chunkS3Key - Full S3 key to chunk file (e.g., "chunks/person-full/.../chunk-0000.ndjson")
   * @param region - AWS region
   */
  public async readFromChunkKey(
    bucketName: string | undefined,
    chunkS3Key: string,
    region?: string
  ): Promise<Partial<ChunkMetadata>> {
    // Derive chunk directory from chunk key
    // "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0000.ndjson" 
    // -> "chunks/person-full/2026-03-03T19:58:41.277Z"
    const chunkDirectory = chunkS3Key.substring(0, chunkS3Key.lastIndexOf('/'));
    
    return this.read({ bucketName, chunkDirectory, region });
  }

  /**
   * List all chunk files in a directory with pagination support.
   * Returns sorted array of chunk file keys matching chunk-*.ndjson pattern.
   * Handles S3 pagination to support 1000+ chunk files.
   * 
   * @param bucketName - S3 bucket name
   * @param chunkDirectory - Chunk directory path (e.g., "chunks/person-full/2026-03-03T19:58:41.277Z")
   * @param region - AWS region
   * @returns Array of chunk file keys sorted by chunk number
   */
  public async listChunkFiles(
    bucketName: string | undefined,
    chunkDirectory: string,
    region?: string
  ): Promise<string[]> {
    if (!bucketName) {
      throw new Error('bucketName is required for S3 metadata operations');
    }

    const s3Client = new S3Client({ region });
    const prefix = `${chunkDirectory}/`;
    const chunkFiles: string[] = [];
    let continuationToken: string | undefined;

    try {
      do {
        const response = await s3Client.send(
          new ListObjectsV2Command({
            Bucket: bucketName,
            Prefix: prefix,
            ContinuationToken: continuationToken,
          })
        );

        // Filter for chunk-*.ndjson files
        if (response.Contents) {
          for (const obj of response.Contents) {
            if (obj.Key && /chunk-\d+\.ndjson$/.test(obj.Key)) {
              chunkFiles.push(obj.Key);
            }
          }
        }

        // Handle pagination
        if (response.IsTruncated) {
          continuationToken = response.NextContinuationToken;
        } else {
          continuationToken = undefined;
        }
      } while (continuationToken);

      // Sort by chunk number to ensure deterministic ordering
      chunkFiles.sort((a, b) => {
        const numA = parseInt(a.match(/chunk-(\d+)\.ndjson$/)?.[1] || '0', 10);
        const numB = parseInt(b.match(/chunk-(\d+)\.ndjson$/)?.[1] || '0', 10);
        return numA - numB;
      });

      console.log(`✓ Listed ${chunkFiles.length} chunk files from s3://${bucketName}/${prefix}`);
      return chunkFiles;
    } catch (error: any) {
      console.error(`Error listing chunk files: ${error.message}`);
      throw error;
    }
  }

  /**
   * Compute total records by summing NDJSON line counts across chunk files.
   * Streams each file to avoid buffering large payloads.
   * 
   * @param bucketName - S3 bucket name
   * @param chunkKeys - Array of chunk file S3 keys
   * @param region - AWS region
   * @returns Total record count across all chunks
   */
  public async computeTotalRecords(
    bucketName: string | undefined,
    chunkKeys: string[],
    region?: string
  ): Promise<number> {
    if (!bucketName) {
      throw new Error('bucketName is required for S3 metadata operations');
    }

    const s3Client = new S3Client({ region });
    let totalRecords = 0;

    for (const chunkKey of chunkKeys) {
      try {
        const response = await s3Client.send(
          new GetObjectCommand({
            Bucket: bucketName,
            Key: chunkKey,
          })
        );

        const body = await response.Body?.transformToString();
        if (body) {
          // Count non-empty lines (each NDJSON line is one record)
          const lineCount = body.split('\n').filter(line => line.trim().length > 0).length;
          totalRecords += lineCount;
        }
      } catch (error: any) {
        console.warn(`Warning: Could not read chunk file ${chunkKey}: ${error.message}`);
        // Continue with other chunks even if one fails
      }
    }

    console.log(`✓ Computed total records: ${totalRecords} across ${chunkKeys.length} chunks`);
    return totalRecords;
  }

  /**
   * Build aggregated metadata from run-level S3 state.
   * Discovers all chunk files in the directory and computes aggregate totals.
   * Used when finalizing metadata at end-of-run to ensure all chunks are reflected.
   * 
   * @param bucketName - S3 bucket name
   * @param chunkDirectory - Chunk directory path
   * @param region - AWS region
   * @returns Aggregated metadata with full chunk list and totals
   */
  public async buildAggregatedMetadata(
    bucketName: string | undefined,
    chunkDirectory: string,
    region?: string
  ): Promise<{ chunkKeys: string[]; totalRecords: number; chunkCount: number }> {
    try {
      // Discover all chunk files
      const chunkKeys = await this.listChunkFiles(bucketName, chunkDirectory, region);

      if (chunkKeys.length === 0) {
        throw new Error(`No chunk files found in ${chunkDirectory}`);
      }

      // Compute total records
      const totalRecords = await this.computeTotalRecords(bucketName, chunkKeys, region);

      const chunkCount = chunkKeys.length;

      console.log(`✓ Aggregated metadata: chunkCount=${chunkCount}, totalRecords=${totalRecords}`);

      return { chunkKeys, totalRecords, chunkCount };
    } catch (error: any) {
      console.error(`Failed to build aggregated metadata: ${error.message}`);
      throw error;
    }
  }

  // ========================================
  // Static wrapper methods for backward compatibility
  // ========================================

  /**
   * Static wrapper for write() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async write(params: WriteMetadataParams): Promise<void> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.write(params);
  }

  /**
   * Static wrapper for writeFlags() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async writeFlags(params: WriteFlagsParams): Promise<void> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.writeFlags(params);
  }

  /**
   * Static wrapper for read() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async read(params: ReadMetadataParams): Promise<Partial<ChunkMetadata>> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.read(params);
  }

  /**
   * Static wrapper for readFlags() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async readFlags(params: ReadFlagsParams): Promise<Partial<Flags>> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.readFlags(params);
  }

  /**
   * Static wrapper for markRunFailed() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async markRunFailed(params: MarkRunFailedParams): Promise<void> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.markRunFailed(params);
  }

  /**
   * Static wrapper for isRunFailed() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async isRunFailed(params: ReadFlagsParams): Promise<boolean> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.isRunFailed(params);
  }

  /**
   * Static wrapper for terminalErrorExists() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async terminalErrorExists(params: ReadTerminalErrorParams): Promise<boolean> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.terminalErrorExists(params);
  }

  /**
   * Static wrapper for readTerminalError() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async readTerminalError(params: ReadTerminalErrorParams): Promise<TerminalError | undefined> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.readTerminalError(params);
  }

  /**
   * Static wrapper for readFlagsFromChunkKey() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async readFlagsFromChunkKey(
    bucketName: string | undefined,
    chunkS3Key: string,
    region?: string
  ): Promise<Partial<Flags>> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.readFlagsFromChunkKey(bucketName, chunkS3Key, region);
  }

  /**
   * Static wrapper for readFromChunkKey() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async readFromChunkKey(
    bucketName: string | undefined,
    chunkS3Key: string,
    region?: string
  ): Promise<Partial<ChunkMetadata>> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.readFromChunkKey(bucketName, chunkS3Key, region);
  }

  /**
   * Static wrapper for listChunkFiles() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async listChunkFiles(
    bucketName: string | undefined,
    chunkDirectory: string,
    region?: string
  ): Promise<string[]> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.listChunkFiles(bucketName, chunkDirectory, region);
  }

  /**
   * Static wrapper for computeTotalRecords() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async computeTotalRecords(
    bucketName: string | undefined,
    chunkKeys: string[],
    region?: string
  ): Promise<number> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.computeTotalRecords(bucketName, chunkKeys, region);
  }

  /**
   * Static wrapper for buildAggregatedMetadata() method.
   * Creates a temporary instance for backward compatibility with existing code.
   */
  public static async buildAggregatedMetadata(
    bucketName: string | undefined,
    chunkDirectory: string,
    region?: string
  ): Promise<{ chunkKeys: string[]; totalRecords: number; chunkCount: number }> {
    const instance = new MetadataForS3({ config: {} as Config });
    return instance.buildAggregatedMetadata(bucketName, chunkDirectory, region);
  }
}
