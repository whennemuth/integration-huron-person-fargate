import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Config } from 'integration-huron-person';
import { S3StorageAdapter } from '../../storage/S3StorageAdapter';
import { objectExistsInS3 } from '../../Utils';
import {
  IMetadataStorage,
  ChunkMetadata,
  Flags,
  MarkRunFailedParams,
  ReadFlagsParams,
  ReadMetadataParams,
  ReadTerminalErrorParams,
  TerminalError,
  WriteFlagsParams,
  WriteMetadataParams
} from './IMetadataStorage';
import { StandardMetadataUtils, validateMetadata } from './MetadataUtils';

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
export class MetadataForS3 implements IMetadataStorage {
  protected config: Config;
  private storage?: S3StorageAdapter;
  private metadataUtils: StandardMetadataUtils;

  constructor(params: { config: Config; storage?: S3StorageAdapter }) {
    this.config = params.config;
    this.storage = params.storage;
    this.metadataUtils = new StandardMetadataUtils({});
  }

  /**
   * Write chunk metadata to S3.
   * Persists the run manifest (flags, paths, timestamps) and aggregate data (chunkCount, totalRecords, chunkKeys).
   * Aggregate data is used by merger for completion validation to ensure all expected chunks are processed.
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
      chunkCount,
      totalRecords,
      chunkKeys,
      finalOffsetProcessed,
      dryRun = false,
      region,
      replace = false
    } = params;

    if (!bucketName) {
      throw new Error('bucketName is required for S3 metadata operations');
    }

    const metadataKey = this.metadataUtils.getMetadataKey(chunkDirectory);
    const deltaStoragePath = this.metadataUtils.deriveDeltaStoragePath(chunkDirectory);

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

    // Add aggregate data for merger completion validation
    if (chunkCount !== undefined) {
      metadata.chunkCount = chunkCount;
    }
    if (totalRecords !== undefined) {
      metadata.totalRecords = totalRecords;
    }
    if (chunkKeys !== undefined) {
      metadata.chunkKeys = chunkKeys;
    }
    if (finalOffsetProcessed !== undefined) {
      metadata.finalOffsetProcessed = finalOffsetProcessed;
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
      useMockTarget,
      mockTargetValidateOnly,
      runFailed,
      runFailureMessage,
      runFailureTimestamp,
      personRecordProcessorCustomizations,
      dryRun = false,
      region,
      replace = false
    } = params;

    if (!bucketName) {
      throw new Error('bucketName is required for S3 metadata operations');
    }

    const flagsKey = this.metadataUtils.getFlagsKey(chunkDirectory);
    const flags: Flags = {
      bulkReset,
      trustPreviousStorage,
      syncPopulation
    };

    if (useMockTarget !== undefined) {
      flags.useMockTarget = useMockTarget;
    }
    if (mockTargetValidateOnly !== undefined) {
      flags.mockTargetValidateOnly = mockTargetValidateOnly;
    }
    if (runFailed !== undefined) {
      flags.runFailed = runFailed;
    }
    if (runFailureMessage !== undefined) {
      flags.runFailureMessage = runFailureMessage;
    }
    if (runFailureTimestamp !== undefined) {
      flags.runFailureTimestamp = runFailureTimestamp;
    }
    if (personRecordProcessorCustomizations !== undefined) {
      flags.personRecordProcessorCustomizations = personRecordProcessorCustomizations;
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

    const metadataKey = this.metadataUtils.getMetadataKey(chunkDirectory);
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
      validateMetadata(metadata);
      
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

    const flagsKey = this.metadataUtils.getFlagsKey(chunkDirectory);
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

    const markerKey = this.metadataUtils.getTerminalErrorKey(chunkDirectory);
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

    const key = this.metadataUtils.getTerminalErrorKey(chunkDirectory);
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

    const key = this.metadataUtils.getTerminalErrorKey(chunkDirectory);
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
}
