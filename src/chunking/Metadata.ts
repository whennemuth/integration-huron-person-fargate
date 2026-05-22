import { GetObjectCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { S3StorageAdapter } from '../storage/S3StorageAdapter';
import { SyncPopulation } from '../../docker/chunkTypes';
import { objectExistsInS3 } from '../Utils';

/**
 * Chunk Metadata Management
 * 
 * This module manages two types of metadata files used in the 3-phase ECS Fargate chunking pipeline:
 * 
 * ## 1. Flags File (_flags.json)
 * 
 * **Purpose:** Provides immediate access to sync configuration flags for processor tasks.
 * 
 * **Written:** By the chunker task (Phase 1) BEFORE chunking begins.
 * 
 * **Read by:** Processor tasks (Phase 2) which start processing chunks before chunking completes.
 * 
 * **Contains:** 
 * - `bulkReset` (boolean): Whether to force target system lookups for all records
 * - `syncPopulation` (SyncPopulation): Population type being synced (PersonFull vs PersonDelta)
 * 
 * **Why needed:** Solves timing race condition where processor tasks start before chunking completes
 * and need configuration flags immediately. Without this, processors would have to wait for the
 * full metadata file to be written at the end of chunking.
 * 
 * **Location:** `s3://{bucket}/chunks/{populationType}/{timestamp}/_flags.json`
 * 
 * ## 2. Metadata File (_metadata.json)
 * 
 * **Purpose:** Provides complete chunking results and coordinates merger trigger.
 * 
 * **Written:** By the chunker task (Phase 1) AFTER chunking completes.
 * 
 * **Read by:** Merger subscriber (Phase 3) to determine when all chunks have been processed
 * and merging can begin. The merger subscriber is triggered every time a delta chunk is written
 * to the bucket, and the information in the metadata file is used to determine if that was
 * the last chunk and the way is clear to start the merging phase.
 * 
 * **Contains:**
 * - All flags (bulkReset, syncPopulation)
 * - Chunking results (chunkCount, totalRecords, itemsPerChunk)
 * - File paths (chunkDirectory, deltaStoragePath, chunkKeys)
 * - Source/target information
 * - Timestamps
 * 
 * **Location:** `s3://{bucket}/chunks/{populationType}/{timestamp}/_metadata.json`
 * 
 * ## Pipeline Flow
 * 
 * 1. **Chunker (Phase 1):**
 *    - Writes _flags.json immediately
 *    - Creates chunk files (chunk-0000.ndjson, chunk-0001.ndjson, ...)
 *    - Writes _metadata.json after chunking completes
 * 
 * 2. **Processor (Phase 2):**
 *    - Reads _flags.json for sync configuration
 *    - Processes individual chunks in parallel
 *    - Creates marker files when complete
 * 
 * 3. **Merger (Phase 3):**
 *    - Reads _metadata.json to get expected chunk count
 *    - Waits for all processor marker files
 *    - Merges delta results when complete
 */

export type Flags = {
  bulkReset: boolean;
  trustPreviousStorage: boolean;
  syncPopulation: SyncPopulation;
  [key: string]: any; // Allow additional fields for flexibility
}

/**
 * Core metadata fields shared between input parameters and stored metadata.
 * These fields are always present when writing and expected when reading properly formed metadata.
 */
type CoreMetadataFields = Flags & {
  source: string;
  target?: string;
  chunkDirectory: string;
  chunkCount: number;
  chunkKeys: string[];
  itemsPerChunk: number;
  totalRecords: number;
};

/**
 * Stored metadata format - what gets persisted to S3.
 * This is the canonical definition used throughout the codebase.
 */
export type ChunkMetadata = CoreMetadataFields & {
  deltaStoragePath: string;
  createdAt: string;
};

/**
 * Input parameters for writing metadata.
 * Extends core fields with write-specific operational parameters.
 */
export type WriteMetadataParams = CoreMetadataFields & {
  bucketName: string;
  dryRun?: boolean;
  storage?: S3StorageAdapter;
  replace?: boolean; // Whether to replace existing metadata file if it exists (default: false)
  region?: string;
};

/**
 * Parameters for reading metadata
 */
export interface ReadMetadataParams {
  bucketName: string;
  chunkDirectory: string;
  region?: string;
}

/**
 * Parameters for writing flags
 */
export type WriteFlagsParams = Flags & {
  bucketName: string;
  chunkDirectory: string;
  dryRun?: boolean;
  storage?: S3StorageAdapter;
  replace?: boolean; // Whether to replace existing flags file if it exists (default: false)
  region?: string;
};

/**
 * Parameters for reading flags
 */
export interface ReadFlagsParams {
  bucketName: string;
  chunkDirectory: string;
  region?: string;
}

/**
 * Centralized manager for chunk metadata operations.
 */
export class MetadataManager {
  private static readonly METADATA_FILENAME = '_metadata.json';
  private static readonly FLAGS_FILENAME = '_flags.json';

  /**
   * Derive delta storage path from chunk directory
   * Example: "chunks/person-full/2026-03-03T19:58:41.277Z" -> "deltas/person-full/2026-03-03T19:58:41.277Z"
   */
  public static deriveDeltaStoragePath(chunkDirectory: string): string {
    return chunkDirectory.replace(/^chunks\//, 'deltas/');
  }

  /**
   * Derive chunk directory from delta storage path
   * Example: "deltas/person-full/2026-03-03T19:58:41.277Z" -> "chunks/person-full/2026-03-03T19:58:41.277Z"
   */
  public static deriveChunkDirectory(deltaStoragePath: string): string {
    return deltaStoragePath.replace(/^deltas\//, 'chunks/');
  }

  /**
   * Get metadata file key from chunk directory
   */
  public static getMetadataKey(chunkDirectory: string): string {
    return `${chunkDirectory}/${MetadataManager.METADATA_FILENAME}`;
  }

  /**
   * Get flags file key from chunk directory
   */
  public static getFlagsKey(chunkDirectory: string): string {
    return `${chunkDirectory}/${MetadataManager.FLAGS_FILENAME}`;
  }

  /**
   * Write chunk metadata to S3.
   * Replaces the sprawling writeMetadata function with cleaner parameter object.
   */
  public static async write(params: WriteMetadataParams): Promise<void> {
    const { 
      bucketName, chunkDirectory, chunkCount, totalRecords, itemsPerChunk, chunkKeys,
      source, target, bulkReset, trustPreviousStorage, syncPopulation, dryRun = false, storage, region, replace = false
    } = params;

    const metadataKey = MetadataManager.getMetadataKey(chunkDirectory);
    const deltaStoragePath = MetadataManager.deriveDeltaStoragePath(chunkDirectory);

    const metadata: ChunkMetadata = {
      chunkCount, totalRecords, itemsPerChunk, source, chunkDirectory,
      deltaStoragePath, bulkReset, trustPreviousStorage, syncPopulation, createdAt: new Date().toISOString(),
      chunkKeys
    };

    // Add optional target field
    if (target) {
      metadata.target = target;
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

      if (storage) {
        // Use provided storage adapter or create S3Client
        await storage.writeFile(metadataKey, metadataJson, 'application/json');
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
  public static async writeFlags(params: WriteFlagsParams): Promise<void> {
    const {
      bucketName, chunkDirectory, bulkReset, trustPreviousStorage, syncPopulation, dryRun = false, storage, region, replace = false
    } = params;

    const flagsKey = MetadataManager.getFlagsKey(chunkDirectory);
    const flags: Flags = {
      bulkReset,
      trustPreviousStorage,
      syncPopulation
    };

    const flagsJson = JSON.stringify(flags, null, 2);
    const flagsLog = `s3://${bucketName}/${flagsKey}`;

    if (dryRun) {
      console.log(`[DRY RUN] Would write flags to: ${flagsLog}`);
      console.log(`[DRY RUN] Content: ${flagsJson}`);
    } else {
      // Use provided storage adapter or create S3Client
      if (storage) {
        await storage.writeFile(flagsKey, flagsJson, 'application/json');
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
  public static async read(params: ReadMetadataParams): Promise<Partial<ChunkMetadata>> {
    const { bucketName, chunkDirectory, region } = params;
    
    if (!bucketName) {
      console.error('Missing required bucket name');
      return {};
    }

    const metadataKey = MetadataManager.getMetadataKey(chunkDirectory);
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
      MetadataManager.validateMetadata(metadata);
      
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
  public static async readFlags(params: ReadFlagsParams): Promise<Partial<Flags>> {
    const { bucketName, chunkDirectory, region } = params;
    
    if (!bucketName) {
      console.error('Missing required bucket name');
      return {};
    }

    const flagsKey = MetadataManager.getFlagsKey(chunkDirectory);
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
   * Read flags from a chunk file S3 key by deriving the chunk directory
   * @param bucketName - S3 bucket name
   * @param chunkS3Key - Full S3 key to chunk file (e.g., "chunks/person-full/.../chunk-0000.ndjson")
   * @param region - AWS region
   */
  public static async readFlagsFromChunkKey(
    bucketName: string,
    chunkS3Key: string,
    region?: string
  ): Promise<Partial<Flags>> {
    // Derive chunk directory from chunk key
    // "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0000.ndjson" 
    // -> "chunks/person-full/2026-03-03T19:58:41.277Z"
    const chunkDirectory = chunkS3Key.substring(0, chunkS3Key.lastIndexOf('/'));
    
    return MetadataManager.readFlags({ bucketName, chunkDirectory, region });
  }

  /**
   * Validate metadata and log warnings for missing fields
   */
  private static validateMetadata(metadata: Partial<ChunkMetadata>): void {
    const requiredFields: (keyof ChunkMetadata)[] = [
      'bulkReset', 'chunkCount', 'totalRecords', 'deltaStoragePath', 'syncPopulation'
    ];

    for (const field of requiredFields) {
      if (metadata[field] === undefined) {
        const defaultValue = field === 'syncPopulation' 
          ? SyncPopulation.PersonFull 
          : field === 'bulkReset' 
            ? false 
            : 0;
        console.warn(`⚠️ ${field} value not found in metadata, defaulting to ${defaultValue}`);
      }
    }
  }

  /**
   * Read metadata from a chunk file S3 key by deriving the chunk directory
   * @param bucketName - S3 bucket name
   * @param chunkS3Key - Full S3 key to chunk file (e.g., "chunks/person-full/.../chunk-0000.ndjson")
   * @param region - AWS region
   */
  public static async readFromChunkKey(
    bucketName: string,
    chunkS3Key: string,
    region?: string
  ): Promise<Partial<ChunkMetadata>> {
    // Derive chunk directory from chunk key
    // "chunks/person-full/2026-03-03T19:58:41.277Z/chunk-0000.ndjson" 
    // -> "chunks/person-full/2026-03-03T19:58:41.277Z"
    const chunkDirectory = chunkS3Key.substring(0, chunkS3Key.lastIndexOf('/'));
    
    return MetadataManager.read({ bucketName, chunkDirectory, region });
  }
}