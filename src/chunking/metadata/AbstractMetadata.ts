import { Config } from 'integration-huron-person';
import { SyncPopulation } from '../../../docker/chunkTypes';

/**
 * Chunk Metadata Management - Abstract Base Class
 * 
 * This module manages two types of metadata files used in the 3-phase ECS Fargate chunking pipeline:
 * 
 * ## 1. Flags File (_flags.json in S3 / FLAGS record in DynamoDB)
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
 * **Storage:**
 * - S3: `s3://{bucket}/chunks/{populationType}/{timestamp}/_flags.json`
 * - DynamoDB: PK=syncRunId, SK="FLAGS"
 * 
 * ## 2. Metadata File (_metadata.json in S3 / METADATA record in DynamoDB)
 * 
 * **Purpose:** Provides run manifest and configuration for post-run diagnostics and audit.
 * Merger completion is now determined by contiguous processing-complete marker files, not metadata.
 * 
 * **Written:** By the chunker task (Phase 1) AFTER chunking completes.
 * 
 * **Read by:** Diagnostic/audit tools to understand run context and source/target configuration.
 * 
 * **Contains:**
 * - All flags (bulkReset, trustPreviousStorage, syncPopulation)
 * - File paths (chunkDirectory, deltaStoragePath)
 * - Source/target information
 * - Timestamps (createdAt)
 * - Optional: itemsPerChunk for reference
 * 
 * **Does NOT contain (informational only via logs):**
 * - chunkCount (determined by contiguous marker ordinals 0..N)
 * - totalRecords (computed from marker presence, not persisted)
 * - chunkKeys (full list determined by marker file enumeration)
 * 
 * **Storage:**
 * - S3: `s3://{bucket}/chunks/{populationType}/{timestamp}/_metadata.json`
 * - DynamoDB: PK=syncRunId, SK="METADATA"
 * 
 * ## 3. Terminal Error Marker (_terminal_error.json in S3 / TERMINAL_ERROR record in DynamoDB)
 * 
 * **Purpose:** Marks catastrophic failures that prevent normal pipeline completion.
 * 
 * **Storage:**
 * - S3: `s3://{bucket}/chunks/{populationType}/{timestamp}/_terminal_error.json`
 * - DynamoDB: PK=syncRunId, SK="TERMINAL_ERROR"
 * 
 * ## Pipeline Flow
 * 
 * 1. **Chunker (Phase 1):**
 *    - Writes flags immediately
 *    - Creates chunk files (chunk-0000.ndjson, chunk-0001.ndjson, ...)
 *    - Writes metadata after chunking completes
 * 
 * 2. **Processor (Phase 2):**
 *    - Reads flags for sync configuration
 *    - Processes individual chunks in parallel
 *    - Creates marker files when complete
 * 
 * 3. **Merger (Phase 3):**
 *    - Reads metadata to get expected chunk count
 *    - Waits for all processor marker files
 *    - Merges delta results when complete
 */

export type Flags = {
  bulkReset: boolean;
  trustPreviousStorage: boolean;
  syncPopulation: SyncPopulation;
  runFailed?: boolean;
  runFailureMessage?: string;
  runFailureTimestamp?: string;
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
  itemsPerChunk: number;
};

/**
 * Stored metadata format - what gets persisted to storage.
 * This is the canonical definition used throughout the codebase.
 */
export type ChunkMetadata = CoreMetadataFields & {
  deltaStoragePath: string;
  createdAt: string;
};

/**
 * Input parameters for writing metadata.
 * Extends core fields with write-specific operational parameters.
 * 
 * Note: chunkCount, totalRecords, and chunkKeys are NOT included here.
 * These are informational only and should be computed on-demand from marker files
 * rather than persisted. Callers should log these values separately if needed.
 */
export type WriteMetadataParams = CoreMetadataFields & {
  bucketName?: string;
  dryRun?: boolean;
  replace?: boolean; // Whether to replace existing metadata file if it exists (default: false)
  region?: string;
};

/**
 * Parameters for reading metadata
 */
export interface ReadMetadataParams {
  bucketName?: string;
  chunkDirectory: string;
  region?: string;
}

/**
 * Parameters for writing flags
 */
export type WriteFlagsParams = Flags & {
  bucketName?: string;
  chunkDirectory: string;
  dryRun?: boolean;
  replace?: boolean; // Whether to replace existing flags file if it exists (default: false)
  region?: string;
};

/**
 * Parameters for reading flags
 */
export interface ReadFlagsParams {
  bucketName?: string;
  chunkDirectory: string;
  region?: string;
}

export interface MarkRunFailedParams {
  bucketName?: string;
  chunkDirectory: string;
  region?: string;
  errorMessage?: string;
}

export interface ReadTerminalErrorParams {
  bucketName?: string;
  chunkDirectory: string;
  region?: string;
}

export type TerminalError = {
  stage: 'chunking';
  chunkDirectory: string;
  errorMessage?: string;
  errorTimestamp: string;
};

/**
 * Abstract base class for metadata management.
 * 
 * ## Design Pattern: Template Method
 * 
 * This abstract class defines the interface for metadata operations with storage-agnostic
 * path derivation logic as static methods and storage-specific operations as abstract
 * instance methods.
 * 
 * ## Implementations:
 * - **MetadataForS3**: Stores metadata as JSON files in S3
 * - **MetadataForDynamoDb**: Stores metadata as records in StatisticsTable
 * 
 * ## Usage:
 * ```typescript
 * const metadata = MetadataFactory.create(config);
 * 
 * // Write flags before chunking
 * await metadata.writeFlags({ 
 *   chunkDirectory, bulkReset, trustPreviousStorage, syncPopulation 
 * });
 * 
 * // Write metadata after chunking
 * await metadata.write({ 
 *   chunkDirectory, itemsPerChunk, source, target, ...flags 
 * });
 * 
 * // Read metadata
 * const meta = await metadata.read({ chunkDirectory });
 * const flags = await metadata.readFlags({ chunkDirectory });
 * ```
 */
export abstract class AbstractMetadata {
  protected config: Config;

  constructor(params: { config: Config }) {
    this.config = params.config;
  }

  // ============================================================================
  // Static path derivation methods (storage-agnostic)
  // ============================================================================

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
   * Get metadata file key from chunk directory (for S3) or event type (for DynamoDB)
   */
  public static getMetadataKey(chunkDirectory: string): string {
    return `${chunkDirectory}/_metadata.json`;
  }

  /**
   * Get flags file key from chunk directory (for S3) or event type (for DynamoDB)
   */
  public static getFlagsKey(chunkDirectory: string): string {
    return `${chunkDirectory}/_flags.json`;
  }

  /**
   * Get terminal error marker key from chunk directory (for S3) or event type (for DynamoDB)
   */
  public static getTerminalErrorKey(chunkDirectory: string): string {
    return `${chunkDirectory}/_terminal_error.json`;
  }

  /**
   * Extract syncRunId (ISO timestamp) from chunk directory.
   * Example: "chunks/person-full/2026-03-03T19:58:41.277Z" -> "2026-03-03T19:58:41.277Z"
   */
  public static extractSyncRunId(chunkDirectory: string): string {
    const parts = chunkDirectory.split('/');
    return parts[parts.length - 1];
  }

  // ============================================================================
  // Abstract instance methods (storage-specific)
  // ============================================================================

  /**
   * Write chunk metadata to storage.
   * Persists only the run manifest (flags, paths, timestamps).
   * Parameters like chunkCount, totalRecords, chunkKeys are accepted for caller convenience but NOT persisted.
   * These values are now determined from contiguous marker files and storage state, not from metadata.
   */
  public abstract write(params: WriteMetadataParams): Promise<void>;

  /**
   * Write flags to storage before chunking starts.
   * This file contains only bulkReset and syncPopulation flags needed by processors.
   * Written early so processor tasks can read flags even before chunking completes.
   */
  public abstract writeFlags(params: WriteFlagsParams): Promise<void>;

  /**
   * Read chunk metadata from storage.
   * Replaces getChunkMetadata and readChunkMetadata with unified implementation.
   */
  public abstract read(params: ReadMetadataParams): Promise<Partial<ChunkMetadata>>;

  /**
   * Read flags from storage.
   * Flags file contains bulkReset and syncPopulation needed by processors.
   */
  public abstract readFlags(params: ReadFlagsParams): Promise<Partial<Flags>>;

  /**
   * Mark the chunking run as failed.
   * This prevents merger orchestration from treating partial chunk output as successful completion.
   */
  public abstract markRunFailed(params: MarkRunFailedParams): Promise<void>;

  /**
   * Determine whether the run has been explicitly marked as failed.
   */
  public abstract isRunFailed(params: ReadFlagsParams): Promise<boolean>;

  /**
   * Check whether a terminal error marker exists for the run.
   */
  public abstract terminalErrorExists(params: ReadTerminalErrorParams): Promise<boolean>;

  /**
   * Read terminal error marker details if present.
   */
  public abstract readTerminalError(params: ReadTerminalErrorParams): Promise<TerminalError | undefined>;

  /**
   * Read flags from a chunk file key by deriving the chunk directory
   * @param bucketName - S3 bucket name (optional for DynamoDB)
   * @param chunkS3Key - Full S3 key to chunk file (e.g., "chunks/person-full/.../chunk-0000.ndjson")
   * @param region - AWS region
   */
  public abstract readFlagsFromChunkKey(
    bucketName: string | undefined,
    chunkS3Key: string,
    region?: string
  ): Promise<Partial<Flags>>;

  /**
   * Read metadata from a chunk file key by deriving the chunk directory
   * @param bucketName - S3 bucket name (optional for DynamoDB)
   * @param chunkS3Key - Full S3 key to chunk file (e.g., "chunks/person-full/.../chunk-0000.ndjson")
   * @param region - AWS region
   */
  public abstract readFromChunkKey(
    bucketName: string | undefined,
    chunkS3Key: string,
    region?: string
  ): Promise<Partial<ChunkMetadata>>;

  /**
   * List all chunk files in a directory with pagination support.
   * Returns sorted array of chunk file keys matching chunk-*.ndjson pattern.
   * Handles pagination to support 1000+ chunk files.
   * 
   * @param bucketName - S3 bucket name (optional for DynamoDB)
   * @param chunkDirectory - Chunk directory path (e.g., "chunks/person-full/2026-03-03T19:58:41.277Z")
   * @param region - AWS region
   * @returns Array of chunk file keys sorted by chunk number
   */
  public abstract listChunkFiles(
    bucketName: string | undefined,
    chunkDirectory: string,
    region?: string
  ): Promise<string[]>;

  /**
   * Compute total records by summing NDJSON line counts across chunk files.
   * Streams each file to avoid buffering large payloads.
   * 
   * @param bucketName - S3 bucket name (optional for DynamoDB)
   * @param chunkKeys - Array of chunk file S3 keys
   * @param region - AWS region
   * @returns Total record count across all chunks
   */
  public abstract computeTotalRecords(
    bucketName: string | undefined,
    chunkKeys: string[],
    region?: string
  ): Promise<number>;

  /**
   * Build aggregated metadata from run-level storage state.
   * Discovers all chunk files in the directory and computes aggregate totals.
   * Used when finalizing metadata at end-of-run to ensure all chunks are reflected.
   * 
   * @param bucketName - S3 bucket name (optional for DynamoDB)
   * @param chunkDirectory - Chunk directory path
   * @param region - AWS region
   * @returns Aggregated metadata with full chunk list and totals
   */
  public abstract buildAggregatedMetadata(
    bucketName: string | undefined,
    chunkDirectory: string,
    region?: string
  ): Promise<{ chunkKeys: string[]; totalRecords: number; chunkCount: number }>;

  /**
   * Validate metadata and log warnings for missing fields.
   * Note: chunkCount, totalRecords, chunkKeys are no longer persisted in metadata.
   * They are now determined from contiguous marker files.
   */
  protected validateMetadata(metadata: Partial<ChunkMetadata>): void {
    const requiredFields: (keyof ChunkMetadata)[] = [
      'bulkReset', 'deltaStoragePath', 'syncPopulation'
    ];

    for (const field of requiredFields) {
      if (metadata[field] === undefined) {
        const defaultValue = field === 'syncPopulation' 
          ? SyncPopulation.PersonFull 
          : false;
        console.warn(`⚠️ ${field} value not found in metadata, defaulting to ${defaultValue}`);
      }
    }
  }
}
