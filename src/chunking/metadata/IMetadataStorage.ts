import { SyncPopulation } from '../../../docker/chunkTypes';

/**
 * Flags configuration for chunking pipeline.
 * 
 * These flags control sync behavior and are written early (before chunking starts)
 * so processors can read them immediately.
 */
export type Flags = {
  bulkReset: boolean;
  trustPreviousStorage: boolean;
  syncPopulation: SyncPopulation;
  runFailed?: boolean;
  runFailureMessage?: string;
  runFailureTimestamp?: string;
  /** Mock target configuration - when present, processors use MockPersonDataTarget instead of real target API */
  useMockTarget?: boolean;
  /** Validation-only mode for mock target: log operations but don't execute */
  mockTargetValidateOnly?: boolean;
  /**
   * Comma-delimited list of personRecordProcessor Customization enum keys (see
   * src/processing/custom/AbstractCustomPersonProcessor.ts) to activate for this run, e.g.
   * 'ORG_COMPARISON_LOGGING' or 'ORG_COMPARISON_LOGGING,SOME_OTHER_CUSTOMIZATION'. Propagated
   * from the Runner/EventBridge schedule through to processor tasks via this Flags record
   * (not baked into task definition environment variables), so it can be changed per-run
   * without a stack redeploy.
   */
  personRecordProcessorCustomizations?: string;
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
 * 
 * Aggregate fields usage:
 * - chunkCount: REQUIRED by merger for completion validation (ensures all expected chunks are processed)
 * - totalRecords: Used for statistics and audit trail
 * - chunkKeys: Used for debugging and audit trail (not used by merger for validation)
 */
export type ChunkMetadata = CoreMetadataFields & {
  deltaStoragePath: string;
  createdAt: string;
  chunkCount?: number;
  totalRecords?: number;
  chunkKeys?: string[];
};

/**
 * Input parameters for writing metadata.
 * Extends core fields with write-specific operational parameters.
 * 
 * Aggregate values usage:
 * - chunkCount: Should be provided for merger completion validation
 * - totalRecords: Optional, used for statistics and audit trail
 * - chunkKeys: Optional, used for debugging and audit trail
 */
export type WriteMetadataParams = CoreMetadataFields & {
  bucketName?: string;
  dryRun?: boolean;
  replace?: boolean; // Whether to replace existing metadata file if it exists (default: false)
  region?: string;
  chunkCount?: number;
  totalRecords?: number;
  chunkKeys?: string[];
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

/**
 * Parameters for marking a run as failed
 */
export interface MarkRunFailedParams {
  bucketName?: string;
  chunkDirectory: string;
  region?: string;
  errorMessage?: string;
}

/**
 * Parameters for reading terminal error marker
 */
export interface ReadTerminalErrorParams {
  bucketName?: string;
  chunkDirectory: string;
  region?: string;
}

/**
 * Terminal error marker structure
 */
export type TerminalError = {
  stage: 'chunking';
  chunkDirectory: string;
  errorMessage?: string;
  errorTimestamp: string;
};


/**
 * IMetadataStorage - Core metadata storage operations interface
 * 
 * Defines truly common metadata operations that both S3 and DynamoDB implementations support.
 * This interface excludes chunk file management operations (listChunkFiles, computeTotalRecords,
 * buildAggregatedMetadata) which are S3-specific concerns handled by ChunkFileManager.
 * 
 * ## Design Rationale
 * 
 * Previously, AbstractMetadata forced both implementations to provide chunk file operations
 * that only made sense for S3. This led to "NOT IMPLEMENTED" errors in MetadataForDynamoDb
 * for operations that required direct filesystem access.
 * 
 * The split recognizes that:
 * - **Metadata Storage**: Storage-agnostic (FLAGS, METADATA, TERMINAL_ERROR records)
 * - **Chunk File Management**: Always S3-based (listing/reading actual chunk files)
 * 
 * Both S3 and DynamoDB modes store chunk files in S3, but only differ in where they
 * store metadata about those chunks.
 * 
 * @see ChunkFileManager for S3 chunk file operations
 * @see MetadataForS3 for S3 implementation
 * @see MetadataForDynamoDb for DynamoDB implementation
 */
export interface IMetadataStorage {
  /**
   * Write chunk metadata to storage.
   * Persists only the run manifest (flags, paths, timestamps).
   * Parameters like chunkCount, totalRecords, chunkKeys are accepted for caller convenience but NOT persisted.
   */
  write(params: WriteMetadataParams): Promise<void>;

  /**
   * Write flags to storage before chunking starts.
   * This file contains only bulkReset and syncPopulation flags needed by processors.
   * Written early so processor tasks can read flags even before chunking completes.
   */
  writeFlags(params: WriteFlagsParams): Promise<void>;

  /**
   * Read chunk metadata from storage.
   */
  read(params: ReadMetadataParams): Promise<Partial<ChunkMetadata>>;

  /**
   * Read flags from storage.
   * Flags file contains bulkReset and syncPopulation needed by processors.
   */
  readFlags(params: ReadFlagsParams): Promise<Partial<Flags>>;

  /**
   * Mark the chunking run as failed.
   * This prevents merger orchestration from treating partial chunk output as successful completion.
   */
  markRunFailed(params: MarkRunFailedParams): Promise<void>;

  /**
   * Determine whether the run has been explicitly marked as failed.
   */
  isRunFailed(params: ReadFlagsParams): Promise<boolean>;

  /**
   * Check whether a terminal error marker exists for the run.
   */
  terminalErrorExists(params: ReadTerminalErrorParams): Promise<boolean>;

  /**
   * Read terminal error marker details if present.
   */
  readTerminalError(params: ReadTerminalErrorParams): Promise<TerminalError | undefined>;

  /**
   * Read flags from a chunk file key by deriving the chunk directory.
   * 
   * @param bucketName - S3 bucket name (optional for DynamoDB)
   * @param chunkS3Key - Full S3 key to chunk file (e.g., "chunks/person-full/.../chunk-0000.ndjson")
   * @param region - AWS region
   */
  readFlagsFromChunkKey(
    bucketName: string | undefined,
    chunkS3Key: string,
    region?: string
  ): Promise<Partial<Flags>>;

  /**
   * Read metadata from a chunk file key by deriving the chunk directory.
   * 
   * @param bucketName - S3 bucket name (optional for DynamoDB)
   * @param chunkS3Key - Full S3 key to chunk file (e.g., "chunks/person-full/.../chunk-0000.ndjson")
   * @param region - AWS region
   */
  readFromChunkKey(
    bucketName: string | undefined,
    chunkS3Key: string,
    region?: string
  ): Promise<Partial<ChunkMetadata>>;
}
