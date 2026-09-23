
  // ============================================================================
  // Storage-agnostic helper functions.
  // ============================================================================

  import { SyncPopulation } from '../../../docker/chunkTypes';
  import { ChunkMetadata } from './IMetadataStorage';

  /**
   * Validate metadata and log warnings for missing fields.
   * This is a utility function that can be called by both S3 and DynamoDB implementations.
   * 
   * Note: chunkCount, totalRecords, chunkKeys are no longer persisted in metadata.
   * They are now determined from contiguous marker files.
   */
  export function validateMetadata(metadata: Partial<ChunkMetadata>): void {
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

  export type MetadataUtils = {
    deriveDeltaStoragePath(chunkDirectory: string): string;
    deriveChunkDirectory(deltaStoragePath: string): string;
    getMetadataKey(chunkDirectory: string): string;
    getFlagsKey(chunkDirectory: string): string;
    getTerminalErrorKey(chunkDirectory: string): string;
    extractSyncRunId(chunkDirectory: string): string;
  };

  export class StandardMetadataUtils implements MetadataUtils {

    constructor(private params: { chunkDirectory?: string, deltaStoragePath?: string }) { }

    /**
     * Derive delta storage path from chunk directory
     * Example: "chunks/person-full/2026-03-03T19:58:41.277Z" -> "deltas/person-full/2026-03-03T19:58:41.277Z"
     */
    public deriveDeltaStoragePath(chunkDirectory?: string): string {
      chunkDirectory = chunkDirectory ?? this.params.chunkDirectory;
      if (!chunkDirectory) {
        throw new Error('chunkDirectory is required');
      }
      return chunkDirectory.replace(/^chunks\//, 'deltas/');
    }

    /**
     * Derive chunk directory from delta storage path
     * Example: "deltas/person-full/2026-03-03T19:58:41.277Z" -> "chunks/person-full/2026-03-03T19:58:41.277Z"
     */
    public deriveChunkDirectory(deltaStoragePath?: string): string {
      deltaStoragePath = deltaStoragePath ?? this.params.deltaStoragePath;
      if (!deltaStoragePath) {
        throw new Error('deltaStoragePath is required');
      }
      return deltaStoragePath.replace(/^deltas\//, 'chunks/');
    }

    /**
     * Get metadata file key from chunk directory (for S3) or event type (for DynamoDB)
     */
    public getMetadataKey(chunkDirectory?: string): string {
      chunkDirectory = chunkDirectory ?? this.params.chunkDirectory;
      if (!chunkDirectory) {
        throw new Error('chunkDirectory is required');
      }
      return `${chunkDirectory}/_metadata.json`;
    }

    /**
     * Get flags file key from chunk directory (for S3) or event type (for DynamoDB)
     */
    public getFlagsKey(chunkDirectory?: string): string {
      chunkDirectory = chunkDirectory ?? this.params.chunkDirectory;
      if (!chunkDirectory) {
        throw new Error('chunkDirectory is required');
      }
      return `${chunkDirectory}/_flags.json`;
    }

    /**
     * Get terminal error marker key from chunk directory (for S3) or event type (for DynamoDB)
     */
    public getTerminalErrorKey(chunkDirectory?: string): string {
      chunkDirectory = chunkDirectory ?? this.params.chunkDirectory;
      if (!chunkDirectory) {
        throw new Error('chunkDirectory is required');
      }
      return `${chunkDirectory}/_terminal_error.json`;
    }

    /**
     * Extract syncRunId (ISO timestamp) from chunk directory.
     * Example: "chunks/person-full/2026-03-03T19:58:41.277Z" -> "2026-03-03T19:58:41.277Z"
     */
    public extractSyncRunId(chunkDirectory?: string): string {
      chunkDirectory = chunkDirectory ?? this.params.chunkDirectory ?? '';
      const timestamp = this.extractIntegrationTimestamp(chunkDirectory);
      if (timestamp) {
        return timestamp;
      }
      if (!chunkDirectory) {
        throw new Error('chunkDirectory is required');
      }
      const parts = chunkDirectory.split('/');
      return parts[parts.length - 1];
    }

    /**
     * Extract integration timestamp from S3 key
     */
    public extractIntegrationTimestamp = (s3Key: string): string | undefined => {
      const match = s3Key.match(/\/(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)\//);
      return match ? match[1] : undefined;
    };

    /**
     * Extract chunk ID from S3 key
     */
    public extractChunkId = (s3Key: string): string | undefined => {
      const match = s3Key.match(/chunk-(\d+)\.ndjson$/);
      return match ? match[1] : undefined;
    };
  }

