import { Config } from 'integration-huron-person';
import { StatisticsTable } from '../../dynamodb/StatisticsTable';
import { IContext } from '../../../context/IContext';
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
 * DynamoDB-based metadata management implementation.
 * 
 * Stores metadata as records in StatisticsTable with different eventType values:
 * - eventType="METADATA": Run manifest and configuration
 * - eventType="FLAGS": Sync configuration flags
 * - eventType="TERMINAL_ERROR": Terminal error marker
 * 
 * ## Record Structure
 * All records use:
 * - PK: syncRunId (ISO timestamp extracted from chunkDirectory)
 * - SK: eventType ("METADATA" | "FLAGS" | "TERMINAL_ERROR")
 * - Additional fields: Varies by eventType
 * 
 * ## Design Notes
 * 
 * Unlike S3 implementation which stores chunk files in the storage system,
 * DynamoDB implementation only stores metadata. The actual chunk files and
 * delta storage still use S3/file system.
 * 
 * Therefore, methods like listChunkFiles(), computeTotalRecords(), and
 * buildAggregatedMetadata() are NOT IMPLEMENTED because they require
 * filesystem access which DynamoDB doesn't provide.
 * 
 * ## Usage:
 * ```typescript
 * const metadata = new MetadataForDynamoDb({ config, context });
 * await metadata.writeFlags({ chunkDirectory, ...flags });
 * await metadata.write({ chunkDirectory, itemsPerChunk, source, ...flags });
 * ```
 */
export class MetadataForDynamoDb extends AbstractMetadata {
  private statisticsTable: StatisticsTable;

  constructor(params: { config: Config; context: IContext }) {
    super(params);
    this.statisticsTable = new StatisticsTable(params.context);
  }

  /**
   * Write chunk metadata to DynamoDB.
   * Creates a record with PK=syncRunId, SK="METADATA".
   */
  public async write(params: WriteMetadataParams): Promise<void> {
    const { 
      chunkDirectory, itemsPerChunk,
      source,
      target,
      bulkReset,
      trustPreviousStorage,
      syncPopulation,
      runFailed,
      runFailureMessage,
      runFailureTimestamp,
      dryRun = false,
      replace = false
    } = params;

    const syncRunId = AbstractMetadata.extractSyncRunId(chunkDirectory);
    const deltaStoragePath = AbstractMetadata.deriveDeltaStoragePath(chunkDirectory);

    const metadata: ChunkMetadata = {
      itemsPerChunk, source, chunkDirectory,
      deltaStoragePath, bulkReset, trustPreviousStorage, syncPopulation, 
      createdAt: new Date().toISOString()
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

    if (dryRun) {
      console.log(`[DRY RUN] Would write metadata to DynamoDB:`);
      console.log(`[DRY RUN] syncRunId: ${syncRunId}`);
      console.log(`[DRY RUN] Content: ${JSON.stringify(metadata, null, 2)}`);
      return;
    }

    // Check if metadata already exists (unless replace=true)
    if (!replace) {
      const existing = await this.statisticsTable.readMetadata(syncRunId);
      if (existing) {
        console.warn(`Metadata already exists for syncRunId ${syncRunId}. Skipping write.`);
        return;
      }
    }

    await this.statisticsTable.writeMetadata(syncRunId, metadata);
    console.log(`\n✓ Metadata written to DynamoDB: syncRunId=${syncRunId}`);
  }

  /**
   * Write flags to DynamoDB before chunking starts.
   * Creates a record with PK=syncRunId, SK="FLAGS".
   */
  public async writeFlags(params: WriteFlagsParams): Promise<void> {
    const {
      chunkDirectory,
      bulkReset,
      trustPreviousStorage,
      syncPopulation,
      runFailed,
      runFailureMessage,
      runFailureTimestamp,
      dryRun = false,
      replace = false
    } = params;

    const syncRunId = AbstractMetadata.extractSyncRunId(chunkDirectory);

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

    if (dryRun) {
      console.log(`[DRY RUN] Would write flags to DynamoDB:`);
      console.log(`[DRY RUN] syncRunId: ${syncRunId}`);
      console.log(`[DRY RUN] Content: ${JSON.stringify(flags, null, 2)}`);
      return;
    }

    // Check if flags already exist (unless replace=true)
    if (!replace) {
      const existing = await this.statisticsTable.readFlags(syncRunId);
      if (existing) {
        console.warn(`Flags already exist for syncRunId ${syncRunId}. Skipping write.`);
        return;
      }
    }

    await this.statisticsTable.writeFlags(syncRunId, flags);
    console.log(`✓ Flags written to DynamoDB: syncRunId=${syncRunId}`);
  }

  /**
   * Read chunk metadata from DynamoDB.
   */
  public async read(params: ReadMetadataParams): Promise<Partial<ChunkMetadata>> {
    const { chunkDirectory } = params;
    const syncRunId = AbstractMetadata.extractSyncRunId(chunkDirectory);

    try {
      console.log(`Reading metadata from DynamoDB: syncRunId=${syncRunId}`);
      
      const metadata = await this.statisticsTable.readMetadata(syncRunId);
      
      if (!metadata) {
        console.log(`No metadata found for syncRunId ${syncRunId}`);
        return {};
      }

      console.log(`✓ Metadata loaded: ${JSON.stringify(metadata)}`);
      
      // Log warnings for missing expected fields
      this.validateMetadata(metadata as ChunkMetadata);
      
      return metadata as Partial<ChunkMetadata>;
    } catch (error: any) {
      console.warn(`Warning: Could not read metadata: ${error.message}`);
      console.warn('Falling back to environment variables for configuration');
      return {};
    }
  }

  /**
   * Read flags from DynamoDB.
   */
  public async readFlags(params: ReadFlagsParams): Promise<Partial<Flags>> {
    const { chunkDirectory } = params;
    const syncRunId = AbstractMetadata.extractSyncRunId(chunkDirectory);

    try {
      console.log(`Reading flags from DynamoDB: syncRunId=${syncRunId}`);
      
      const flags = await this.statisticsTable.readFlags(syncRunId);
      
      if (!flags) {
        console.log(`No flags found for syncRunId ${syncRunId} (chunking may not have started yet)`);
        return {};
      }

      console.log(`✓ Flags loaded: ${JSON.stringify(flags)}`);
      
      return flags as Partial<Flags>;
    } catch (error: any) {
      console.warn(`Warning: Could not read flags: ${error.message}`);
      console.warn('Falling back to environment variables for configuration');
      return {};
    }
  }

  /**
   * Mark the chunking run as failed.
   * Creates a record with PK=syncRunId, SK="TERMINAL_ERROR".
   */
  public async markRunFailed(params: MarkRunFailedParams): Promise<void> {
    const { chunkDirectory, errorMessage } = params;
    const syncRunId = AbstractMetadata.extractSyncRunId(chunkDirectory);

    const marker: TerminalError = {
      stage: 'chunking',
      chunkDirectory,
      errorMessage,
      errorTimestamp: new Date().toISOString()
    };

    // Write terminal error as a separate record
    await this.statisticsTable.writeMetadata(syncRunId, {
      eventType: 'TERMINAL_ERROR',
      ...marker
    });

    console.error(`⛔ Terminal error marker written to DynamoDB: syncRunId=${syncRunId}`);
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
    const { chunkDirectory } = params;
    const syncRunId = AbstractMetadata.extractSyncRunId(chunkDirectory);

    try {
      const error = await this.readTerminalError(params);
      return error !== undefined;
    } catch (error: any) {
      return false;
    }
  }

  /**
   * Read terminal error marker details if present.
   */
  public async readTerminalError(params: ReadTerminalErrorParams): Promise<TerminalError | undefined> {
    const { chunkDirectory } = params;
    const syncRunId = AbstractMetadata.extractSyncRunId(chunkDirectory);

    try {
      // Query for TERMINAL_ERROR record
      const metadata = await this.statisticsTable.readMetadata(syncRunId);
      
      if (metadata && metadata.eventType === 'TERMINAL_ERROR') {
        return metadata as TerminalError;
      }
      
      return undefined;
    } catch (error: any) {
      console.warn(`Warning: Could not read terminal error: ${error.message}`);
      return undefined;
    }
  }

  /**
   * Read flags from a chunk file key by deriving the chunk directory
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
    
    return this.readFlags({ chunkDirectory });
  }

  /**
   * Read metadata from a chunk file key by deriving the chunk directory
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
    
    return this.read({ chunkDirectory });
  }

  /**
   * List all chunk files in a directory.
   * 
   * NOT IMPLEMENTED for DynamoDB storage because chunk files are stored in S3/file system,
   * not in DynamoDB. DynamoDB only stores metadata about the run.
   * 
   * If you need to list chunk files when using DynamoDB metadata storage, you must
   * still use S3/file system APIs to access the actual chunk files.
   * 
   * @throws Error indicating this operation is not supported
   */
  public async listChunkFiles(
    bucketName: string | undefined,
    chunkDirectory: string,
    region?: string
  ): Promise<string[]> {
    throw new Error(
      'listChunkFiles() is not implemented for DynamoDB metadata storage. ' +
      'Chunk files are stored in S3/file system. Use S3StorageAdapter or FileSystemStorageAdapter ' +
      'to list chunk files, or use MetadataForS3 for full S3-based metadata management.'
    );
  }

  /**
   * Compute total records by summing NDJSON line counts across chunk files.
   * 
   * NOT IMPLEMENTED for DynamoDB storage because chunk files are stored in S3/file system,
   * not in DynamoDB. DynamoDB only stores metadata about the run.
   * 
   * @throws Error indicating this operation is not supported
   */
  public async computeTotalRecords(
    bucketName: string | undefined,
    chunkKeys: string[],
    region?: string
  ): Promise<number> {
    throw new Error(
      'computeTotalRecords() is not implemented for DynamoDB metadata storage. ' +
      'Chunk files are stored in S3/file system. Use S3StorageAdapter or FileSystemStorageAdapter ' +
      'to read chunk files and compute totals.'
    );
  }

  /**
   * Build aggregated metadata from run-level storage state.
   * 
   * NOT IMPLEMENTED for DynamoDB storage because chunk files are stored in S3/file system,
   * not in DynamoDB. DynamoDB only stores metadata about the run.
   * 
   * @throws Error indicating this operation is not supported
   */
  public async buildAggregatedMetadata(
    bucketName: string | undefined,
    chunkDirectory: string,
    region?: string
  ): Promise<{ chunkKeys: string[]; totalRecords: number; chunkCount: number }> {
    throw new Error(
      'buildAggregatedMetadata() is not implemented for DynamoDB metadata storage. ' +
      'Chunk files are stored in S3/file system. Use S3StorageAdapter or FileSystemStorageAdapter ' +
      'to list chunk files and build aggregated metadata.'
    );
  }
}
