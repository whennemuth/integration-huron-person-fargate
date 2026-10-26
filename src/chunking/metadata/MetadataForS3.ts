import { GetObjectCommand, PutObjectCommand, DeleteObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Config } from 'integration-huron-person';
import { S3StorageAdapter } from '../../storage/S3StorageAdapter';
import { StatisticsTable } from '../../dynamodb/StatisticsTable';
import { objectExistsInS3 } from '../../Utils';
import {
  IMetadataStorage,
  AccumulateMetadataTotalsParams,
  ChunkMetadata,
  ClaimProcessorBoostParams,
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
  // The processor-boost claim (see claimProcessorBoost below) is routed through the DynamoDB
  // statistics table rather than S3 files - that table already exists in both storage modes (for
  // error tracking), so this reuses the same race-free OCCFlag-backed mechanism as DynamoDB mode
  // instead of maintaining a separate, non-atomic S3 implementation. (See TODO.md: this is the one
  // remaining S3-mode decision that depends on the statistics table.) Run totals are NOT kept there:
  // they live in _metadata.json, with only a best-effort copy sent to the table, as statistics.
  private statisticsTableName?: string;

  /** Attempts at the conditional read-modify-write of _metadata.json's run totals before giving up */
  private static readonly TOTALS_MAX_ATTEMPTS = 8;

  constructor(params: { config: Config; storage?: S3StorageAdapter; statisticsTableName?: string }) {
    this.config = params.config;
    this.storage = params.storage;
    this.statisticsTableName = params.statisticsTableName;
    this.metadataUtils = new StandardMetadataUtils({});
  }

  /**
   * Write chunk metadata to S3.
   * Persists the run manifest (flags, paths, timestamps) and aggregate data (chunkCount, totalRecords, chunkKeys).
   * Aggregate data is used by post-processor for completion validation to ensure all expected chunks are processed.
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

    // Add aggregate data for post-processor completion validation
    if (chunkCount !== undefined) {
      metadata.chunkCount = chunkCount;
    }
    if (totalRecords !== undefined) {
      metadata.totalRecords = totalRecords;
    }
    if (chunkKeys !== undefined) {
      metadata.chunkKeys = chunkKeys;
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
      personRecordProcessorCustomizations,
      termsEndpoint,
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
    if (termsEndpoint !== undefined) {
      flags.termsEndpoint = termsEndpoint;
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
      const response = await s3Client.send(
        new GetObjectCommand({
          Bucket: bucketName,
          Key: metadataKey
        })
      );

      const body = await response.Body?.transformToString();
      if (!body) {
        return {};
      }

      const metadata = JSON.parse(body) as ChunkMetadata;
      console.log(`✓ Metadata loaded: ${JSON.stringify(metadata)}`);
      
      // Log warnings for missing expected fields
      validateMetadata(metadata);
      
      return metadata;
    } catch (error: any) {
      if (error.name === 'NoSuchKey') {
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
   * This prevents post-processor orchestration from treating partial chunk output as successful completion.
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

  /**
   * Attempt to claim the processor-boost claim for this run. Delegates to the shared DynamoDB
   * statistics table (see the statisticsTableName field's doc comment) rather than an S3 file.
   */
  public async claimProcessorBoost(params: ClaimProcessorBoostParams): Promise<boolean> {
    const { chunkDirectory, region, claimedByChunk } = params;
    if (!this.statisticsTableName) {
      throw new Error('statisticsTableName is required for processor boost claims');
    }
    const syncRunId = this.metadataUtils.extractSyncRunId(chunkDirectory);
    const statisticsTable = StatisticsTable.fromTableName(this.statisticsTableName, region);
    return statisticsTable.claimProcessorBoost(syncRunId, claimedByChunk);
  }

  /**
   * Release a previously-won processor-boost claim for this run.
   */
  public async releaseProcessorBoostClaim(params: ClaimProcessorBoostParams): Promise<void> {
    const { chunkDirectory, region } = params;
    if (!this.statisticsTableName) {
      throw new Error('statisticsTableName is required for processor boost claims');
    }
    const syncRunId = this.metadataUtils.extractSyncRunId(chunkDirectory);
    const statisticsTable = StatisticsTable.fromTableName(this.statisticsTableName, region);
    await statisticsTable.releaseProcessorBoostClaim(syncRunId);
  }

  /**
   * Add this task's own chunkCount/totalRecords contribution to the run's totals in _metadata.json,
   * and record that the end of the source records was reached if this task reached it. These are
   * the values S3-mode decisions read back from _metadata.json (see read()): whether to chain
   * another chunker task, whether a late task should abandon its work, the total-records safety
   * cutoff, and the post-processor's expected chunk count.
   *
   * Parallel chunker tasks update the same file, so the update is a conditional read-modify-write:
   * the file is written back only if it is still the version that was read (IfMatch on its ETag),
   * or created only if it does not exist yet (IfNoneMatch '*'). A task that loses a race re-reads
   * and re-applies its contribution, so no task's totals are lost. Descriptive fields (source,
   * createdAt, ...) are set by whichever task writes first; partialOrEmptyChunkEncountered is only
   * ever set, never cleared.
   *
   * Afterwards, if a statistics table is configured, the same contribution is copied to it on a
   * best-effort basis, for reporting only - no S3-mode decision reads it from there.
   *
   * @returns The run's running totals after this task's contribution
   * @throws If the update keeps losing races after TOTALS_MAX_ATTEMPTS attempts
   */
  public async accumulateMetadataTotals(params: AccumulateMetadataTotalsParams): Promise<{ chunkCount: number; totalRecords: number }> {
    const {
      bucketName, chunkDirectory, region, source, target, itemsPerChunk, bulkReset, trustPreviousStorage,
      syncPopulation, chunkCountDelta, totalRecordsDelta, partialOrEmptyChunkEncountered
    } = params;
    if (!bucketName) {
      throw new Error('bucketName is required for S3 metadata operations');
    }
    const metadataKey = this.metadataUtils.getMetadataKey(chunkDirectory);
    const deltaStoragePath = this.metadataUtils.deriveDeltaStoragePath(chunkDirectory);
    const metadataLog = `s3://${bucketName}/${metadataKey}`;
    const s3Client = new S3Client({ region });
    const maxAttempts = MetadataForS3.TOTALS_MAX_ATTEMPTS;

    for (let attempt = 1; ; attempt++) {
      const { metadata: existing, etag } = await this.readMetadataWithETag(s3Client, bucketName, metadataKey);

      const updated: ChunkMetadata = {
        source, chunkDirectory, itemsPerChunk, bulkReset, trustPreviousStorage, syncPopulation, deltaStoragePath,
        createdAt: new Date().toISOString(),
        ...(target !== undefined ? { target } : {}),
        ...existing, // Fields already in the file win - they were set by whichever task wrote first
        chunkCount: (existing?.chunkCount ?? 0) + chunkCountDelta,
        totalRecords: (existing?.totalRecords ?? 0) + totalRecordsDelta
      } as ChunkMetadata;
      if (partialOrEmptyChunkEncountered) {
        updated.partialOrEmptyChunkEncountered = true;
      }

      try {
        await s3Client.send(new PutObjectCommand({
          Bucket: bucketName,
          Key: metadataKey,
          Body: JSON.stringify(updated, null, 2),
          ContentType: 'application/json',
          ...(etag ? { IfMatch: etag } : { IfNoneMatch: '*' })
        }));
        console.log(`✓ Metadata totals updated (attempt ${attempt}): ${metadataLog} ` +
          `[chunkCount=${updated.chunkCount}, totalRecords=${updated.totalRecords}` +
          `${updated.partialOrEmptyChunkEncountered ? ', partialOrEmptyChunkEncountered=true' : ''}]`);

        await this.copyTotalsToStatistics(params, deltaStoragePath);
        return { chunkCount: updated.chunkCount!, totalRecords: updated.totalRecords! };
      }
      catch (error: any) {
        if (!isConditionalWriteConflict(error)) {
          throw error;
        }
        if (attempt >= maxAttempts) {
          throw new Error(`Could not update run totals in ${metadataLog}: it kept changing under ` +
            `this task's conditional write (${maxAttempts} attempts)`);
        }
        const delayMs = Math.min(100 * 2 ** (attempt - 1), 3000) + Math.floor(Math.random() * 100);
        console.log(`Metadata totals write lost a race with another task (attempt ${attempt}) - retrying in ${delayMs}ms`);
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }

  /**
   * Read _metadata.json together with its ETag, for a conditional write back.
   * @returns metadata and etag both undefined if the file does not exist yet
   */
  private readMetadataWithETag = async (s3Client: S3Client, bucketName: string, metadataKey: string):
    Promise<{ metadata?: Partial<ChunkMetadata>, etag?: string }> => {
    try {
      const response = await s3Client.send(new GetObjectCommand({ Bucket: bucketName, Key: metadataKey }));
      const body = await response.Body?.transformToString();
      return { metadata: body ? JSON.parse(body) as Partial<ChunkMetadata> : {}, etag: response.ETag };
    }
    catch (error: any) {
      if (error.name === 'NoSuchKey') {
        return {};
      }
      throw error;
    }
  }

  /**
   * Best-effort copy of this task's totals contribution (and the run's RUN marker) to the statistics
   * table, for reporting. Never throws: statistics must not affect how the sync runs.
   */
  private copyTotalsToStatistics = async (params: AccumulateMetadataTotalsParams, deltaStoragePath: string): Promise<void> => {
    if (!this.statisticsTableName) {
      return;
    }
    const {
      chunkDirectory, region, source, target, itemsPerChunk, bulkReset, trustPreviousStorage,
      syncPopulation, chunkCountDelta, totalRecordsDelta, partialOrEmptyChunkEncountered
    } = params;
    try {
      const syncRunId = this.metadataUtils.extractSyncRunId(chunkDirectory);
      await StatisticsTable.fromTableName(this.statisticsTableName, region).addToMetadataTotals(syncRunId, {
        source, target, chunkDirectory, itemsPerChunk, bulkReset, trustPreviousStorage,
        syncPopulation, deltaStoragePath, chunkCountDelta, totalRecordsDelta, partialOrEmptyChunkEncountered
      });
    }
    catch (error: any) {
      console.warn(`Could not copy run totals to the statistics table (reporting only, sync unaffected): ${error?.message ?? error}`);
    }
  }
}

/**
 * True if an S3 conditional write failed because the object changed (or appeared, or is being
 * written) since it was read: 412 PreconditionFailed, or 409 ConditionalRequestConflict.
 */
const isConditionalWriteConflict = (error: any): boolean => {
  const status = error?.$metadata?.httpStatusCode;
  return error?.name === 'PreconditionFailed' || error?.name === 'ConditionalRequestConflict'
    || status === 412 || status === 409;
}
