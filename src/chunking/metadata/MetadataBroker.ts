import { Config } from 'integration-huron-person';
import { IMetadataStorage, MarkRunFailedParams, ReadMetadataParams, WriteFlagsParams, WriteMetadataParams } from './IMetadataStorage';
import { MetadataFactory } from './MetadataFactory';

export type MetadataBrokerParams = {
  config: Config;
  bucketName: string;
  chunkDirectory: string;
  region: string | undefined;
  /** Route to the isolated mock statistics table when true (DynamoDB mode) */
  useMockTarget?: boolean;
};

/**
 * Centralizes metadata read/write/status-check operations for a single chunk directory.
 *
 * Extracted out of docker/chunker.ts (where these previously lived as free functions) so that
 * src/chunking/fetch/ChunkFromAPI.ts and src/chunking/filedrop/ChunkFromS3.ts - both of which
 * docker/chunker.ts itself imports - no longer need to import back from docker/chunker.ts,
 * avoiding a mutual-import relationship between those modules.
 *
 * Holds a single IMetadataStorage instance (created via MetadataFactory) plus the
 * bucketName/chunkDirectory/region/useMockTarget context shared by all reads/writes for one
 * chunk directory, so callers stop repeating those on every call.
 */
export class MetadataBroker {
  private readonly metadata: IMetadataStorage;
  private readonly bucketName: string;
  private readonly chunkDirectory: string;
  private readonly region: string | undefined;

  constructor(params: MetadataBrokerParams) {
    const { config, bucketName, chunkDirectory, region, useMockTarget = false } = params;
    this.bucketName = bucketName;
    this.chunkDirectory = chunkDirectory;
    this.region = region;
    this.metadata = MetadataFactory.create({
      config,
      previousStorageType: process.env.PREVIOUS_STORAGE_TYPE,
      statisticsTableName: useMockTarget
        ? process.env.DYNAMODB_MOCK_STATISTICS_TABLE_NAME
        : process.env.DYNAMODB_STATISTICS_TABLE_NAME
    });
  }

  /** Escape hatch for IMetadataStorage methods not wrapped below (e.g. readFlags). */
  public get storage(): IMetadataStorage {
    return this.metadata;
  }

  public async write(params: WriteMetadataParams): Promise<void> {
    await this.metadata.write(params);
  }

  public async writeFlags(params: WriteFlagsParams): Promise<void> {
    await this.metadata.writeFlags(params);
  }

  public async markRunFailed(params: MarkRunFailedParams): Promise<void> {
    await this.metadata.markRunFailed(params);
  }

  /**
   * Bail out if this is an extraneous task where the end of chunking was reached after its SQS
   * message was created. Presence of the metadata record indicates that the chunking process had
   * already completed and the service had already "realized" it had reached the end and scaled
   * down, but due to the asynchronous nature of SQS and scaling, we may have some tasks that were
   * triggered by messages that were created before the service realized it had reached the end,
   * and these tasks should just exit immediately without doing any work.
   *
   * When metadata carries finalOffsetProcessed (the offset of the last real page fetched by
   * whichever task discovered the end), currentOffset is compared against it so tasks whose own
   * offset window is still legitimately below that boundary are NOT aborted (they proceed and
   * fill in what would otherwise be a silently-skipped gap) - only tasks strictly beyond it are
   * considered finished. Falls back to the old blanket abort when finalOffsetProcessed or
   * currentOffset is unavailable.
   * @param currentOffset This task's own starting offset (from ChunkFromAPI), if applicable
   * @returns true if this task should abort, false otherwise
   */
  public async isAlreadyFinished(currentOffset?: number): Promise<boolean> {
    const { bucketName, chunkDirectory, region } = this;
    const result = await this.metadata.read({ bucketName, chunkDirectory, region } satisfies ReadMetadataParams);

    if (!result || Object.keys(result).length === 0) {
      return false;
    }

    console.log(`🔍 Existing metadata found for this chunk directory: ${JSON.stringify(result)}`);

    const { finalOffsetProcessed } = result;
    if (finalOffsetProcessed === undefined || currentOffset === undefined) {
      return true;
    }

    const stillLegitimate = currentOffset <= finalOffsetProcessed;
    if (stillLegitimate) {
      console.log(`ℹ️  currentOffset=${currentOffset} is at or below finalOffsetProcessed=${finalOffsetProcessed} - proceeding instead of aborting.`);
    }
    return !stillLegitimate;
  }

  /**
   * Bail out early if a terminal chunking error marker exists for this run.
   * The marker itself provides at-a-glance failure visibility.
   */
  public async isTerminalErrorEncountered(): Promise<boolean> {
    const { bucketName, chunkDirectory, region } = this;
    return this.metadata.terminalErrorExists({ bucketName, chunkDirectory, region });
  }

  /**
   * Read the finalOffsetProcessed boundary (if recorded) so callers can tell whether another
   * task already established the true end of the population before their own result arrived.
   * Returns undefined if no boundary is recorded yet.
   */
  public async getFinalOffsetProcessed(): Promise<number | undefined> {
    const { bucketName, chunkDirectory, region } = this;
    const result = await this.metadata.read({ bucketName, chunkDirectory, region } satisfies ReadMetadataParams);
    return result?.finalOffsetProcessed;
  }

  /**
   * Guard checked before each fetch iteration in ChunkFromAPI's batch loop: true if offset is
   * already past a finalOffsetProcessed boundary established by another parallel task, meaning
   * any data the API returns for it is untrustworthy and should be discarded as an API glitch.
   */
  public async isOffsetPastKnownEnd(offset: number): Promise<boolean> {
    const finalOffsetProcessed = await this.getFinalOffsetProcessed();
    return finalOffsetProcessed !== undefined && offset > finalOffsetProcessed;
  }

  /**
   * Attempt to claim exclusive right to perform the processor-boost check/delay/scale-up for
   * this run (ProcessorServiceBooster). See ClaimProcessorBoostParams for why this is a
   * dedicated record rather than a Flags/ChunkMetadata field.
   */
  public async claimProcessorBoost(claimedByChunk?: string): Promise<boolean> {
    const { bucketName, chunkDirectory, region } = this;
    return this.metadata.claimProcessorBoost({ bucketName, chunkDirectory, region, claimedByChunk });
  }

  /**
   * Release a previously-won processor-boost claim, allowing another task to win it later.
   */
  public async releaseProcessorBoostClaim(): Promise<void> {
    const { bucketName, chunkDirectory, region } = this;
    await this.metadata.releaseProcessorBoostClaim({ bucketName, chunkDirectory, region });
  }
}
