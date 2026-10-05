import { Config } from 'integration-huron-person';
import { SyncPopulation } from '../../../docker/chunkTypes';
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
   * True once any parallel chunker task has encountered a partial-or-empty batch. The source
   * queue only depletes over a run's life (never refills), so this is a reliable "nothing
   * meaningful left" signal regardless of which task set it - shared by isAlreadyFinished()
   * (should a late-arriving task abort) and chain-stop decisions (should the next task be
   * chained at all).
   */
  public async hasAnyTaskEncounteredPartial(): Promise<boolean> {
    const { bucketName, chunkDirectory, region } = this;
    const result = await this.metadata.read({ bucketName, chunkDirectory, region } satisfies ReadMetadataParams);
    return result?.partialOrEmptyChunkEncountered === true;
  }

  /**
   * Bail out if this is an extraneous task whose SQS message was created before any parallel
   * task had yet encountered a partial batch. The source queue only depletes over a run's life
   * (never refills), so once partialOrEmptyChunkEncountered is true, the queue can only be as
   * drained or more drained by the time a late-arriving task actually runs - aborting it
   * immediately is safe.
   * @returns true if this task should abort, false otherwise
   */
  public async isAlreadyFinished(): Promise<boolean> {
    return this.hasAnyTaskEncounteredPartial();
  }

  /** Run-wide totalRecords accumulated so far by completed chunker tasks (0 if none yet). */
  public async getRunningTotalRecords(): Promise<number> {
    const { bucketName, chunkDirectory, region } = this;
    const result = await this.metadata.read({ bucketName, chunkDirectory, region } satisfies ReadMetadataParams);
    return result?.totalRecords ?? 0;
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

  /**
   * Atomically add this task's own chunkCount/totalRecords contribution to the run's METADATA
   * record - every parallel chunker task calls this once (partial or full-iterationLimit).
   */
  public async accumulateMetadataTotals(params: {
    source: string;
    target?: string;
    itemsPerChunk: number;
    bulkReset: boolean;
    trustPreviousStorage: boolean;
    syncPopulation: SyncPopulation;
    chunkCountDelta: number;
    totalRecordsDelta: number;
    partialOrEmptyChunkEncountered?: boolean;
  }): Promise<{ chunkCount: number; totalRecords: number }> {
    const { bucketName, chunkDirectory, region } = this;
    return this.metadata.accumulateMetadataTotals({ bucketName, chunkDirectory, region, ...params });
  }
}
