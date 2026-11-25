import { IContext as LandscapeContext } from '../../context/IContext';
import { integrationSecretName, readConfiguredStorage, resolveSecretStorage } from '../../lib/Secrets';
import { FUNCTION_BASE_NAME as chunkerSubscriberBaseName } from '../../src/chunking/fetch/ChunkerApiSubscriber';
import { DYNAMODB_TABLE_NAME as personCurrentStateTableName } from '../../src/dynamodb/PersonCurrentStateTable';
import { DYNAMODB_TABLE_NAME as personHistoryTableName } from '../../src/dynamodb/PersonHistoryTable';
import { DYNAMODB_TABLE_NAME as statisticsTableName } from '../../src/dynamodb/StatisticsTable';
import { FUNCTION_BASE_NAME as processorSubscriberBaseName } from '../../src/processing/ProcessorSubscriber';
import { CLUSTER_BASE_NAME } from '../../lib/EcsInfrastructure';
import { SERVICE_LOGICAL_ID as CHUNKER_SERVICE_NAME } from '../../lib/services/chunker/ChunkerService';
import { FUNCTION_BASE_NAME as sourceSimulatorBaseName } from '../../src/chunking/fetch/SourceSimulator';
import { isMockLandscape } from '../../src/Utils';
import { DYNAMODB_TABLE_NAME as atomicCounterTableName } from '../../src/dynamodb/AtomicCounter';
import { ScheduledRun } from '../../src/schedule/ScheduledRun';

/**
 * Names of the landscape's pipeline resources that its dashboard reads, derived the same way the
 * pipeline stack names them, so the dashboard stack needs no CloudFormation imports.
 */
export type LandscapeResources = {
  landscape: string;
  storageMode: string;
  integrationSecretName: string;
  statisticsTableName: string;
  /** Only in DynamoDB storage mode */
  personHistoryTableName?: string;
  /**
   * Only in DynamoDB storage mode: the table of each person's stored hash, which Lookup and sync's Sync now writes
   * after a successful push (dashboard/src/services/PersonSync.ts), as does a manual SyncPerson
   */
  personCurrentStateTableName?: string;
  chunksBucketName: string;
  /** Starting a run (BulkSync): the chunker subscriber it invokes, as the EventBridge schedule does */
  chunkerFunctionName: string;
  /** Starting a run: the processor subscriber, whose DRY_RUN it checks */
  processorSubscriberFunctionName: string;
  /** A parallel start: the chunker service, whose most tasks bounds Bulk sync's Parallel start */
  chunkerClusterName: string;
  chunkerServiceName: string;
  /** A run's review shows these: the pipeline stack's STACK_ID, and the chunker task's ITEMS_PER_CHUNK */
  pipelineStackId: string;
  itemsPerChunk?: number;
  /**
   * The processor service's deployed maximum tasks (the context's ECS.processorService.maxScalingCapacity, as
   * EcsInfrastructure sets it): Bulk sync's Processor tasks starts there and warns above it
   */
  processorMaxScalingCapacity: number;
  /**
   * A mock landscape only: the source simulator Lambda, whose Function URL Bulk sync looks up to offer it as the source
   * (absent in any other landscape, which has no simulator)
   */
  sourceSimulatorFunctionName?: string;
  /**
   * A mock landscape only: the atomic counter table, whose `not-a-counter:mock-landscape-reset` item records the last reset
   * (src/chunking/MockLandscapeReset.ts), which Overview shows
   */
  atomicCounterTableName?: string;
  /**
   * The landscape's bulk sync schedule (src/schedule/ScheduledRun.ts), which the Bulk sync schedule page manages: its
   * name, the role it invokes the chunker subscriber with (the pipeline stack creates it), and the context's SCHEDULE
   * cron a new schedule starts from
   */
  scheduleName: string;
  scheduleRoleName: string;
  scheduleDefaultCron?: string;
};

export const landscapeResources = (context: LandscapeContext): LandscapeResources => {
  const { S3: { chunksBucket }, TAGS: { Landscape } } = context;
  const landscape = Landscape.toLowerCase();
  // The storage mode the pipeline's tasks and config secret resolve to
  const storageMode = resolveSecretStorage(context, readConfiguredStorage(context)).type;
  return {
    landscape,
    storageMode,
    integrationSecretName: integrationSecretName(context),
    statisticsTableName: statisticsTableName(context),
    personHistoryTableName: storageMode === 'dynamodb' ? personHistoryTableName(context) : undefined,
    personCurrentStateTableName: storageMode === 'dynamodb' ? personCurrentStateTableName(context) : undefined,
    chunksBucketName: `${chunksBucket}-${landscape}`,
    chunkerFunctionName: `${chunkerSubscriberBaseName}-${landscape}`,
    processorSubscriberFunctionName: `${processorSubscriberBaseName}-${landscape}`,
    chunkerClusterName: `${CLUSTER_BASE_NAME}-${landscape}`,
    chunkerServiceName: CHUNKER_SERVICE_NAME,
    pipelineStackId: context.STACK_ID,
    itemsPerChunk: context.ITEMS_PER_CHUNK,
    processorMaxScalingCapacity: context.ECS?.processorService?.maxScalingCapacity ?? 1,
    scheduleName: ScheduledRun.scheduleName(landscape),
    scheduleRoleName: ScheduledRun.roleName(landscape),
    ...(context.SCHEDULE?.cronExpression ? { scheduleDefaultCron: context.SCHEDULE.cronExpression } : {}),
    ...(isMockLandscape(landscape) ? {
      sourceSimulatorFunctionName: `${sourceSimulatorBaseName}-${landscape}`,
      atomicCounterTableName: atomicCounterTableName(context)
    } : {})
  };
}
