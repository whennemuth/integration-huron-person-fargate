import { IContext as LandscapeContext } from '../../context/IContext';
import { integrationSecretName, readConfiguredStorage, resolveSecretStorage } from '../../lib/Secrets';
import { FUNCTION_BASE_NAME as chunkerSubscriberBaseName } from '../../src/chunking/fetch/ChunkerApiSubscriber';
import { DYNAMODB_TABLE_NAME as personHistoryTableName } from '../../src/dynamodb/PersonHistoryTable';
import { DYNAMODB_TABLE_NAME as statisticsTableName } from '../../src/dynamodb/StatisticsTable';
import { FUNCTION_BASE_NAME as processorSubscriberBaseName } from '../../src/processing/ProcessorSubscriber';

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
  chunksBucketName: string;
  /** Starting a run (BulkSync): the chunker subscriber it invokes, as the EventBridge schedule does */
  chunkerFunctionName: string;
  /** Starting a run: the processor subscriber, whose DRY_RUN it checks */
  processorSubscriberFunctionName: string;
  /** Starting a run: the context's values the schedule uses (ECS.chunkerService.chunksPerTask, BULK_RESET, TRUST_PREVIOUS_STORAGE) */
  chunksPerTask: number;
  bulkReset: boolean;
  trustPreviousStorage: boolean;
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
    chunksBucketName: `${chunksBucket}-${landscape}`,
    chunkerFunctionName: `${chunkerSubscriberBaseName}-${landscape}`,
    processorSubscriberFunctionName: `${processorSubscriberBaseName}-${landscape}`,
    chunksPerTask: context.ECS?.chunkerService?.chunksPerTask ?? 0,
    bulkReset: context.BULK_RESET ?? false,
    trustPreviousStorage: context.TRUST_PREVIOUS_STORAGE ?? false
  };
}
