import { IContext as LandscapeContext } from '../../context/IContext';
import { integrationSecretName, readConfiguredStorage, resolveSecretStorage } from '../../lib/Secrets';
import { DYNAMODB_TABLE_NAME as personHistoryTableName } from '../../src/dynamodb/PersonHistoryTable';
import { DYNAMODB_TABLE_NAME as statisticsTableName } from '../../src/dynamodb/StatisticsTable';

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
};

export const landscapeResources = (context: LandscapeContext): LandscapeResources => {
  const { S3: { chunksBucket }, TAGS: { Landscape } } = context;
  // The storage mode the pipeline's tasks and config secret resolve to
  const storageMode = resolveSecretStorage(context, readConfiguredStorage(context)).type;
  return {
    landscape: Landscape.toLowerCase(),
    storageMode,
    integrationSecretName: integrationSecretName(context),
    statisticsTableName: statisticsTableName(context),
    personHistoryTableName: storageMode === 'dynamodb' ? personHistoryTableName(context) : undefined,
    chunksBucketName: `${chunksBucket}-${Landscape.toLowerCase()}`
  };
}
