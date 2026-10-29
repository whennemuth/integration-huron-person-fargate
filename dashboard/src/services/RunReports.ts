import { RunReporting } from '../../../src/reporting/RunReporting';
import { RunReportingService } from './ServiceTypes';

/**
 * This landscape's run reporting, over the tables and chunks bucket the dashboard stack names in
 * the Lambda's environment (dashboard/lib/FunctionUrl.ts). There is no landscape parameter: the
 * landscape is the deployment.
 */
export const runReportingFromEnvironment = (env: NodeJS.ProcessEnv = process.env): RunReportingService => {
  const {
    LANDSCAPE, PREVIOUS_STORAGE_TYPE, DYNAMODB_STATISTICS_TABLE_NAME, DYNAMODB_PERSON_HISTORY_TABLE_NAME, CHUNKS_BUCKET, REGION
  } = env;
  const missing = Object.entries({ LANDSCAPE, PREVIOUS_STORAGE_TYPE, DYNAMODB_STATISTICS_TABLE_NAME })
    .filter(([, value]) => !value)
    .map(([name]) => name);
  if (missing.length > 0) {
    throw new Error(`Run reporting needs environment variable(s): ${missing.join(', ')}`);
  }
  return new RunReporting({
    landscape: LANDSCAPE!,
    storageMode: PREVIOUS_STORAGE_TYPE!,
    statisticsTableName: DYNAMODB_STATISTICS_TABLE_NAME!,
    personHistoryTableName: DYNAMODB_PERSON_HISTORY_TABLE_NAME || undefined,
    chunksBucket: CHUNKS_BUCKET || undefined,
    region: REGION || undefined
  });
}
