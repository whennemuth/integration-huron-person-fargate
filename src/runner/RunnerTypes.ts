import { TestEnvironment } from "integration-core";
import { SyncPopulation } from "../../docker/chunkTypes";

/**
 * Environment configuration extracted from process.env for runner operations
 */
export interface RunnerEnv {
  secretArn?: string;
  configPath?: string;
  queueUrl?: string;
  populationType?: string;
  populationScope?: 'standard' | 'single';
  callLimit?: string;
  buid?: string;
  region?: string;
  stackId?: string;
  landscape?: string;
  bulkReset?: boolean;
  trustPreviousStorage?: boolean;
  messagesToPrepopulate: string;
  desiredCount: number;
  clusterName?: string;
  serviceName?: string;
  messagingOnly?: boolean;
  chunkingOnly?: boolean;
  sourceSimulator?: boolean;
  sourceSimulatorMockTotalPopulation?: number;
  sourceSimulatorMockSimulatedDelaySeconds?: number;
  sourceSimulatorMockErrorRate?: number;
}

/**
 * Data source endpoint configuration (baseUrl + fetchPath)
 */
export interface Endpoint {
  baseUrl: string;
  fetchPath: string;
}

/**
 * Normalized population type for chunking operations
 */
export type NormalizedPopulationType = typeof SyncPopulation.PersonDelta | typeof SyncPopulation.PersonFull;

/**
 * Extract environment variables into RunnerEnv structure.
 * Common implementation for all runners.
 */
export const extractEnvironment = (): RunnerEnv => {
  const {
    SECRET_ARN: secretArn,
    HURON_PERSON_CONFIG_PATH: configPath,
    CHUNKER_QUEUE_URL: queueUrl,
    POPULATION_TYPE: populationType,
    DATASOURCE_ENDPOINTCONFIG_CALL_LIMIT: callLimit,
    SINGLE_PERSON_BUID: buid,
    REGION: region,
    STACK_ID: stackId,
    LANDSCAPE: landscape,
    BULK_RESET: bulkReset,
    TRUST_PREVIOUS_STORAGE: trustPreviousStorage,
    SOURCE_SIMULATOR: sourceSimulator,
    MESSAGES_TO_PREPOPULATE: messagesToPrepopulate = '0',
    DESIRED_COUNT,
    ECS_CLUSTER_NAME: clusterName,
    ECS_SERVICE_NAME: serviceName,
    MESSAGING_ONLY: messagingOnly,
    CHUNKING_ONLY: chunkingOnly,
    SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION: sourceSimulatorMockTotalPopulation,
    SOURCE_SIMULATOR_MOCK_SIMULATED_DELAY_SECONDS: sourceSimulatorMockSimulatedDelaySeconds,
    SOURCE_SIMULATOR_MOCK_ERROR_RATE: sourceSimulatorMockErrorRate
  } = process.env;

  return {
    secretArn,
    configPath,
    queueUrl,
    populationType,
    callLimit,
    buid,
    region,
    stackId,
    landscape,
    messagesToPrepopulate,
    clusterName,
    serviceName,
    desiredCount: DESIRED_COUNT ? parseInt(DESIRED_COUNT) : 0,
    bulkReset: `${bulkReset}`.toLowerCase().trim() === 'true',
    trustPreviousStorage: `${trustPreviousStorage}`.toLowerCase().trim() === 'true',
    messagingOnly: `${messagingOnly}`.toLowerCase().trim() === 'true',
    chunkingOnly: `${chunkingOnly}`.toLowerCase().trim() === 'true',
    populationScope: buid ? 'single' : 'standard',
    sourceSimulator: `${sourceSimulator}`.toLowerCase().trim() === 'true',
    sourceSimulatorMockTotalPopulation: sourceSimulatorMockTotalPopulation ? parseInt(sourceSimulatorMockTotalPopulation) : undefined,
    sourceSimulatorMockSimulatedDelaySeconds: sourceSimulatorMockSimulatedDelaySeconds ? parseInt(sourceSimulatorMockSimulatedDelaySeconds) : undefined,
    sourceSimulatorMockErrorRate: sourceSimulatorMockErrorRate ? parseFloat(sourceSimulatorMockErrorRate) : undefined
  } satisfies RunnerEnv;
}

export const setTestEnvironment = (): void => {
  const testEnvironment = TestEnvironment('RUNNER');

  [
    'HURON_PERSON_CONFIG_PATH',
    'SECRET_ARN',
    'CHUNKER_QUEUE_URL',
    'POPULATION_TYPE',
    'DATASOURCE_ENDPOINTCONFIG_CALL_LIMIT',
    'SINGLE_PERSON_BUID',
    'REGION',
    'MESSAGES_TO_PREPOPULATE',
    'DESIRED_COUNT',
    'ECS_CLUSTER_NAME',
    'ECS_SERVICE_NAME',
    'STACK_ID',
    'MESSAGING_ONLY',
    'CHUNKING_ONLY',
    'SOURCE_SIMULATOR',
    'SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION',
    'SOURCE_SIMULATOR_MOCK_SIMULATED_DELAY_SECONDS',
    'SOURCE_SIMULATOR_MOCK_ERROR_RATE'
  ].forEach(testEnvironment.getVar);

  [
    'BULK_RESET',
    'TRUST_PREVIOUS_STORAGE'
  ].forEach(testEnvironment.getVarOrEmptyString);
}