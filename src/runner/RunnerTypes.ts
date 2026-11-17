import { TestEnvironment } from "integration-core";
import { SyncPopulation } from "../../docker/chunkTypes";
import type { ProcessorCapacityRecord } from "../chunking/ProcessorCapacityRecord";

/**
 * Environment configuration extracted from process.env for runner operations
 */
export interface RunnerEnv {
  secretArn?: string;
  configPath?: string;
  queueUrl?: string;
  populationType?: string;
  populationScope?: 'standard' | 'single';
  iterationLimit?: number;
  buid?: string;
  region?: string;
  stackId?: string;
  landscape?: string;
  bulkReset?: boolean;
  trustPreviousStorage?: boolean;
  skipAtomicCounterReset?: boolean;
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
  /** Proportion (0 to 1) of simulated students given a term that isn't current */
  sourceSimulatorMockNonCurrentTermRate?: number;
  /** Mock landscapes only: truncate the landscape's run-state tables before the run */
  mockTargetResetState?: boolean;
  /** Comma-delimited list of personRecordProcessor Addon enum keys to activate for this run */
  personRecordProcessorAddons?: string;
  /**
   * The processor service's maximum tasks for this run (1-20), in place of the context's
   * ECS.processorService.maxScalingCapacity (src/chunking/ProcessorScalingCapacity.ts); unset, the run gets that
   */
  processorScalingCapacityOverride?: number;
}

/**
 * Data source endpoint configuration (baseUrl + fetchPath)
 */
export interface Endpoint {
  baseUrl: string;
  fetchPath: string;
  /**
   * Source endpoints only: the terms endpoint the processor tasks should use for this run instead
   * of the configured one. Set by SourceSimulatorRunnerDecorator, so that a simulated-source run
   * also gets simulated terms (see Flags.termsEndpoint).
   */
  termsEndpoint?: Endpoint;
  /**
   * Source endpoints only: the processor maximum the run started with, which start() adds once it has set it, for the
   * run's Flags (see Flags.processorScalingCapacity)
   */
  processorScalingCapacity?: ProcessorCapacityRecord;
}

/**
 * Target configuration metadata.
 * Which target system is used (the real Huron API, or a mock landscape's target simulator) is NOT
 * decided here - it is part of the landscape's integration config (see isMockLandscape in src/Utils.ts).
 */
export type TargetConfig = {
  endpoint?: Endpoint;
};

/**
 * Normalized population type for chunking operations
 */
export type NormalizedPopulationType = typeof SyncPopulation.PersonDelta | typeof SyncPopulation.PersonFull;

/** A number from an environment variable, fractions included; undefined when unset, blank or not a number */
export const optionalNumber = (raw: string | undefined): number | undefined => {
  if (raw === undefined || raw.trim() === '') {
    return undefined;
  }
  const value = Number(raw);
  if (!Number.isFinite(value)) {
    console.warn(`Ignoring ${raw}: not a number`);
    return undefined;
  }
  return value;
}

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
    DATASOURCE_ENDPOINTCONFIG_ITERATION_LIMIT: iterationLimit,
    SINGLE_PERSON_BUID: buid,
    REGION: region,
    STACK_ID: stackId,
    LANDSCAPE: landscape,
    BULK_RESET: bulkReset,
    TRUST_PREVIOUS_STORAGE: trustPreviousStorage,
    SKIP_ATOMIC_COUNTER_RESET: skipAtomicCounterReset,
    MESSAGES_TO_PREPOPULATE: messagesToPrepopulate = '0',
    DESIRED_COUNT,
    ECS_CLUSTER_NAME: clusterName,
    ECS_SERVICE_NAME: serviceName,
    MESSAGING_ONLY: messagingOnly,
    CHUNKING_ONLY: chunkingOnly,
    SOURCE_SIMULATOR: sourceSimulator,
    SOURCE_SIMULATOR_MOCK_TOTAL_POPULATION: sourceSimulatorMockTotalPopulation,
    SOURCE_SIMULATOR_MOCK_SIMULATED_DELAY_SECONDS: sourceSimulatorMockSimulatedDelaySeconds,
    SOURCE_SIMULATOR_MOCK_ERROR_RATE: sourceSimulatorMockErrorRate,
    SOURCE_SIMULATOR_MOCK_NON_CURRENT_TERM_RATE: sourceSimulatorMockNonCurrentTermRate,
    MOCK_TARGET_RESET_STATE: mockTargetResetState,
    PERSON_RECORD_PROCESSOR_ADDONS: personRecordProcessorAddons,
    PROCESSOR_SCALING_CAPACITY_OVERRIDE: processorScalingCapacityOverride
  } = process.env;

  return {
    secretArn,
    configPath,
    queueUrl,
    populationType,
    buid,
    region,
    stackId,
    landscape,
    messagesToPrepopulate,
    clusterName,
    serviceName,
    iterationLimit: iterationLimit ? parseInt(iterationLimit) : undefined,
    desiredCount: DESIRED_COUNT ? parseInt(DESIRED_COUNT) : 0,
    bulkReset: `${bulkReset}`.toLowerCase().trim() === 'true',
    trustPreviousStorage: `${trustPreviousStorage}`.toLowerCase().trim() === 'true',
    skipAtomicCounterReset: `${skipAtomicCounterReset}`.toLowerCase().trim() === 'true',
    messagingOnly: `${messagingOnly}`.toLowerCase().trim() === 'true',
    chunkingOnly: `${chunkingOnly}`.toLowerCase().trim() === 'true',
    populationScope: buid ? 'single' : 'standard',
    sourceSimulator: `${sourceSimulator}`.toLowerCase().trim() === 'true',
    // The simulator's settings for the run: each sent as given, 0 included; unset leaves the simulator's own
    sourceSimulatorMockTotalPopulation: optionalNumber(sourceSimulatorMockTotalPopulation),
    sourceSimulatorMockSimulatedDelaySeconds: optionalNumber(sourceSimulatorMockSimulatedDelaySeconds),
    sourceSimulatorMockErrorRate: optionalNumber(sourceSimulatorMockErrorRate),
    sourceSimulatorMockNonCurrentTermRate: optionalNumber(sourceSimulatorMockNonCurrentTermRate),
    mockTargetResetState: `${mockTargetResetState}`.toLowerCase().trim() === 'true',
    personRecordProcessorAddons,
    // Checked when the run starts (ChunkingServiceRunner.start()): a value that isn't 1 to 20 stops it there
    processorScalingCapacityOverride: optionalNumber(processorScalingCapacityOverride)
  } satisfies RunnerEnv;
}

export const setTestEnvironment = (): void => {
  const testEnvironment = TestEnvironment('RUNNER');

  [
    'HURON_PERSON_CONFIG_PATH',
    'SECRET_ARN',
    'CHUNKER_QUEUE_URL',
    'POPULATION_TYPE',
    'DATASOURCE_ENDPOINTCONFIG_ITERATION_LIMIT',
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
    'SOURCE_SIMULATOR_MOCK_ERROR_RATE',
    'SOURCE_SIMULATOR_MOCK_NON_CURRENT_TERM_RATE',
    'MOCK_TARGET_RESET_STATE',
    'PERSON_RECORD_PROCESSOR_ADDONS',
    'PROCESSOR_SCALING_CAPACITY_OVERRIDE'
  ].forEach(testEnvironment.getVar);

  [
    'BULK_RESET',
    'TRUST_PREVIOUS_STORAGE'
  ].forEach(testEnvironment.getVarOrEmptyString);
}