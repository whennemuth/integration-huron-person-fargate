import { ScalingInterval } from 'aws-cdk-lib/aws-applicationautoscaling';
import { IFunction } from 'aws-cdk-lib/aws-lambda';
import { Schedule, ScheduleExpression, ScheduleTargetInput } from 'aws-cdk-lib/aws-scheduler';
import { LambdaInvoke } from 'aws-cdk-lib/aws-scheduler-targets';
import { Construct } from 'constructs';
import { TestEnvironment } from 'integration-core';
import { ConfigManager, DataSourceConfig } from 'integration-huron-person';
import { SyncPopulation } from '../../../docker/chunkTypes';
import { ApiChunkerEvent } from '../../../src/chunking/ChunkerSubscriber';
import { handleApiEvent } from '../../../src/chunking/fetch/ChunkerApiSubscriber';
import { QueueSeeder } from '../../../src/chunking/fetch/QueueSeeder';
import { DesiredCount } from '../../../src/DesiredCount';
import { getLocalConfig } from '../../../src/Utils';
import { AbstractService, AbstractServiceProps } from '../AbstractService';

export const SERVICE_LOGICAL_ID = 'Chunker';

export interface ChunkerServiceProps extends AbstractServiceProps {
  /** The ChunkerSubscriber Lambda function to invoke on schedule */
  chunkerLambda?: IFunction;
  /**
   * Optional: Number of chunks each task should process before exiting (default: 0 = process ALL 
   * of the chunks for ALL of the people in the source system in just one task execution)
   */
  chunksPerTask?: number;
}

/**
 * Creates a QueueProcessingFargateService for chunker tasks
 * - Uses dedicated SQS queue + DLQ for chunker messages
 * - Auto-scales based on queue depth (0 to maxScalingCapacity)
 * - Task reads INPUT_BUCKET and INPUT_KEY from SQS messages
 * - Chunks large JSON files into smaller NDJSON files
 * 
 * Also optionally creates an EventBridge schedule to trigger API-based chunking:
 * - Schedule only created if HURON_PERSON_CONFIG.dataSource.people.fetchSchedule is configured
 * - Schedule triggers ChunkerSubscriber Lambda with API parameters
 * - Lambda sends message to chunker queue to trigger Fargate task
 */
export class ChunkerService extends AbstractService {
  public schedule?: Schedule;

  public getServiceName(): string {
    return SERVICE_LOGICAL_ID;
  }

  constructor(scope: Construct, props: ChunkerServiceProps) {
    super(scope, props);

    // Expand the scaling activity to include invisible messages in the queue (not just visible).
    super.setupCompositeScaling();

    // Create EventBridge schedule for API-based chunking if configured
    this.createApiChunkingSchedule(props);
  }

  /**
   * Creates an EventBridge schedule to trigger API-based chunking on a cron schedule.
   * Only creates the schedule if:
   * - chunkerLambda is provided
   * - context is provided
   * - HURON_PERSON_CONFIG.dataSource.people.fetchSchedule is configured
   * - fetchSchedule.enabled is true
   * - fetchSchedule.cronExpression is valid
   */
  private createApiChunkingSchedule(props: ChunkerServiceProps): void {
    const { chunkerLambda, chunksPerTask: limit = 0 } = props;
    const context = this.props.context;
    const trustPreviousStorage = context?.TRUST_PREVIOUS_STORAGE ?? false;

    if (!chunkerLambda || !context) {
      console.log('[ChunkerService] Skipping EventBridge schedule: chunkerLambda or context not provided');
      return;
    }

    // Extract fetchSchedule config from HURON_PERSON_CONFIG
    let huronConfig = context.HURON_PERSON_CONFIG as any;
    if (huronConfig.configPath) {
      huronConfig = ConfigManager.getInstance().fromFileSystem(huronConfig.configPath).getConfig('people');
    }

    // Extract API configuration for the event payload
    const { 
      dataSource: { 
        people: {  
          fetchSchedule, fetchSchedule: { cronExpression, enabled: cronEnabled } = {}, 
          fetchPath, 
          endpointConfig: { baseUrl } = {}  
        } = {} 
      } = {} 
    } = huronConfig;

    if (!fetchSchedule) {
      console.log('[ChunkerService] Skipping EventBridge schedule: fetchSchedule not configured');
      return;
    }

    if (!fetchSchedule.cronExpression) {
      console.warn('[ChunkerService] Skipping EventBridge schedule: fetchSchedule.cronExpression is missing');
      return;
    }
    
    if (!baseUrl || !fetchPath) {
      console.warn('[ChunkerService] Skipping EventBridge schedule: baseUrl or fetchPath not configured');
      return;
    }

    // Create the EventBridge schedule with cron as a child of the QueueProcessingFargateService
    const { Landscape } = context.TAGS;
    this.schedule = new Schedule(this.service, 'ApiChunkingSchedule', {
      scheduleName: `chunker-api-schedule-${Landscape.toLowerCase()}`,
      description: `Triggers API-based chunker on schedule: ${cronExpression}`,
      schedule: ScheduleExpression.expression(cronExpression),
      enabled: cronEnabled,
      target: new LambdaInvoke(chunkerLambda, {
        input: ScheduleTargetInput.fromObject({
          baseUrl,
          fetchPath,
          populationType: SyncPopulation.PersonDelta,
          limit, 
          offset: 0,
          bulkReset: false, // Default value; can be overridden by message parameters if needed
          trustPreviousStorage,
          processingMetadata: {
            processedAt: new Date().toISOString(),
            processorVersion: '1.0.0'
          }
        })
      })
    });

    console.log(`[ChunkerService] Created EventBridge schedule: ${cronExpression} (enabled: ${cronEnabled})`);
  }
  
  /**
   * https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-scaling-simple-step.html
   * @returns An array of ScalingInterval objects defining the scaling behavior based on queue depth
   */
  public getScalingSteps(): ScalingInterval[] {
    return [
      // Scale IN: when 0 messages, remove 1 task
      { upper: 0, change: -1 },
    
      // Scale OUT: when >= 1 message, add 1 task (up to the max of set in the service props)
      { lower: 1, change: +1 }
    ];
  }
}


/**
 * FOR TESTING:
 * 
 * It is expected that an AWS EventBridge schedule will trigger the chunking process on a cron schedule,
 * which will invoke the ChunkerSubscriber Lambda to send messages to the chunker SQS queue. call this
 * function to manually trigger the chunking service to start a task off schedule, for example to kick 
 * off an initial chunking run or to test the service.
 * 
 * NOTE: If you do not want the appearance of new chunk files appearing in the bucket to set off the
 * processor service, you can set the environment variable DRY_RUN=true for the chunker subscriber
 * lambda function to prevent the chunker from sending messages to the processor queue. The is most
 * easily done through the AWS management console, or you can use the AWS CLI:
 * 
 * aws lambda update-function-configuration \
 *  --region us-east-2 \
 *  --cli-input-json "$(aws lambda get-function-configuration \
 *     --function-name chunker-subscriber-dev \
 *     --region us-east-2 | jq '.Environment.Variables += {"DRY_RUN": "true"} | {FunctionName: .FunctionName, Environment: .Environment}')"
 */
async function startChunkingService() {
  /** Read additional configuration from environment */
  const {
    // For configuring the chunking tasks.
    SECRET_ARN: secretArn,
    HURON_PERSON_CONFIG_PATH: configPath,
    CHUNKER_QUEUE_URL: queueUrl,
    POPULATION_TYPE: populationType,
    DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT: peopleLimit,
    SINGLE_PERSON_BUID: buid,
    REGION: region,
    STACK_ID: stackId,
    LANDSCAPE: landscape,
    BULK_RESET: bulkReset,
    TRUST_PREVIOUS_STORAGE: trustPreviousStorage,
    // For seeding the queue
    MESSAGES_TO_PREPOPULATE: messagesToPrepopulate = '0',
    DESIRED_COUNT,
    ECS_CLUSTER_NAME: clusterName,
    ECS_SERVICE_NAME: serviceName
  } = process.env;

  /** Load configuration. */
  const configManager = ConfigManager.getInstance();
  const localConfigPath = configPath || getLocalConfig();
  const config = await configManager
    .reset()
    .fromEnvironment()                            // ← Environment is first - takes precedence over all.
    .fromFileSystem(localConfigPath)              // ← Local dev only
    .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← TaskDef secret injection
    .fromSecretManager(secretArn)                 // ← Fallback to Secrets Manager
    .getConfigAsync(buid ? 'person' : 'people');

  let desiredCount = DESIRED_COUNT ? parseInt(DESIRED_COUNT) : 0;
  if(desiredCount > 0) {
    if( ! clusterName ) {
      console.error('ECS_CLUSTER_NAME environment variable is required to set desired count.');
      return;
    }
    if( ! serviceName ) {
      console.error('ECS_SERVICE_NAME environment variable is required to set desired count.');
      return;
    }
  }

  /**
   * Single person test?
   * 
   * If a buid is provided, pretend the single person endpoint is the full dataset. This allows us to 
   * test the full end to end flow in such a way as to confine errors to one person and in a 
   * conveniently short time due to the miniscule payload.
   */
  let baseUrl: string | undefined, fetchPath: string | undefined;
  if(buid) {
    const { 
      endpointConfig: { baseUrl: personBaseUrl } = {}, fetchPath: personFetchPath 
    } = config.dataSource.person || {};
    baseUrl = personBaseUrl;
    fetchPath = `${personFetchPath}?buid=${buid}`;
  }
  else {
    const { 
      endpointConfig: { baseUrl: peopleBaseUrl } = {}, fetchPath: peopleFetchPath 
    } = config.dataSource?.people as DataSourceConfig || {};
    baseUrl = peopleBaseUrl;
    fetchPath = peopleFetchPath;
  }

  /** Validate configuration */
  if(!baseUrl) {
    console.error('Missing baseUrl in configuration!');
    return;
  }
  if(!fetchPath) {
    console.error('Missing fetchPath in configuration!');
    return;
  }
  if(!queueUrl) {
    console.error('Missing CHUNKER_QUEUE_URL environment variable!');
    return;
  }
  if(messagesToPrepopulate && isNaN(Number(messagesToPrepopulate))) {
    console.error(`Invalid MESSAGES_TO_PREPOPULATE environment variable: ${messagesToPrepopulate}. Must be a number.`);
    return;
  }

  const seedNumber = parseInt(messagesToPrepopulate);
  const { PersonDelta, PersonFull } = SyncPopulation;
  const normalizedPopulationType = populationType?.toLowerCase() === PersonDelta ? PersonDelta : PersonFull;
  
  if(seedNumber > 0) {

    if(buid) {
      console.warn(`MESSAGES_TO_PREPOPULATE is set to ${seedNumber} > 0, but SINGLE_PERSON_BUID is also ` +
        `set (${buid}). Seeding the queue is not appropriate when processing just one person. Cancelling operation`);
      return;
    }

    if( ! region) {
      console.error('REGION environment variable is required to set desired count.');
      return;
    }
    if( ! stackId) {
      console.error('STACK_ID environment variable is required to set desired count.');
      return;
    }
    if( ! landscape) {
      console.error('LANDSCAPE environment variable is required to set desired count.');
      return;
    }

    // Raise the desired count to a level "worthy" of the prepopulation to kick off the processing of the 
    // seeded messages. This is done after seeding to allow the service to process the initial messages 
    // as quickly as possible rather than waiting for the scale up to occur first.
    if(desiredCount > 0) {
      console.log(`\n🚀 Scaling up ECS service ${serviceName} in cluster ${clusterName} to desired count of ${desiredCount}...\n`);
      const desiredCountManager = new DesiredCount({ clusterName, serviceName, region });
      const max = await desiredCountManager.getMax();
      if(max !== undefined && desiredCount > max) {
        console.warn(`Desired count of ${desiredCount} exceeds the maximum allowed by the auto-scaling configuration (${max}). ` +
          `Cancelling operation`);
        return;
      }
      await desiredCountManager.setTo(desiredCount);
    }

    // Seed the queue with initial messages to enable "hitting the ground running"
    console.log(`\n🌱 Seeding queue with ${seedNumber} messages...\n`);    
    const queueSeeder = new QueueSeeder({
      region,
      stackId,
      landscape,
      baseUrl,
      fetchPath,
      populationType: normalizedPopulationType,
      bulkReset: bulkReset?.toLowerCase() === 'true',
      trustPreviousStorage: trustPreviousStorage?.toLowerCase() === 'true',
      limit: peopleLimit ? parseInt(peopleLimit) : 0,
      messagesToSeed: seedNumber,
      queueUrl: queueUrl!,
      dryRun: false
    });
    // await queueSeeder.resetAtomicCounter();
    // await queueSeeder.seedQueue();
    
    console.log(`\n✓ Queue seeding complete. Ready to scale up desiredCount to ${seedNumber}.\n`);
  } else {
    // Send a single initial message to trigger the chunking process (normal one-in/one-out pattern)
    const apiChunkerEvent = {
      baseUrl,
      fetchPath,
      populationType: normalizedPopulationType,
      bulkReset: bulkReset?.toLowerCase() === 'true',
      trustPreviousStorage: trustPreviousStorage?.toLowerCase() === 'true',
      limit: peopleLimit ? parseInt(peopleLimit) : 0,
      offset: 0,
      processingMetadata: {
        processedAt: new Date().toISOString(),
        processorVersion: '1.0.0'
      }
    } satisfies ApiChunkerEvent;

    // Send the event to the queue to trigger the chunking process.
    await handleApiEvent(apiChunkerEvent, queueUrl);
  }
}

// Run if executed directly
if (require.main === module) {
  const testEnvironment = TestEnvironment('CHUNK_SERVICE');

  [
    'HURON_PERSON_CONFIG_PATH',
    'SECRET_ARN',
    'CHUNKER_QUEUE_URL',
    'POPULATION_TYPE',
    'DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT',
    'SINGLE_PERSON_BUID',
    'REGION',
    'MESSAGES_TO_PREPOPULATE',
    'DESIRED_COUNT',
    'ECS_CLUSTER_NAME',
    'ECS_SERVICE_NAME',
    'STACK_ID',
  ].forEach(testEnvironment.getVar);

  [
    'BULK_RESET',
    'TRUST_PREVIOUS_STORAGE'
  ].forEach(testEnvironment.getVarOrEmptyString);

  startChunkingService();
}