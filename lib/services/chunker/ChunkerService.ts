import { ScalingInterval } from 'aws-cdk-lib/aws-applicationautoscaling';
import { IFunction } from 'aws-cdk-lib/aws-lambda';
import { Schedule, ScheduleExpression, ScheduleTargetInput } from 'aws-cdk-lib/aws-scheduler';
import { LambdaInvoke } from 'aws-cdk-lib/aws-scheduler-targets';
import { Construct } from 'constructs';
import { ConfigManager, DataSourceConfig } from 'integration-huron-person';
import { IContext } from '../../../context/IContext';
import { SyncPopulation } from '../../../docker/chunkTypes';
import { ApiChunkerEvent } from '../../../src/chunking/ChunkerSubscriber';
import { getLocalConfig } from '../../../src/Utils';
import { AbstractService, AbstractServiceProps } from '../AbstractService';
import { handleApiEvent } from '../../../src/chunking/fetch/ChunkerApiSubscriber';

export interface ChunkerServiceProps extends AbstractServiceProps {
  /** The ChunkerSubscriber Lambda function to invoke on schedule */
  chunkerLambda?: IFunction;
  /** Context configuration containing fetchSchedule settings */
  context?: IContext;
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

  constructor(scope: Construct, props: ChunkerServiceProps) {
    super(scope, props);

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
    const { chunkerLambda, context } = props;

    if (!chunkerLambda || !context) {
      console.log('[ChunkerService] Skipping EventBridge schedule: chunkerLambda or context not provided');
      return;
    }

    // Extract fetchSchedule config from HURON_PERSON_CONFIG
    let huronConfig = context.HURON_PERSON_CONFIG as any;
    if (huronConfig.configPath) {
      huronConfig = ConfigManager.getInstance().fromFileSystem(huronConfig.configPath).getConfig('people');
    }
    const fetchSchedule = huronConfig?.dataSource?.people?.fetchSchedule;

    if (!fetchSchedule) {
      console.log('[ChunkerService] Skipping EventBridge schedule: fetchSchedule not configured');
      return;
    }

    if (!fetchSchedule.cronExpression) {
      console.warn('[ChunkerService] Skipping EventBridge schedule: fetchSchedule.cronExpression is missing');
      return;
    }

    // Extract API configuration for the event payload
    const peopleConfig = huronConfig?.dataSource?.people;
    const baseUrl = peopleConfig?.endpointConfig?.baseUrl;
    const fetchPath = peopleConfig?.fetchPath;
    
    if (!baseUrl || !fetchPath) {
      console.warn('[ChunkerService] Skipping EventBridge schedule: baseUrl or fetchPath not configured');
      return;
    }

    // Create the EventBridge schedule with cron as a child of the QueueProcessingFargateService
    this.schedule = new Schedule(this.service, 'ApiChunkingSchedule', {
      scheduleName: 'chunker-api-schedule',
      description: `Triggers API-based chunker on schedule: ${fetchSchedule.cronExpression}`,
      schedule: ScheduleExpression.expression(fetchSchedule.cronExpression),
      enabled: fetchSchedule.enabled,
      target: new LambdaInvoke(chunkerLambda, {
        input: ScheduleTargetInput.fromObject({
          baseUrl,
          fetchPath,
          populationType: SyncPopulation.PersonDelta,
          bulkReset: false, // Default value; can be overridden by message parameters if needed
          processingMetadata: {
            processedAt: new Date().toISOString(),
            processorVersion: '1.0.0'
          }
        })
      })
    });

    console.log(`[ChunkerService] Created EventBridge schedule: ${fetchSchedule.cronExpression} (enabled: ${fetchSchedule.enabled})`);
  }
  
  /**
   * https://docs.aws.amazon.com/autoscaling/ec2/userguide/as-scaling-simple-step.html
   * @returns An array of ScalingInterval objects defining the scaling behavior based on queue depth
   */
  public getScalingSteps(): ScalingInterval[] {
    return [
      // Scale IN: when 0 messages, remove 1 task
      { upper: 0, change: -1 },
    
      // Scale OUT: when >= 1 message, add 1 task (up to max of 5)
      { lower: 1, change: +1 }
    ];
  }
  
  public getServiceLogicalId(): string {
    return 'Chunker';
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
 */
async function startChunkingService() {
  /** Read additional configuration from environment */
  const {
    HURON_PERSON_CONFIG_PATH, 
    SECRET_ARN,
    BULK_RESET = 'false',
    POPULATION_TYPE,
    SINGLE_PERSON_BUID: buid,
    CHUNKER_QUEUE_URL: queueUrl
  } = process.env;

  /** Load configuration. */
  const configManager = ConfigManager.getInstance();
  const localConfigPath = HURON_PERSON_CONFIG_PATH || getLocalConfig();
  const config = await configManager
    .reset()
    .fromFileSystem(localConfigPath)              // ← Local dev only
    .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← TaskDef secret injection
    .fromSecretManager(SECRET_ARN)                // ← Fallback to Secrets Manager
    .fromEnvironment()                            // ← Fallback to individual env var overrides
    .getConfigAsync(buid ? 'person' : 'people');

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
  
  /** Define the API chunker event */
  const { PersonDelta, PersonFull } = SyncPopulation;
  const apiChunkerEvent = {
    baseUrl,
    fetchPath,
    populationType: POPULATION_TYPE?.toLowerCase() === PersonDelta ? PersonDelta : PersonFull,
    bulkReset: BULK_RESET.toLowerCase() === 'true',
    processingMetadata: {
      processedAt: new Date().toISOString(),
      processorVersion: '1.0.0'
    }
  } satisfies ApiChunkerEvent;

  // Send the event to the queue to trigger the chunking process.
  handleApiEvent(apiChunkerEvent);
}

// Run if executed directly
if (require.main === module) {
  startChunkingService();
}