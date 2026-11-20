import { ScalingInterval } from 'aws-cdk-lib/aws-applicationautoscaling';
import { IFunction } from 'aws-cdk-lib/aws-lambda';
import { Schedule, ScheduleExpression, ScheduleTargetInput } from 'aws-cdk-lib/aws-scheduler';
import { LambdaInvoke } from 'aws-cdk-lib/aws-scheduler-targets';
import { Construct } from 'constructs';
import { ConfigManager } from 'integration-huron-person';
import { ScheduledRun } from '../../../src/schedule/ScheduledRun';
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
 * - Schedule only created if the context has SCHEDULE.cronExpression, and always disabled (bulk-sync-schedule-plan.md:
 *   the dashboard is to manage the schedule, and this deployed one goes)
 * - Schedule triggers ChunkerSubscriber Lambda with API parameters: a full sync with stored hashes trusted
 *   (ScheduledRun, src/schedule/ScheduledRun.ts, which the dashboard's Bulk sync defaults also take)
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
   * Creates an EventBridge schedule to trigger API-based chunking on a cron schedule, always disabled (the
   * integration config's fetchSchedule it replaces was disabled in every landscape). Only creates the schedule if:
   * - chunkerLambda is provided
   * - context is provided
   * - the context has SCHEDULE.cronExpression
   * - the integration config's dataSource.people has endpointConfig.baseUrl and fetchPath
   */
  private createApiChunkingSchedule(props: ChunkerServiceProps): void {
    const { chunkerLambda, chunksPerTask: iterationLimit = 0 } = props;
    const context = this.props.context;

    if (!chunkerLambda || !context) {
      console.log('[ChunkerService] Skipping EventBridge schedule: chunkerLambda or context not provided');
      return;
    }

    const cronExpression = context.SCHEDULE?.cronExpression;
    if (!cronExpression) {
      console.log('[ChunkerService] Skipping EventBridge schedule: the context has no SCHEDULE.cronExpression');
      return;
    }
    const cronEnabled = false;

    // The endpoint for the event payload, from the integration config
    let huronConfig = context.HURON_PERSON_CONFIG as any;
    if (huronConfig.configPath) {
      huronConfig = ConfigManager.getInstance().fromFileSystem(huronConfig.configPath).getConfig('people');
    }
    const {
      dataSource: {
        people: {
          fetchPath,
          endpointConfig: { baseUrl } = {}
        } = {}
      } = {}
    } = huronConfig;

    if (!baseUrl || !fetchPath) {
      console.warn('[ChunkerService] Skipping EventBridge schedule: baseUrl or fetchPath not configured');
      return;
    }

    // TODO LATER: Figure out the best way to assign this variable a value.
    const personRecordProcessorAddons = undefined;

    // Create the EventBridge schedule with cron as a child of the QueueProcessingFargateService
    const { Landscape } = context.TAGS;
    this.schedule = new Schedule(this.service, 'ApiChunkingSchedule', {
      scheduleName: ScheduledRun.scheduleName(Landscape),
      description: `Triggers API-based chunker on schedule: ${cronExpression}`,
      schedule: ScheduleExpression.expression(cronExpression),
      enabled: cronEnabled,
      target: new LambdaInvoke(chunkerLambda, {
        input: ScheduleTargetInput.fromObject(
          ScheduledRun.scheduleInput({ baseUrl, fetchPath, iterationLimit, personRecordProcessorAddons })
        )
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
