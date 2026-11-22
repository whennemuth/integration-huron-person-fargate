import { ScalingInterval } from 'aws-cdk-lib/aws-applicationautoscaling';
import { PolicyStatement, Role, ServicePrincipal } from 'aws-cdk-lib/aws-iam';
import { IFunction } from 'aws-cdk-lib/aws-lambda';
import { Stack } from 'aws-cdk-lib';
import { ScheduledRun } from '../../../src/schedule/ScheduledRun';
import { Construct } from 'constructs';
import { AbstractService, AbstractServiceProps } from '../AbstractService';

export const SERVICE_LOGICAL_ID = 'Chunker';

export interface ChunkerServiceProps extends AbstractServiceProps {
  /** The ChunkerSubscriber Lambda function, which the bulk sync schedule's role may invoke */
  chunkerLambda?: IFunction;
}

/**
 * Creates a QueueProcessingFargateService for chunker tasks
 * - Uses dedicated SQS queue + DLQ for chunker messages
 * - Auto-scales based on queue depth (0 to maxScalingCapacity)
 * - Task reads INPUT_BUCKET and INPUT_KEY from SQS messages
 * - Chunks large JSON files into smaller NDJSON files
 *
 * Also creates the role the landscape's bulk sync schedule invokes the chunker subscriber with
 * (ScheduledRun.roleName(), chunker-schedule-<landscape>). The schedule itself is never deployed: it is managed at run
 * time through ScheduledRun (src/schedule/ScheduledRun.ts), from the dashboard or its harness, so a deploy can't put
 * back a schedule someone changed, and a schedule created in the dashboard can't collide with one CloudFormation
 * would create. bin/teardown.sh deletes it.
 */
export class ChunkerService extends AbstractService {
  /** The role the bulk sync schedule assumes to invoke the chunker subscriber; absent without the subscriber */
  public scheduleRole?: Role;

  public getServiceName(): string {
    return SERVICE_LOGICAL_ID;
  }

  constructor(scope: Construct, props: ChunkerServiceProps) {
    super(scope, props);

    // Expand the scaling activity to include invisible messages in the queue (not just visible).
    super.setupCompositeScaling();

    this.createScheduleRole(props);
  }

  /**
   * The role EventBridge Scheduler assumes for the landscape's bulk sync schedule: trusted by the scheduler only for
   * schedules in this account, and allowed only to invoke the chunker subscriber. Its name is fixed, so ScheduledRun
   * can pass it when creating the schedule.
   */
  private createScheduleRole(props: ChunkerServiceProps): void {
    const { chunkerLambda } = props;
    const context = this.props.context;

    if (!chunkerLambda || !context) {
      console.log('[ChunkerService] Skipping the schedule role: chunkerLambda or context not provided');
      return;
    }

    const { Landscape } = context.TAGS;
    const { account } = Stack.of(this.service);
    this.scheduleRole = new Role(this.service, 'ScheduleRole', {
      roleName: ScheduledRun.roleName(Landscape),
      description: `Lets the bulk sync schedule ${ScheduledRun.scheduleName(Landscape)} invoke the chunker subscriber`,
      assumedBy: new ServicePrincipal('scheduler.amazonaws.com', {
        conditions: { StringEquals: { 'aws:SourceAccount': account } },
      }),
    });
    this.scheduleRole.addToPolicy(new PolicyStatement({
      actions: ['lambda:InvokeFunction'],
      resources: [chunkerLambda.functionArn],
    }));
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
