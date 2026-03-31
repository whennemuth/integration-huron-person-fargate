import { ScalingInterval } from 'aws-cdk-lib/aws-applicationautoscaling';
import { Construct } from 'constructs';
import { AbstractService, AbstractServiceProps } from '../AbstractService';

export interface MergerServiceProps extends AbstractServiceProps { }

/**
 * Creates a QueueProcessingFargateService for merger tasks
 * - Uses dedicated SQS queue + DLQ for merger messages
 * - Auto-scales based on queue depth (0 to maxScalingCapacity)
 * - Task reads CHUNKS_BUCKET and CHUNK_DIRECTORY from SQS messages
 * - Merges delta chunk files into final output
 */
export class MergerService extends AbstractService {

  constructor(scope: Construct, props: MergerServiceProps) {
    super(scope, props);
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
    return 'Merger';
  }
}
