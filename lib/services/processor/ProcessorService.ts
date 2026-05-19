import { ScalingInterval } from 'aws-cdk-lib/aws-applicationautoscaling';
import { Construct } from 'constructs';
import { AbstractService, AbstractServiceProps } from '../AbstractService';

export interface ProcessorServiceProps extends AbstractServiceProps {}

/**
 * Creates a QueueProcessingFargateService for parallel chunk processing
 * - Uses pre-created SQS queue + DLQ
 * - Auto-scales based on queue depth (0 to maxScalingCapacity)
 * - Task reads S3 bucket/key from SQS messages (S3 event notifications)
 * 
 * Note: This class does NOT extend Construct to avoid extra nesting in the CloudFormation hierarchy.
 * The scope is passed to child constructs directly.
 */
export class ProcessorService extends AbstractService {
  constructor(scope: Construct, props: ProcessorServiceProps) {
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
    return 'Processor';
  }
}
