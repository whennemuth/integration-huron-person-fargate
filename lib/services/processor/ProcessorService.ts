import { CfnResource } from 'aws-cdk-lib';
import { ScalingInterval } from 'aws-cdk-lib/aws-applicationautoscaling';
import { Bucket, EventType } from 'aws-cdk-lib/aws-s3';
import { SqsDestination } from 'aws-cdk-lib/aws-s3-notifications';
import { Construct } from 'constructs';
import { AbstractService, AbstractServiceProps } from '../AbstractService';

export interface ProcessorServiceProps extends AbstractServiceProps {
  chunksBucket: Bucket;
}

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

    const { chunksBucket, stackScope, queue } = props;

    /**
     * Add the S3 event notification to the chunks bucket to send messages to SQS when new person 
     * data chunks are deposited into the chunks bucket. This is needed to trigger the processor 
     * service to start processing these chunks as they arrive - one process triggered per arrival.
     */
    chunksBucket.addEventNotification(
      EventType.OBJECT_CREATED,
      new SqsDestination(queue),
      {
        prefix: 'chunks/',
        suffix: '.ndjson',
      }
    );

    /**
     * An "escape hatch" is needed here to override the 'SkipDestinationValidation'
     * property of the custom bucket notification resource created by the addEventNotification
     * method above. The value is overridden to "true". This will prevent the automatic depositing
     * of a default message into the queue as part of the cdk validation attempts of the queue
     * as a message destination. This must be avoided because it brings the message queue depth 
     * from zero to one momentarily, which will cause the auto-scaling of the service to spin up 
     * one task. This is because the desired count of the service is based on the queue depth, 
     * and if the depth is one, then one task will be created. This would not be a problem if 
     * the task polls for the message, sees that it is not a "real" message, and deletes it. 
     * But this will prevent the ability of deploying the service without either the image 
     * behind the task container being available in ECR or the task itself properly deleting 
     * messages it receives that don't conform to the expected format (e.g., the "fake" message 
     * put in the queue during validation), which was/maybe needed during deployment testing.
     */
    const bucketNotificationsResource = stackScope.node.findAll().find(
      (child) =>  child.node.path.includes(`/${chunksBucket.node.id}/`) && 
        child instanceof CfnResource &&
        child.cfnResourceType === 'Custom::S3BucketNotifications'
    ) as CfnResource;

    if (bucketNotificationsResource) {
      bucketNotificationsResource.addPropertyOverride('SkipDestinationValidation', true);
    }
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
