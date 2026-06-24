import { CfnResource, Duration, Tags } from "aws-cdk-lib";
import { ScalingInterval } from "aws-cdk-lib/aws-applicationautoscaling";
import { CfnAlarm } from "aws-cdk-lib/aws-cloudwatch";
import { IVpc } from "aws-cdk-lib/aws-ec2";
import { CfnService, FargatePlatformVersion, FargateTaskDefinition, ICluster } from "aws-cdk-lib/aws-ecs";
import { QueueProcessingFargateService, QueueProcessingFargateServiceProps } from "aws-cdk-lib/aws-ecs-patterns";
import { IQueue } from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";
import { IContext } from "../../context/IContext";

export interface AbstractServiceProps {
  cluster: ICluster;
  taskDefinition: FargateTaskDefinition;
  vpc: IVpc;
  queue: IQueue;
  deadLetterQueue: IQueue;
  maxScalingCapacity: number;
  stackScope: Construct;  // Stack reference for escape hatches
  context: IContext;  // Context for accessing Landscape and other configuration
  tags?: { [key: string]: string };
}

export abstract class AbstractService {
  public service: QueueProcessingFargateService;

  constructor(scope: Construct, protected props: AbstractServiceProps) {
    const { queue, deadLetterQueue } = props;

    // Create QueueProcessingFargateService
    this.service = new QueueProcessingFargateService(scope, this.getServiceName(), {
      serviceName: this.getServiceName(),
      cluster: props.cluster,
      cooldown: Duration.seconds(10),
      taskDefinition: props.taskDefinition,
      disableCpuBasedScaling: true,
      assignPublicIp: false,  // Use private subnets with NAT gateway - no public IP needed
      queue,
      // Use ARM64 platform
      platformVersion: FargatePlatformVersion.LATEST,
      // Start with 0 tasks - only scale up when messages arrive
      minScalingCapacity: 0,
      maxScalingCapacity: props.maxScalingCapacity,
      scalingSteps: this.getScalingSteps(),      
    } satisfies QueueProcessingFargateServiceProps);

    // Apply tags
    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        Tags.of(queue).add(key, value);
        Tags.of(deadLetterQueue).add(key, value);
        Tags.of(this.service).add(key, value);
      });
    }

    
    /**
     * An "escape hatch" is needed here because the default desiredCount of a service created
     * by the QueueProcessingFargateService construct is 1, which means that one task will be 
     * running at all times. Combine this with the fact that the 'SkipDestinationValidation' 
     * property was forcibly set to false, there is a message depth to desired count mismatch
     * (0 to 1) and the service will get stuck in a "CREATE_IN_PROGRESS" state during deployment.
     * By overriding the desired count to 0, we avoid this issue.
     */
    const cfnService = this.service.service.node.defaultChild as CfnService;
    cfnService.addPropertyOverride('DesiredCount', 0);

    /**
     * Override CloudWatch Alarm Period from default 300 seconds to 60 seconds (1 minute)
     * for faster scaling response. QueueProcessingFargateService doesn't expose a property
     * to configure this, so we use an escape hatch to find and modify all CfnAlarm resources.
     */
    this.overrideAlarmPeriod(60);
  }


  /**
   * Escape hatch: Override the default scaling metric to use total messages (visible + not visible)
   * instead of just visible messages. This prevents scale-down during message processing
   * when messages are invisible but still being worked on.
   * 
   * Uses escape hatch to modify the CloudWatch Alarms created by QueueProcessingFargateService.
   */
  protected setupCompositeScaling(): void {
    const { queue } = this.props;
    
    // Get the CloudFormation logical ID of the queue resource
    const queueCfn = queue.node.defaultChild as CfnResource;
    const queueNameRef = { 'Fn::GetAtt': [queueCfn.logicalId, 'QueueName'] };
    
    // Find all CloudWatch Alarms created by QueueProcessingFargateService
    const alarms = this.service.node.findAll().filter(
      (child) => child instanceof CfnAlarm
    ) as CfnAlarm[];

    alarms.forEach((alarm) => {
      // Get the current alarm properties to preserve threshold and comparison operator
      const alarmResource = alarm as any;
      
      // Remove the single-metric properties (they conflict with Metrics array)
      alarm.addPropertyDeletionOverride('MetricName');
      alarm.addPropertyDeletionOverride('Namespace');
      alarm.addPropertyDeletionOverride('Dimensions');
      alarm.addPropertyDeletionOverride('Statistic');
      alarm.addPropertyDeletionOverride('Period');  // Remove outer Period when using Metrics array
      
      // Add the Metrics array with composite expression
      alarm.addPropertyOverride('Metrics', [
        {
          Id: 'visible',
          MetricStat: {
            Metric: {
              MetricName: 'ApproximateNumberOfMessagesVisible',
              Namespace: 'AWS/SQS',
              Dimensions: [
                {
                  Name: 'QueueName',
                  Value: queueNameRef
                }
              ]
            },
            Period: 60,
            Stat: 'Maximum'
          },
          ReturnData: false
        },
        {
          Id: 'notVisible',
          MetricStat: {
            Metric: {
              MetricName: 'ApproximateNumberOfMessagesNotVisible',
              Namespace: 'AWS/SQS',
              Dimensions: [
                {
                  Name: 'QueueName',
                  Value: queueNameRef
                }
              ]
            },
            Period: 60,
            Stat: 'Maximum'
          },
          ReturnData: false
        },
        {
          Expression: 'visible + notVisible',
          Id: 'totalMessages',
          Label: 'Total Messages (Visible + In-Flight)',
          ReturnData: true
        }
      ]);
    });
  }

  /**
   * Override the Period property on all CloudWatch Alarms created by QueueProcessingFargateService
   * @param periodSeconds - The period in seconds (minimum 60)
   */
  private overrideAlarmPeriod(periodSeconds: number): void {
    const alarms = this.service.node.findAll().filter(
      (child) => child instanceof CfnAlarm
    ) as CfnAlarm[];

    alarms.forEach((alarm) => {
      alarm.addPropertyOverride('Period', periodSeconds);
    });
  }
  
  /**
   * Returns scaling step configuration for the service.
   * Each service type may have different scaling behavior.
   */
  public abstract getScalingSteps(): ScalingInterval[];

  /**
   * Returns the service name used for both CloudFormation logical ID and ECS service name.
   * Does NOT include landscape suffix - services are scoped within clusters.
   */
  public abstract getServiceName(): string;
}