import { CfnOutput, Stack, StackProps } from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import { AppConstruct } from './AppConstruct';
import { IContext } from '../context/IContext';

export interface IntegrationHuronPersonLambdaStackProps extends StackProps {
  context: IContext;
}

export class IntegrationHuronPersonLambdaStack extends Stack {
  constructor(scope: Construct, id: string, props: IntegrationHuronPersonLambdaStackProps) {
    super(scope, id, props);

    const { context: ctx } = props;

    // Convert tags object to record for CDK
    const tags: Record<string, string> = { ...ctx.TAGS };

    // ========================================
    // Top-Level Application Construct
    // ========================================
    const app = new AppConstruct(this, 'App', {
      context: ctx,
      tags,
    });

    // ========================================
    // NOTE: Input Bucket Event Notification
    // ========================================
    // The input bucket (ctx.S3.inputBucket) is created and managed by the huron-file-drop project.
    // Event notification to trigger ChunkerSubsriberLambda is configured there, NOT here.
    //
    // Deployment sequence:
    // 1. Deploy this stack (huron-person-fargate):
    //    - Creates ChunkerSubsriberLambda with execution role
    //    - Outputs lambda ARN and role ARN (see stack outputs below)
    //
    // 2. Update huron-file-drop stack:
    //    - Add new entry to context.BUCKET.subdirectories[]
    //    - Set subscriberLambdaArn to ChunkerSubsriberLambda ARN (from step 1 output)
    //    - Set subscriberLambdaExecutionRoleArn to ChunkerSubsriberLambda role ARN (from step 1 output)
    //    - Deploy huron-file-drop stack to wire up S3 event notification
    //
    // The ChunkerSubsriberLambda execution role already has PassRole permission (see ChunkerSubsriberLambda.ts),
    // which allows the huron-file-drop event processor to invoke it.

    // ========================================
    // Stack Outputs
    // ========================================
    new CfnOutput(this, 'EcrRepositoryUri', {
      value: app.ecr.repositoryUri,
      description: 'ECR repository URI for Docker images',
    });

    new CfnOutput(this, 'EcsClusterName', {
      value: app.ecs.cluster.clusterName,
      description: 'ECS cluster name',
    });

    new CfnOutput(this, 'ChunkerSubsriberLambdaArn', {
      value: app.subscribingLambdas.chunker.function.functionArn,
      description: 'Chunker Subscribing Lambda ARN (for huron-file-drop configuration)',
      exportName: `${ctx.STACK_ID}-ChunkerSubsriberLambdaArn`,
    });

    new CfnOutput(this, 'ChunkerSubsriberLambdaRoleArn', {
      value: app.subscribingLambdas.chunker.function.role!.roleArn,
      description: 'Chunker Subscribing Lambda execution role ARN (for huron-file-drop configuration)',
      exportName: `${ctx.STACK_ID}-ChunkerSubsriberLambdaRoleArn`,
    });

    new CfnOutput(this, 'ChunkerTaskDefinitionArn', {
      value: app.ecs.taskDefinitions.chunker.taskDefinition.taskDefinitionArn,
      description: 'Chunker task definition ARN',
    });

    new CfnOutput(this, 'ProcessorTaskDefinitionArn', {
      value: app.ecs.taskDefinitions.processor.taskDefinition.taskDefinitionArn,
      description: 'Processor task definition ARN',
    });

    new CfnOutput(this, 'ProcessorQueueUrl', {
      value: app.queue.processorQueue.queueUrl,
      description: 'Processor SQS queue URL',
    });

    new CfnOutput(this, 'ChunksBucketName', {
      value: app.chunksBucket.bucketName,
      description: 'Chunks S3 bucket name',
    });
  }
}