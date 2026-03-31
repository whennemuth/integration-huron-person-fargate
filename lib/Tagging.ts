import { Aspects, CfnResource, IAspect, Stack, Tags } from "aws-cdk-lib";
import { IConstruct } from "constructs";

/**
 * Utility class to apply tags to a CDK stack and its resources.
 */
export class TaggingAspect {
  private _stack: Stack;
  private _tags: { [key: string]: string };

  constructor(stack: Stack, tags?: { [key: string]: string }) {
    this._stack = stack;
    this._tags = tags || {};
  }

  public applyTags(params: { tags?: { [key: string]: string }, aspect?: IAspect }): void {
    const { _stack, _tags } = this;
    const { aspect, tags=_tags } = params;
    
    if(aspect) {
      // Add BU naming aspect with priority 100 to ensure it runs BEFORE tag aspects (priority 200)
      Aspects.of(_stack).add(aspect, { priority: 100 });
    }

    for (const [key, value] of Object.entries(tags)) {
      if (value) {
        // NOTE: Adding tags into the stackProps does not seem to work - have to apply tags using aspects
        Tags.of(_stack).add(key, value);
      }
    }
  }
}

/**
 * BU Tagging Aspect to apply BU-standard 'name' tags to resources.
 */
export class BU_NameTagAspect implements IAspect {
  private tags: { [key: string]: string };
  private ec2Index: number = 1;

  constructor(tags: { [key: string]: string }) {
    this.tags = tags;
  }

  private nextEc2Index = (): number => {
    return this.ec2Index++;
  }

  public visit(node: IConstruct): void {
    const { tags, getResourceId, nextEc2Index } = this;

    if (node instanceof CfnResource) {
      const resourceType = node.cfnResourceType;
      const id = getResourceId(resourceType);
      const tv = (tn: string) => tags[tn];

      if( ! tv) {
        return;
      };

      let tagValue = `${tv('Service')}-${tv('Function')}-${tv('Landscape')}-${id}`;
      if (id) {
        if(id === 'ec2') {
          tagValue = tagValue.replace(/-ec2$/, `-Instance${nextEc2Index()}-ec2`);
        }
       Tags.of(node).add('Name', tagValue);
      }
    }
  }

  private getResourceId = (resourceType: string): string | undefined => {

    // BU mappings per https://github.com/bu-ist/buaws-istcloud-information/blob/main/aws-tagging-standard.md#name
    const mappings: Record<string, string> = {
      'AWS::EC2::Instance': 'ec2',
      'AWS::IAM::Role': 'role',
      'AWS::EC2::SecurityGroup': 'sg',
      'AWS::ECS::Cluster': 'ecscluster',
      'AWS::Logs::LogGroup': 'cwloggroup',
      'AWS::S3::Bucket': 's3',
      'AWS::S3::BucketPolicy': 's3policy',
      'AWS::ElasticLoadBalancingV2::LoadBalancer': 'alb',
      'AWS::ElasticLoadBalancingV2::NetworkLoadBalancer': 'nlb',
      'AWS::ElasticLoadBalancingV2::ListenerRule': 'albrule',
      'AWS::ElasticLoadBalancingV2::Listener': 'alblsnr',
      'AWS::CodePipeline::Pipeline': 'pipeline',
      'AWS::CodeBuild::Project': 'codebuild',
      'AWS::ECS::TaskDefinition': 'ecstask',
      'AWS::AutoScaling::AutoScalingGroup': 'autoscaling',
    };

    // Supplemental mappings not in BU doc
    const othermappings: Record<string, string> = {
      'AWS::DMS::ReplicationSubnetGroup': 'dmsrpsubnetgrp',
      'AWS::ElasticLoadBalancingV2::TargetGroup': 'albtg',
      'AWS::RDS::DBInstance': 'rds',
      'AWS::DMS::ReplicationInstance': 'dmsri',
      'AWS::DMS::Endpoint': 'dmsendpt',
      'AWS::DMS::ReplicationTask': 'dmsrptsk',
      'AWS::Lambda::LayerVersion': 'lambdalv',
      'AWS::Lambda::Function': 'lambda',
      'AWS::DynamoDB::Table': 'dynamodbtbl',
      'AWS::ApiGateway::RestApi': 'restapi',
      'AWS::Events::Rule': 'eventrule',
      'AWS::StepFunctions::StateMachine': 'statemachine',
      'AWS::SNS::Topic': 'sns',
      'AWS::SQS::Queue': 'sqs',
      'AWS::CloudWatch::Alarm': 'cldwtchalarm',
      'AWS::CloudFront::Distribution': 'cloudfront',
      'AWS::KMS::Key': 'kmskey',
      'AWS::SecretsManager::Secret': 'secretsmgrsecret',
      'AWS::EFS::FileSystem': 'efs',
      'AWS::EFS::MountTarget': 'efsmounttarget',
      'AWS::CloudWatchLogs::LogGroup': 'cwloggroup',
    };
    
    return mappings[resourceType];
  }
} 