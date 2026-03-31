import { Duration, RemovalPolicy, Tags } from 'aws-cdk-lib';
import { Repository, TagStatus } from 'aws-cdk-lib/aws-ecr';
import { Construct } from 'constructs';

export interface EcrRepositoryProps {
  registryId?: string;  // Optional AWS account ID of the ECR registry (if not provided, will check public ECR)
  repositoryName: string;
  tags?: { [key: string]: string };
}

/**
 * Creates an ECR repository for storing Docker images
 */
export class EcrRepository extends Construct {
  public readonly repository: Repository;

  constructor(scope: Construct, id: string, props: EcrRepositoryProps) {
    super(scope, id);

    // Create ECR repository with lifecycle policy
    this.repository = new Repository(this, 'Repository', {
      repositoryName: props.repositoryName,
      // Keep last 10 images, remove untagged images after 1 day
      lifecycleRules: [
        {
          description: 'Remove untagged images after 1 day',
          maxImageAge: Duration.days(1),
          rulePriority: 1,
          tagStatus: TagStatus.UNTAGGED,
        },
        {
          description: 'Keep last 10 images',
          maxImageCount: 10,
          rulePriority: 2,
          tagStatus: TagStatus.ANY,
        },
      ],
      // Delete repository when stack is deleted (change to RETAIN for production)
      removalPolicy: RemovalPolicy.DESTROY,
      emptyOnDelete: true,
    });

    // Apply any resource-specific tags - tags not defined in IContext.TAGS
    if (props.tags) {
      Object.entries(props.tags).forEach(([key, value]) => {
        Tags.of(this.repository).add(key, value);
      });
    }
  }

  /**
   * Get the repository URI for pushing Docker images
   */
  public get repositoryUri(): string {
    return this.repository.repositoryUri;
  }

  /**
   * Get the repository name
   */
  public get repositoryName(): string {
    return this.repository.repositoryName;
  }
}
