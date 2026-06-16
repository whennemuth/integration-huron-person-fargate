import { Config as HuronPersonConfig } from 'integration-huron-person';
import { RetryStrategyConfig } from '../src/processing/ApiErrorRetryStrategy';

export interface IContext {

  /** Unique identifier for the CDK stack */
  STACK_ID: string;

  /** AWS account ID where resources will be deployed */
  ACCOUNT: string;

  /** AWS region for deployment */
  REGION: string;

  /** Resource tags for cost allocation and organization */
  TAGS: {
    Landscape: string;
    Service: string;
    Function: string;
    CostCenter?: string;
    Ticket?: string;
  };

  /** Number of person records per chunk */
  ITEMS_PER_CHUNK: number;

  /** S3 bucket configuration */
  S3: {
    /** Bucket where input JSON files are uploaded */
    inputBucket: string;
    /** Bucket where chunks are stored */
    chunksBucket: string;
    /** Expiration for chunk files in days (temporary, processed immediately) */
    chunkExpirationDays: number;
    /** Expiration for delta files in days (merged by merger, but need failsafe cleanup) */
    deltaExpirationDays: number;
  };

  /** ECR repository configuration */
  ECR: {
    /** AWS account ID of the ECR registry */
    registryId?: string; // Optional because we can check public ECR if not provided
    /** Name of the ECR repository for Docker images */
    repositoryName: string;
  };

  /** ECS Fargate configuration */
  ECS: {
    /** Name of the ECS cluster */
    clusterName: string;
    /** Chunker task definition configuration */
    chunkerTaskDefinition: {
      cpu: number;
      memoryLimitMiB: number;
      memoryReservationMiB: number;
      logRetentionDays: number;
    };
    /** Processor task definition configuration */
    processorTaskDefinition: {
      cpu: number;
      memoryLimitMiB: number;
      memoryReservationMiB: number;
      logRetentionDays: number;
      retries?: RetryStrategyConfig;
    };
    /** Merger task definition configuration */
    mergerTaskDefinition: {
      cpu: number;
      memoryLimitMiB: number;
      memoryReservationMiB: number;
      logRetentionDays: number;
    };
    /** Processor service auto-scaling configuration */
    processorService: {
      maxScalingCapacity: number;
    };
    /** Chunker service auto-scaling configuration */
    chunkerService?: {
      maxScalingCapacity: number;
      /**
       * Optional: Number of chunks each task should process before exiting (default: 0 = process ALL 
       * of the chunks for ALL of the people in the source system in just one task execution - used
       * for testing.)
       */
      chunksPerTask?: number;
    };
  };

  /** SQS queue configuration */
  SQS: {
     /** Visibility timeout in seconds (should be > task timeout) */
    visibilityTimeoutSeconds: number;
    /** Message retention period in days */
    retentionPeriodDays: number;
    /** Max receive count before sending to DLQ */
    deadLetterQueueMaxReceiveCount: number;
  };

  /** Lambda function configuration */
  LAMBDA: {
    /** Chunker subscriber Lambda configuration */
    chunkerSubscriber: {
      timeoutSeconds: number;
      memorySizeMb: number;
    };
    /** Processor subscriber Lambda configuration */
    processorSubscriber: {
      timeoutSeconds: number;
      memorySizeMb: number;
    };
    /** Merger subscriber Lambda configuration */
    mergerSubscriber: {
      timeoutSeconds: number;
      memorySizeMb: number;
      /** How often to poll for chunk completion (in minutes) */
      pollIntervalMinutes: number;
    };
  };

  /**
   * Huron Person Integration Configuration
   * This is passed to the processor tasks as environment variables
   * Note: Most HURON_API fields are replaced by this comprehensive config
   * 
   * For API-based chunking via EventBridge schedule, configure:
   *   dataSource.people.fetchSchedule = {
   *     enabled: boolean,        // Enable/disable the schedule
   *     cronExpression: string   // AWS cron expression (e.g., 'cron(0 2 * * ? *)')
   *   }
   * 
   * The EventBridge schedule is only created when:
   * - dataSource.people.fetchSchedule exists
   * - fetchSchedule.enabled is true
   * - fetchSchedule.cronExpression is a valid cron expression
   */
  HURON_PERSON_CONFIG: Partial<HuronPersonConfig> | {
    /** 
     * Alternative: Provide JSON string path to config file in S3 or filesystem
     * Processor will load this instead of individual env vars
     */
    configPath?: string;
  };

  /** Bulk reset configuration - controls whether to perform a bulk reset (delete all existing records in source system) before syncing */
  BULK_RESET?: boolean;

  /** 
   * Trust previous storage configuration - controls whether to trust previously stored 
   * data (e.g., previous-input.ndjson) as the source of truth for existing records insofar
   * as they already exist in the target system or not), or pre-load all of the sourceIdentifier
   * values of every person in the target system before syncing and compare against those to
   * supplement the previous storage data. This is comprises a backup method of choosing 
   * between create vs patch when syncing person records to the target system.
   * .
   */
  TRUST_PREVIOUS_STORAGE?: boolean;

  /** 
   * Dry-run mode configuration - controls whether operations actually modify data
   * or just simulate the operations without making changes.
   */
  DRY_RUN?: {
    /** Lambda function dry-run settings */
    lambda?: {
      /** Chunker subscriber lambda - if true, sends messages to SQS without actually triggering tasks */
      chunker?: boolean;
      /** Processor subscriber lambda - if true, sends no messages to processor queue */
      processor?: boolean;
      /** Merger subscriber lambda - if true, sends messages to SQS without actually triggering tasks */
      merger?: boolean;
    };
    /** Task definition dry-run settings */
    taskdef?: {
      /** Chunker task - if true, simulates chunking without writing chunk files to S3 */
      chunker?: boolean;
      /** Processor task - if true, simulates person sync without making API calls or updating deltas */
      processor?: boolean;
      /** Merger task - if true, simulates merging without writing previous-input.ndjson or deleting chunks */
      merger?: boolean;
    };
  };
}
