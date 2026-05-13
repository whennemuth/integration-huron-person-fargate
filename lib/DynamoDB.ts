import { RemovalPolicy } from 'aws-cdk-lib';
import { AttributeType, BillingMode, Table, TableEncryption } from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';

export const DYNAMODB_TABLE_NAME = (context: IContext) => `${context.STACK_ID}-statistics`;
export const DYNAMODB_PARTITION_KEY = 'integrationTimestamp';
export const DYNAMODB_SECONDARY_PARTITION_KEY = 'errorType';
export const DYNAMODB_SORT_KEY = 'eventType';
export const DYNAMODB_GSI_INDEX_NAME = 'errorType-timestamp-index';
export interface ProcessorStatisticsTableProps {
  context: IContext;
  tags?: { [key: string]: string };
}

/**
 * DynamoDB table for storing processor statistics and error events.
 * 
 * Table Design:
 * - Partition Key (PK): `integrationTimestamp` - ISO timestamp of the processing run
 * - Sort Key (SK): `eventType` - Event type identifier (e.g., "STATISTICS", "ERROR:429", "ERROR:500")
 * 
 * Item Types:
 * 1. Statistics Record (SK = "STATISTICS"):
 *    - Stores overall sync statistics for a single processor run
 *    - 1 record per integration run
 * 
 * 2. Error Record (SK = "ERROR:<statusCode>:<timestamp>"):
 *    - Stores individual error events during processing
 *    - Multiple records per integration run (one per error)
 *    - SK includes timestamp for uniqueness and chronological sorting
 * 
 * Access Patterns:
 * 1. Get all data for a specific run: Query by PK = integrationTimestamp
 * 2. Get statistics for a run: Query by PK = integrationTimestamp, SK = "STATISTICS"
 * 3. Get all errors for a run: Query by PK = integrationTimestamp, SK begins_with "ERROR:"
 * 4. Get specific error type for a run: Query by PK = integrationTimestamp, SK begins_with "ERROR:429"
 * 5. Query errors by type across all runs: Use GSI1 (errorType-timestamp-index)
 * 6. Query time-series statistics: Scan with filter (or use GSI for chronological queries)
 * 
 * GSI1 (errorType-timestamp-index):
 * - PK: `errorType` - Error classification (e.g., "ERROR:429", "ERROR:500", "STATISTICS")
 * - SK: `integrationTimestamp` - Enables chronological queries across runs
 * - Use case: "Get all throttling events across all runs in the past 30 days"
 */
export class ProcessorStatisticsTable extends Construct {
  public readonly table: Table;

  constructor(scope: Construct, id: string, props: ProcessorStatisticsTableProps) {
    super(scope, id);

    const { context, tags } = props;

    // Create DynamoDB table with pay-per-request billing
    this.table = new Table(this, 'StatisticsTable', {
      tableName: DYNAMODB_TABLE_NAME(context),
      partitionKey: {
        name: DYNAMODB_PARTITION_KEY,
        type: AttributeType.STRING,
      },
      sortKey: {
        name: DYNAMODB_SORT_KEY,
        type: AttributeType.STRING,
      },
      billingMode: BillingMode.PAY_PER_REQUEST, // No capacity planning needed
      encryption: TableEncryption.AWS_MANAGED, // Encrypt at rest
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true, // Enable PITR for backup and restore
        recoveryPeriodInDays: 35, // Retain PITR data for 35 days (max allowed)
      },
      removalPolicy: RemovalPolicy.DESTROY, // For now, delete table when stack is destroyed (change to RETAIN for production)
    });

    // GSI for querying errors by type across all integration runs
    // Example: "Get all 429 throttling events in the last 30 days"
    this.table.addGlobalSecondaryIndex({
      indexName: 'errorType-timestamp-index',
      partitionKey: {
        name: DYNAMODB_SECONDARY_PARTITION_KEY,
        type: AttributeType.STRING,
      },
      sortKey: {
        name: DYNAMODB_PARTITION_KEY,
        type: AttributeType.STRING,
      },
    });
  }

  /**
   * Grant read/write permissions to a principal
   */
  public grantReadWriteData(grantee: any) {
    return this.table.grantReadWriteData(grantee);
  }

  /**
   * Grant read-only permissions to a principal
   */
  public grantReadData(grantee: any) {
    return this.table.grantReadData(grantee);
  }
}
