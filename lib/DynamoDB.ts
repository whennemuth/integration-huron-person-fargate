import { RemovalPolicy } from 'aws-cdk-lib';
import { AttributeType, BillingMode, Table, TableEncryption } from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';
import { 
  DYNAMODB_PARTITION_KEY as statisticsPartitionKey, 
  DYNAMODB_SECONDARY_PARTITION_KEY as statisticsSecondaryPartitionKey, 
  DYNAMODB_SORT_KEY as statisticsSortKey, 
  DYNAMODB_TABLE_NAME as statisticsTableName
} from '../src/statistics/StatisticsTable';
import {
  DYNAMODB_TABLE_NAME as atomicCounterTableName,
  DYNAMODB_PARTITION_KEY as atomicCounterPartitionKey
} from '../src/AtomicCounter';

export enum TableResourceIds {
  STATISTICS_TABLE = 'StatisticsTable',
  ATOMIC_COUNTER_TABLE = 'AtomicCounterTable'
}
export interface ProcessorStatisticsTableProps {
  context: IContext;
  tags?: { [key: string]: string };
}

/**
 * Construct for DynamoDB tables used to store: 
 *   1) Processor statistics and error events.
 *   2) Atomic counters for various operations.
 */
export class DynamoDbTables extends Construct {
  public statisticsTable: Table;
  public atomicCounterTable: Table;

  constructor(private params: { scope: Construct, id: string, props: ProcessorStatisticsTableProps }) {
    super(params.scope, params.id);

    this.createStatisticsTable();

    this.createAtomicCounterTable();
  }

  /**
   * DynamoDB table for storing processor statistics and error events.
   * 
   * Table Design:
   * - Partition Key (PK): `integrationTimestamp` - ISO timestamp of the processing run
   * - Sort Key (SK): `eventType` - Event type identifier
   * 
   * Item Types:
   * 1. Statistics Record (SK = "STATISTICS" or "STATISTICS-chunk-XXXX"):
   *    - Aggregated stats: SK = "STATISTICS" (1 record per integration run)
   *    - Chunk-specific stats: SK = "STATISTICS-chunk-0009" (1 record per chunk)
   *    - Chunk-specific records prevent parallel processors from overwriting each other
   * 
   * 2. Error Record (SK = "ERROR:<statusCode>:<timestamp>"):
   *    - Stores individual error events during processing
   *    - Multiple records per integration run (one per error)
   *    - SK includes timestamp for uniqueness and chronological sorting
   * 
   * Access Patterns:
   * 1. Get all data for a specific run: Query by PK = integrationTimestamp
   * 2. Get aggregated statistics: Query by PK = integrationTimestamp, SK = "STATISTICS"
   * 3. Get all chunk statistics: Query by PK = integrationTimestamp, SK begins_with "STATISTICS-chunk-"
   * 4. Get specific chunk statistics: Query by PK = integrationTimestamp, SK = "STATISTICS-chunk-0009"
   * 5. Get all errors for a run: Query by PK = integrationTimestamp, SK begins_with "ERROR:"
   * 6. Get specific error type for a run: Query by PK = integrationTimestamp, SK begins_with "ERROR:429"
   * 7. Query errors by type across all runs: Use GSI1 (errorType-timestamp-index)
   * 8. Query time-series statistics: Scan with filter (or use GSI for chronological queries)
   * 
   * GSI1 (errorType-timestamp-index):
   * - PK: `errorType` - Error classification (e.g., "ERROR:429", "ERROR:500", "STATISTICS")
   * - SK: `integrationTimestamp` - Enables chronological queries across runs
   * - Use case: "Get all throttling events across all runs in the past 30 days"
   */
  private createStatisticsTable = () => {
    const { context, tags } = this.params.props;

    const { STATISTICS_TABLE } = TableResourceIds;

    // Create the statistics DynamoDB table with pay-per-request billing
    this.statisticsTable = new Table(this, STATISTICS_TABLE, {
      tableName: statisticsTableName(context),
      partitionKey: {
        name: statisticsPartitionKey,
        type: AttributeType.STRING,
      },
      sortKey: {
        name: statisticsSortKey,
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
    this.statisticsTable.addGlobalSecondaryIndex({
      indexName: 'errorType-timestamp-index',
      partitionKey: {
        name: statisticsSecondaryPartitionKey,
        type: AttributeType.STRING,
      },
      sortKey: {
        name: statisticsPartitionKey,
        type: AttributeType.STRING,
      },
    });
  }

  /**
   * DynamoDB table for generating atomic counters used in various operations (e.g., generating unique chunk IDs).
   */
  private createAtomicCounterTable = () => {
    const { context, tags } = this.params.props;

    const { ATOMIC_COUNTER_TABLE } = TableResourceIds;

    // Create the atomic_counter DynamoDB table with pay-per-request billing
    this.atomicCounterTable = new Table(this, ATOMIC_COUNTER_TABLE, {
      tableName: atomicCounterTableName(context),
      partitionKey: {
        name: atomicCounterPartitionKey,
        type: AttributeType.STRING,
      },
      billingMode: BillingMode.PAY_PER_REQUEST, // No capacity planning needed
      encryption: TableEncryption.AWS_MANAGED, // Encrypt at rest
      removalPolicy: RemovalPolicy.DESTROY, // For now, delete table when stack is destroyed (change to RETAIN for production)
    });
  }

  /**
   * Grant read/write permissions to a principal
   */
  public grantReadWriteData(grantee: any, tableResourceId: TableResourceIds) {
    switch(tableResourceId) {
      case TableResourceIds.STATISTICS_TABLE:
        return this.statisticsTable.grantReadWriteData(grantee);
      case TableResourceIds.ATOMIC_COUNTER_TABLE:
        return this.atomicCounterTable.grantReadWriteData(grantee);
      default:
        throw new Error(`Unknown table resource ID: ${tableResourceId}`);
    }
  }

  /**
   * Grant read-only permissions to a principal
   */
  public grantReadData(grantee: any, tableResourceId: TableResourceIds) {
    switch(tableResourceId) {
      case TableResourceIds.STATISTICS_TABLE:
        return this.statisticsTable.grantReadData(grantee);
      case TableResourceIds.ATOMIC_COUNTER_TABLE:
        return this.atomicCounterTable.grantReadData(grantee);
      default:
        throw new Error(`Unknown table resource ID: ${tableResourceId}`);
    }
  }
}
