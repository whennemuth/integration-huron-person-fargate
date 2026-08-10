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
import {
  DYNAMODB_TABLE_NAME as personCurrentStateTableName,
  DYNAMODB_PARTITION_KEY as personCurrentStatePartitionKey,
  DYNAMODB_GSI_INDEX_NAME as personCurrentStateGSIIndexName,
  DYNAMODB_GSI_PARTITION_KEY as personCurrentStateGSIPartitionKey,
  DYNAMODB_GSI_SORT_KEY as personCurrentStateGSISortKey
} from '../src/PersonCurrentStateTable';
import {
  DYNAMODB_TABLE_NAME as personHistoryTableName,
  DYNAMODB_PARTITION_KEY as personHistoryPartitionKey,
  DYNAMODB_SORT_KEY as personHistorySortKey,
  DYNAMODB_GSI1_INDEX_NAME as personHistoryGSI1IndexName,
  DYNAMODB_GSI1_PARTITION_KEY as personHistoryGSI1PartitionKey,
  DYNAMODB_GSI1_SORT_KEY as personHistoryGSI1SortKey,
  DYNAMODB_GSI2_INDEX_NAME as personHistoryGSI2IndexName,
  DYNAMODB_GSI2_PARTITION_KEY as personHistoryGSI2PartitionKey,
  DYNAMODB_GSI2_SORT_KEY as personHistoryGSI2SortKey
} from '../src/PersonHistoryTable';

export enum TableResourceIds {
  STATISTICS_TABLE = 'StatisticsTable',
  ATOMIC_COUNTER_TABLE = 'AtomicCounterTable',
  PERSON_CURRENT_STATE_TABLE = 'PersonCurrentStateTable',
  PERSON_HISTORY_TABLE = 'PersonHistoryTable'
}
export interface ProcessorStatisticsTableProps {
  context: IContext;
  tags?: { [key: string]: string };
}

/**
 * Construct for DynamoDB tables used to store: 
 *   1) Processor statistics and error events.
 *   2) Atomic counters for various operations.
 *   3) Person current state (DynamoDB-based delta strategy, optional).
 *   4) Person history audit trail (DynamoDB-based delta strategy, optional).
 */
export class DynamoDbTables extends Construct {
  public statisticsTable: Table;
  public atomicCounterTable: Table;
  public personCurrentStateTable?: Table;
  public personHistoryTable?: Table;

  constructor(private params: { scope: Construct, id: string, props: ProcessorStatisticsTableProps }) {
    super(params.scope, params.id);

    this.createStatisticsTable();

    this.createAtomicCounterTable();

    // Conditionally create DynamoDB-based delta storage tables
    if (params.props.context.useDynamoDb) {
      this.createPersonCurrentStateTable();
      this.createPersonHistoryTable();
    }
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
   * DynamoDB table for storing current person hash state (DynamoDB-based delta strategy).
   * 
   * Table Design:
   * - Partition Key (PK): `personId` - Unique person identifier (e.g., "U12345678")
   * - No Sort Key: One record per person (overwrite on change)
   * 
   * Attributes:
   * - hash: string - Current computed hash value
   * - syncRunId: string - ISO timestamp of last sync that modified this person
   * 
   * Access Patterns:
   * 1. Batch fetch by personId: Processors use BatchGetItem to get previous hashes for delta computation
   * 2. Query by syncRunId via GSI: Merger uses this to find all persons seen in current sync for deletion detection
   * 
   * GSI (syncRunId-personId-index):
   * - PK: `syncRunId` - ISO timestamp of sync run
   * - SK: `personId` - Person identifier
   * - Use case: "Get all persons modified in sync run 2026-03-03T19:58:41.277Z"
   */
  private createPersonCurrentStateTable = () => {
    const { context, tags } = this.params.props;

    const { PERSON_CURRENT_STATE_TABLE } = TableResourceIds;

    // Create the person-current-state DynamoDB table
    this.personCurrentStateTable = new Table(this, PERSON_CURRENT_STATE_TABLE, {
      tableName: personCurrentStateTableName(context),
      partitionKey: {
        name: personCurrentStatePartitionKey,
        type: AttributeType.STRING,
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      encryption: TableEncryption.AWS_MANAGED,
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true,
        recoveryPeriodInDays: 35,
      },
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // GSI for querying by sync run (used by merger for deletion detection)
    this.personCurrentStateTable.addGlobalSecondaryIndex({
      indexName: personCurrentStateGSIIndexName,
      partitionKey: {
        name: personCurrentStateGSIPartitionKey,
        type: AttributeType.STRING,
      },
      sortKey: {
        name: personCurrentStateGSISortKey,
        type: AttributeType.STRING,
      },
    });
  }

  /**
   * DynamoDB table for storing person history audit trail (DynamoDB-based delta strategy).
   * 
   * Table Design:
   * - Partition Key (PK): `personId` - Unique person identifier
   * - Sort Key (SK): `syncRunId` - ISO timestamp of sync run (enables chronological ordering)
   * 
   * Attributes:
   * - hash: string - Hash value at this point in time
   * - changeType: 'NEW' | 'UPDATED' | 'DELETED' - Type of change
   * - previousHash?: string - Previous hash value (for UPDATED only)
   * 
   * Write Policy:
   * - NEW: First time person appears in source
   * - UPDATED: Hash changed from previous sync
   * - DELETED: Person removed from source (detected by merger)
   * - UNCHANGED: DO NOT WRITE (skipped entirely to save storage)
   * 
   * Access Patterns:
   * 1. Get person's complete history: Query by personId
   * 2. Get all changes in a sync run: Query GSI1 by syncRunId
   * 3. Get all changes of a specific type: Query GSI2 by changeType
   * 
   * GSI1 (syncRunId-changeType-index):
   * - PK: `syncRunId` - ISO timestamp
   * - SK: `changeType_personId` - Composite for filtering by change type
   * - Use case: "Get all NEW persons in sync 2026-03-03T19:58:41.277Z"
   * 
   * GSI2 (changeType-syncRunId-index):
   * - PK: `changeType` - 'NEW', 'UPDATED', or 'DELETED'
   * - SK: `syncRunId` - ISO timestamp
   * - Use case: "Get all DELETED persons in the last 30 days"
   */
  private createPersonHistoryTable = () => {
    const { context, tags } = this.params.props;

    const { PERSON_HISTORY_TABLE } = TableResourceIds;

    // Create the person-history DynamoDB table
    this.personHistoryTable = new Table(this, PERSON_HISTORY_TABLE, {
      tableName: personHistoryTableName(context),
      partitionKey: {
        name: personHistoryPartitionKey,
        type: AttributeType.STRING,
      },
      sortKey: {
        name: personHistorySortKey,
        type: AttributeType.STRING,
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      encryption: TableEncryption.AWS_MANAGED,
      pointInTimeRecoverySpecification: {
        pointInTimeRecoveryEnabled: true,
        recoveryPeriodInDays: 35,
      },
      removalPolicy: RemovalPolicy.DESTROY,
    });

    // GSI1: Query all changes in a specific sync run
    this.personHistoryTable.addGlobalSecondaryIndex({
      indexName: personHistoryGSI1IndexName,
      partitionKey: {
        name: personHistoryGSI1PartitionKey,
        type: AttributeType.STRING,
      },
      sortKey: {
        name: personHistoryGSI1SortKey,
        type: AttributeType.STRING,
      },
    });

    // GSI2: Query all changes of a specific type across runs
    this.personHistoryTable.addGlobalSecondaryIndex({
      indexName: personHistoryGSI2IndexName,
      partitionKey: {
        name: personHistoryGSI2PartitionKey,
        type: AttributeType.STRING,
      },
      sortKey: {
        name: personHistoryGSI2SortKey,
        type: AttributeType.STRING,
      },
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
      case TableResourceIds.PERSON_CURRENT_STATE_TABLE:
        if (!this.personCurrentStateTable) {
          throw new Error('PersonCurrentStateTable not created - useDynamoDb is false');
        }
        return this.personCurrentStateTable.grantReadWriteData(grantee);
      case TableResourceIds.PERSON_HISTORY_TABLE:
        if (!this.personHistoryTable) {
          throw new Error('PersonHistoryTable not created - useDynamoDb is false');
        }
        return this.personHistoryTable.grantReadWriteData(grantee);
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
      case TableResourceIds.PERSON_CURRENT_STATE_TABLE:
        if (!this.personCurrentStateTable) {
          throw new Error('PersonCurrentStateTable not created - useDynamoDb is false');
        }
        return this.personCurrentStateTable.grantReadData(grantee);
      case TableResourceIds.PERSON_HISTORY_TABLE:
        if (!this.personHistoryTable) {
          throw new Error('PersonHistoryTable not created - useDynamoDb is false');
        }
        return this.personHistoryTable.grantReadData(grantee);
      default:
        throw new Error(`Unknown table resource ID: ${tableResourceId}`);
    }
  }
}
