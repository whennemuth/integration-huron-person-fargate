import { RemovalPolicy } from 'aws-cdk-lib';
import { AttributeType, BillingMode, CfnTable, Table, TableEncryption } from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';
import { 
  DYNAMODB_PARTITION_KEY as statisticsPartitionKey, 
  DYNAMODB_SECONDARY_PARTITION_KEY as statisticsSecondaryPartitionKey, 
  DYNAMODB_SORT_KEY as statisticsSortKey, 
  DYNAMODB_TABLE_NAME as statisticsTableName,
  DYNAMODB_MOCK_TABLE_NAME as mockStatisticsTableName
} from '../src/dynamodb/StatisticsTable';
import {
  DYNAMODB_TABLE_NAME as atomicCounterTableName,
  DYNAMODB_PARTITION_KEY as atomicCounterPartitionKey
} from '../src/dynamodb/AtomicCounter';
import {
  DYNAMODB_TABLE_NAME as personCurrentStateTableName,
  DYNAMODB_MOCK_TABLE_NAME as mockPersonCurrentStateTableName,
  DYNAMODB_PARTITION_KEY as personCurrentStatePartitionKey,
  DYNAMODB_GSI_INDEX_NAME as personCurrentStateGSIIndexName,
  DYNAMODB_GSI_PARTITION_KEY as personCurrentStateGSIPartitionKey,
  DYNAMODB_GSI_SORT_KEY as personCurrentStateGSISortKey
} from '../src/dynamodb/PersonCurrentStateTable';
import {
  DYNAMODB_TABLE_NAME as personHistoryTableName,
  DYNAMODB_MOCK_TABLE_NAME as mockPersonHistoryTableName,
  DYNAMODB_PARTITION_KEY as personHistoryPartitionKey,
  DYNAMODB_SORT_KEY as personHistorySortKey,
  DYNAMODB_GSI1_INDEX_NAME as personHistoryGSI1IndexName,
  DYNAMODB_GSI1_PARTITION_KEY as personHistoryGSI1PartitionKey,
  DYNAMODB_GSI1_SORT_KEY as personHistoryGSI1SortKey,
  DYNAMODB_GSI2_INDEX_NAME as personHistoryGSI2IndexName,
  DYNAMODB_GSI2_PARTITION_KEY as personHistoryGSI2PartitionKey,
  DYNAMODB_GSI2_SORT_KEY as personHistoryGSI2SortKey
} from '../src/dynamodb/PersonHistoryTable';
import {
  DYNAMODB_TABLE_NAME as mockTargetPersonTableName,
  DYNAMODB_PARTITION_KEY as mockTargetPersonPartitionKey
} from '../src/dynamodb/MockTargetPersonTable';
import {
  DYNAMODB_TABLE_NAME as personRecordProcessorLogTableName,
  DYNAMODB_PARTITION_KEY as personRecordProcessorLogPartitionKey,
  DYNAMODB_SORT_KEY as personRecordProcessorLogSortKey
} from '../src/dynamodb/PersonRecordProcessorLogTable';

export enum TableResourceIds {
  STATISTICS_TABLE = 'StatisticsTable',
  ATOMIC_COUNTER_TABLE = 'AtomicCounterTable',
  PERSON_CURRENT_STATE_TABLE = 'PersonCurrentStateTable',
  PERSON_HISTORY_TABLE = 'PersonHistoryTable',
  MOCK_TARGET_PERSON_TABLE = 'MockTargetPersonTable',
  MOCK_STATISTICS_TABLE = 'MockStatisticsTable',
  MOCK_PERSON_CURRENT_STATE_TABLE = 'MockPersonCurrentStateTable',
  MOCK_PERSON_HISTORY_TABLE = 'MockPersonHistoryTable',
  PERSON_RECORD_PROCESSOR_LOG_TABLE = 'PersonRecordProcessorLogTable'
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
 *   5) Mock target state (for testing with source simulator, always created).
 */
export class DynamoDbTables extends Construct {
  public statisticsTable: Table;
  public atomicCounterTable: Table;
  public personCurrentStateTable?: Table;
  public personHistoryTable?: Table;
  public mockTargetPersonTable: Table;
  public mockStatisticsTable: Table;
  public mockPersonCurrentStateTable?: Table;
  public mockPersonHistoryTable?: Table;
  public personRecordProcessorLogTable: Table;
  mockConstruct: Construct;

  constructor(private params: { scope: Construct, id: string, props: ProcessorStatisticsTableProps }) {
    super(params.scope, params.id);

    // Nested under `this` (not params.scope) so mock tables' aws:cdk:path shows as
    // App/DynamoDb/Mocks/... rather than a sibling of DynamoDb
    this.mockConstruct = new Construct(this, 'MockTables');

    this.createStatisticsTable();

    this.createAtomicCounterTable();

    this.createMockTargetPersonTable();

    this.createMockStatisticsTable();

    this.createPersonRecordProcessorLogTable();

    // Conditionally create DynamoDB-based delta storage tables
    // Default to 'dynamodb' mode if PREVIOUS_STORAGE_TYPE is not specified
    const previousStorageType = params.props.context.PREVIOUS_STORAGE_TYPE || 'dynamodb';
    if (previousStorageType === 'dynamodb') {
      this.createPersonCurrentStateTable();
      this.createPersonHistoryTable();
      this.createMockPersonCurrentStateTable();
      this.createMockPersonHistoryTable();
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
   * Shared log table for ALL personRecordProcessor customizations (see
   * src/processing/custom/AbstractCustomPersonProcessor.ts).
   *
   * Table Design:
   * - Partition Key (PK): `customization` - identifies which customization wrote the entry
   * - Sort Key (SK): `sortKey` - `${isoTimestamp}#${personid}`, chronologically browsable and
   *   collision-free per person
   * - `data`: generic JSON blob whose shape is defined by the writing customization
   *
   * Kept as a single shared table (not one per customization, no mock variant) so future
   * customizations require no CDK/schema changes.
   */
  private createPersonRecordProcessorLogTable = () => {
    const { context } = this.params.props;

    const { PERSON_RECORD_PROCESSOR_LOG_TABLE } = TableResourceIds;

    this.personRecordProcessorLogTable = new Table(this, PERSON_RECORD_PROCESSOR_LOG_TABLE, {
      tableName: personRecordProcessorLogTableName(context),
      partitionKey: {
        name: personRecordProcessorLogPartitionKey,
        type: AttributeType.STRING,
      },
      sortKey: {
        name: personRecordProcessorLogSortKey,
        type: AttributeType.STRING,
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      encryption: TableEncryption.AWS_MANAGED,
      removalPolicy: RemovalPolicy.DESTROY,
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
   * DynamoDB table for storing mock target system state (for testing with source simulator).
   * 
   * Table Design:
   * - Partition Key (PK): `personId` - Unique person identifier
   * - No Sort Key: One record per person (overwrite on update)
   * 
   * Attributes:
   * - personId: string - BUID
   * - data: object - Full person record as it would exist in target system
   * - lastModified: string - ISO timestamp of last update
   * - createdAt: string - ISO timestamp when record was first created
   * - syncRunId: string - ISO timestamp of sync run that last modified this person
   * 
   * Access Patterns:
   * 1. Get person: GetItem by personId
   * 2. Batch get: BatchGetItem by personIds
   * 3. Create/Update: PutItem
   * 4. Delete: DeleteItem
   * 5. List all: Scan
   * 
   * Purpose:
   * When flags.useMockTarget is true, processors use MockPersonDataTarget which writes to this
   * table instead of calling the real target API. This allows full end-to-end testing
   * with source simulator without affecting real target system data.
   */
  private createMockTargetPersonTable = () => {
    const { context, tags } = this.params.props;

    const { MOCK_TARGET_PERSON_TABLE } = TableResourceIds;

    this.mockTargetPersonTable = new Table(this.mockConstruct, MOCK_TARGET_PERSON_TABLE, {
      tableName: mockTargetPersonTableName(context),
      partitionKey: {
        name: mockTargetPersonPartitionKey,
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
    // Pinned so the construct-path move doesn't change dependents' Ref values (IAM policies, task defs)
    (this.mockTargetPersonTable.node.defaultChild as CfnTable).overrideLogicalId('AppDynamoDbMockTargetPersonTable2BE0D169');
  }

  /**
   * Isolated statistics table for mocked (source simulator + mock target) runs.
   * Only bulk STATISTICS/ERROR/CHUNK_STATUS records are redirected here - FLAGS/METADATA
   * control-plane records always stay in the real StatisticsTable so every phase can
   * bootstrap discovery of useMockTarget from one well-known location.
   */
  private createMockStatisticsTable = () => {
    const { context } = this.params.props;

    const { MOCK_STATISTICS_TABLE } = TableResourceIds;

    this.mockStatisticsTable = new Table(this.mockConstruct, MOCK_STATISTICS_TABLE, {
      tableName: mockStatisticsTableName(context),
      partitionKey: {
        name: statisticsPartitionKey,
        type: AttributeType.STRING,
      },
      sortKey: {
        name: statisticsSortKey,
        type: AttributeType.STRING,
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      encryption: TableEncryption.AWS_MANAGED,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    // Pinned so the construct-path move doesn't change dependents' Ref values (IAM policies, task defs)
    (this.mockStatisticsTable.node.defaultChild as CfnTable).overrideLogicalId('AppDynamoDbMockStatisticsTable4840F10E');

    this.mockStatisticsTable.addGlobalSecondaryIndex({
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
   * Isolated PersonCurrentState table for mocked runs (see createMockStatisticsTable comment).
   */
  private createMockPersonCurrentStateTable = () => {
    const { context } = this.params.props;

    const { MOCK_PERSON_CURRENT_STATE_TABLE } = TableResourceIds;

    this.mockPersonCurrentStateTable = new Table(this.mockConstruct, MOCK_PERSON_CURRENT_STATE_TABLE, {
      tableName: mockPersonCurrentStateTableName(context),
      partitionKey: {
        name: personCurrentStatePartitionKey,
        type: AttributeType.STRING,
      },
      billingMode: BillingMode.PAY_PER_REQUEST,
      encryption: TableEncryption.AWS_MANAGED,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    // Pinned so the construct-path move doesn't change dependents' Ref values (IAM policies, task defs)
    (this.mockPersonCurrentStateTable.node.defaultChild as CfnTable).overrideLogicalId('AppDynamoDbMockPersonCurrentStateTable93F5EF37');

    this.mockPersonCurrentStateTable.addGlobalSecondaryIndex({
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
   * Isolated PersonHistory table for mocked runs (see createMockStatisticsTable comment).
   */
  private createMockPersonHistoryTable = () => {
    const { context } = this.params.props;

    const { MOCK_PERSON_HISTORY_TABLE } = TableResourceIds;

    this.mockPersonHistoryTable = new Table(this.mockConstruct, MOCK_PERSON_HISTORY_TABLE, {
      tableName: mockPersonHistoryTableName(context),
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
      removalPolicy: RemovalPolicy.DESTROY,
    });
    // Pinned so the construct-path move doesn't change dependents' Ref values (IAM policies, task defs)
    (this.mockPersonHistoryTable.node.defaultChild as CfnTable).overrideLogicalId('AppDynamoDbMockPersonHistoryTableE99331A6');

    this.mockPersonHistoryTable.addGlobalSecondaryIndex({
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

    this.mockPersonHistoryTable.addGlobalSecondaryIndex({
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
          throw new Error('PersonCurrentStateTable not created - PREVIOUS_STORAGE_TYPE is not \'dynamodb\'');
        }
        return this.personCurrentStateTable.grantReadWriteData(grantee);
      case TableResourceIds.PERSON_HISTORY_TABLE:
        if (!this.personHistoryTable) {
          throw new Error('PersonHistoryTable not created - PREVIOUS_STORAGE_TYPE is not \'dynamodb\'');
        }
        return this.personHistoryTable.grantReadWriteData(grantee);
      case TableResourceIds.MOCK_TARGET_PERSON_TABLE:
        return this.mockTargetPersonTable.grantReadWriteData(grantee);
      case TableResourceIds.MOCK_STATISTICS_TABLE:
        return this.mockStatisticsTable.grantReadWriteData(grantee);
      case TableResourceIds.MOCK_PERSON_CURRENT_STATE_TABLE:
        if (!this.mockPersonCurrentStateTable) {
          throw new Error('MockPersonCurrentStateTable not created - PREVIOUS_STORAGE_TYPE is not \'dynamodb\'');
        }
        return this.mockPersonCurrentStateTable.grantReadWriteData(grantee);
      case TableResourceIds.MOCK_PERSON_HISTORY_TABLE:
        if (!this.mockPersonHistoryTable) {
          throw new Error('MockPersonHistoryTable not created - PREVIOUS_STORAGE_TYPE is not \'dynamodb\'');
        }
        return this.mockPersonHistoryTable.grantReadWriteData(grantee);
      case TableResourceIds.PERSON_RECORD_PROCESSOR_LOG_TABLE:
        return this.personRecordProcessorLogTable.grantReadWriteData(grantee);
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
          throw new Error('PersonCurrentStateTable not created - PREVIOUS_STORAGE_TYPE is not \'dynamodb\'');
        }
        return this.personCurrentStateTable.grantReadData(grantee);
      case TableResourceIds.PERSON_HISTORY_TABLE:
        if (!this.personHistoryTable) {
          throw new Error('PersonHistoryTable not created - PREVIOUS_STORAGE_TYPE is not \'dynamodb\'');
        }
        return this.personHistoryTable.grantReadData(grantee);
      case TableResourceIds.MOCK_TARGET_PERSON_TABLE:
        return this.mockTargetPersonTable.grantReadData(grantee);
      case TableResourceIds.MOCK_STATISTICS_TABLE:
        return this.mockStatisticsTable.grantReadData(grantee);
      case TableResourceIds.MOCK_PERSON_CURRENT_STATE_TABLE:
        if (!this.mockPersonCurrentStateTable) {
          throw new Error('MockPersonCurrentStateTable not created - PREVIOUS_STORAGE_TYPE is not \'dynamodb\'');
        }
        return this.mockPersonCurrentStateTable.grantReadData(grantee);
      case TableResourceIds.MOCK_PERSON_HISTORY_TABLE:
        if (!this.mockPersonHistoryTable) {
          throw new Error('MockPersonHistoryTable not created - PREVIOUS_STORAGE_TYPE is not \'dynamodb\'');
        }
        return this.mockPersonHistoryTable.grantReadData(grantee);
      case TableResourceIds.PERSON_RECORD_PROCESSOR_LOG_TABLE:
        return this.personRecordProcessorLogTable.grantReadData(grantee);
      default:
        throw new Error(`Unknown table resource ID: ${tableResourceId}`);
    }
  }
}
