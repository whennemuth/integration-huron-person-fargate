import { IContext } from '../../context/IContext';
import { MockDataTarget } from 'integration-huron-person';
import { Config } from 'integration-huron-person';
import { CrudOperation, FieldSet, PushOneParms, SinglePushResult } from 'integration-core';
import { DynamoDBClient, ScanCommand, DeleteItemCommand, DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';

/**
 * Mock Target State Table
 * 
 * Purpose: Stores simulated target system state for testing integration runs with source simulator.
 * 
 * Table Design:
 * - Partition Key (PK): `personId` - Unique person identifier (e.g., "U12345678")
 * - No Sort Key: One record per person (overwrite on update)
 * 
 * Attributes (matches real target system person schema):
 * - personId: string - BUID
 * - data: object - Full person record as it would exist in target system
 * - lastModified: string - ISO timestamp of last update
 * - createdAt: string - ISO timestamp when record was first created
 * - syncRunId: string - ISO timestamp of sync run that last modified this person
 * 
 * Access Patterns:
 * 1. Get person by ID: GetItem by personId
 * 2. Batch get persons: BatchGetItem by personIds
 * 3. Create/Update person: PutItem
 * 4. Delete person: DeleteItem
 * 5. List all persons: Scan (for validation/audit)
 * 
 * Usage:
 * When flags.useMockTarget is true, processors use MockDataTarget which writes to this table
 * instead of calling the real target API. This allows full end-to-end testing with source
 * simulator without affecting real target system data.
 */

export const DYNAMODB_TABLE_NAME = (context: IContext): string => {
  const { STACK_ID, TAGS: { Landscape } } = context;
  return `${STACK_ID}-mock-target-state-${Landscape.toLowerCase()}`;
};

export const DYNAMODB_PARTITION_KEY = 'personId';

/**
 * Record stored in MockTargetStateTable
 */
export interface MockTargetStateRecord {
  personId: string;
  data: Record<string, any>;
  createdAt: string;
  lastModified: string;
  syncRunId: string;
}

/**
 * MockTargetStateTable utility class
 * 
 * Wraps MockDataTarget with DynamoDB-specific table management operations.
 * Acts as a decorator/wrapper around the inner MockDataTarget instance from integration-huron-person.
 * 
 * Design:
 * - Composes with MockDataTarget (delegation pattern)
 * - Adds table-level operations: listAll(), truncate(), tableExists()
 * - Provides test harness for validation and state inspection
 * - Matches pattern used by PersonCurrentStateTable wrapping DynamoDBTable
 * 
 * Usage:
 * ```typescript
 * const mockTable = new MockTargetStateTable({ config, context });
 * 
 * // Push single person (delegates to inner MockDataTarget)
 * await mockTable.pushOne({ data: personFieldSet, crud: CrudOperation.CREATE });
 * 
 * // List all mock target records
 * const allRecords = await mockTable.listAll();
 * 
 * // Reset state before new run
 * await mockTable.truncate();
 * 
 * // Verify table exists
 * const exists = await mockTable.tableExists();
 * ```
 */
export class MockTargetStateTable {
  private mockDataTarget: MockDataTarget;
  private dynamoDbClient: DynamoDBClient;
  private tableName: string;

  constructor(params: {
    config: Config;
    context?: IContext;
    tableName?: string;
    syncRunId?: string;
    validateOnly?: boolean;
  }) {
    const { config, context, tableName, syncRunId, validateOnly } = params;

    // Resolve table name from context or param
    this.tableName = tableName || 
      (context ? DYNAMODB_TABLE_NAME(context) : '') ||
      process.env.DYNAMODB_MOCK_TARGET_STATE_TABLE_NAME || 
      '';

    if (!this.tableName) {
      throw new Error('MockTargetStateTable requires tableName, context, or DYNAMODB_MOCK_TARGET_STATE_TABLE_NAME');
    }

    // Create inner MockDataTarget instance
    this.mockDataTarget = new MockDataTarget({
      config,
      tableName: this.tableName,
      syncRunId,
      validateOnly
    });

    // Get region from config
    let region = process.env.REGION || 'us-east-1';
    if (config.storage.type === 's3') {
      region = (config.storage.config as any).region || region;
    } else if (config.storage.type === 'dynamodb') {
      region = (config.storage.config as any).region || region;
    }

    this.dynamoDbClient = new DynamoDBClient({ region });
  }

  /**
   * Push a single person record to mock target.
   * Delegates to inner MockDataTarget instance.
   * 
   * @param params - Push parameters (data and CRUD operation)
   * @returns Result of push operation
   */
  public async pushOne(params: PushOneParms): Promise<SinglePushResult> {
    return this.mockDataTarget.pushOne(params);
  }

  /**
   * List all records in mock target table.
   * Useful for validation and audit.
   * 
   * @returns Array of all MockTargetStateRecord entries
   */
  public async listAll(): Promise<MockTargetStateRecord[]> {
    const records: MockTargetStateRecord[] = [];
    let lastEvaluatedKey: Record<string, any> | undefined = undefined;

    do {
      const command: ScanCommand = new ScanCommand({
        TableName: this.tableName,
        ExclusiveStartKey: lastEvaluatedKey
      });

      const response = await this.dynamoDbClient.send(command);

      if (response.Items) {
        for (const item of response.Items) {
          records.push(unmarshall(item) as MockTargetStateRecord);
        }
      }

      lastEvaluatedKey = response.LastEvaluatedKey as Record<string, any> | undefined;
    } while (lastEvaluatedKey);

    return records;
  }

  /**
   * Truncate the table (delete all records).
   * WARNING: This is destructive and irreversible.
   * Use for resetting mock target state before a new test run.
   * 
   * @param chunkSize - Batch size for deletion (default: 25, max: 25)
   */
  public async truncate(chunkSize: number = 25): Promise<void> {
    console.log(`[MockTargetStateTable] Truncating table: ${this.tableName}`);
    
    const allRecords = await this.listAll();
    console.log(`[MockTargetStateTable] Found ${allRecords.length} records to delete`);

    if (allRecords.length === 0) {
      console.log(`[MockTargetStateTable] Table is already empty`);
      return;
    }

    let deleteCount = 0;
    for (let i = 0; i < allRecords.length; i += chunkSize) {
      const batch = allRecords.slice(i, i + chunkSize);
      
      await Promise.all(
        batch.map(record =>
          this.dynamoDbClient.send(new DeleteItemCommand({
            TableName: this.tableName,
            Key: marshall({ [DYNAMODB_PARTITION_KEY]: record.personId })
          }))
        )
      );

      deleteCount += batch.length;
      console.log(`[MockTargetStateTable] Deleted ${deleteCount}/${allRecords.length} records`);
    }

    console.log(`[MockTargetStateTable] ✓ Truncation complete`);
  }

  /**
   * Check if the table exists in DynamoDB.
   * Used for validation before attempting operations.
   * 
   * @returns true if table exists, false otherwise
   */
  public async tableExists(): Promise<boolean> {
    try {
      await this.dynamoDbClient.send(new DescribeTableCommand({
        TableName: this.tableName
      }));
      return true;
    } catch (error: any) {
      if (error.name === 'ResourceNotFoundException') {
        return false;
      }
      throw error;
    }
  }

  /**
   * Get the underlying MockDataTarget instance.
   * Useful for advanced operations or direct access.
   */
  public getInner(): MockDataTarget {
    return this.mockDataTarget;
  }

  /**
   * Get the table name.
   */
  public getTableName(): string {
    return this.tableName;
  }
}

// ==================== TEST HARNESS ====================

/**
 * Test harness for MockTargetStateTable
 * 
 * Environment Variables (with prefix MOCK_TARGET_STATE_TABLE_):
 * - REGION: AWS region
 * - DYNAMODB_MOCK_TARGET_STATE_TABLE_NAME: Table name
 * - STACK_ID: Stack identifier (for table name resolution)
 * - LANDSCAPE: Environment landscape (for table name resolution)
 * 
 * Tasks:
 * - list: List all records
 * - truncate: Delete all records
 * - test-push: Test CREATE/UPDATE/DELETE operations
 * - validate: Check if table exists
 */
async function main() {
  const { TestEnvironment } = await import('integration-core');
  const testEnvironment = TestEnvironment('MOCK_TARGET_STATE_TABLE');

  [
    'REGION',
    'DYNAMODB_MOCK_TARGET_STATE_TABLE_NAME',
    'STACK_ID',
    'LANDSCAPE'
  ].forEach(testEnvironment.getVarOrEmptyString);

  const {
    REGION: region,
    DYNAMODB_MOCK_TARGET_STATE_TABLE_NAME: tableName,
    STACK_ID: stackId,
    LANDSCAPE: landscape
  } = process.env;

  // Create mock context for table name resolution
  const context: IContext = {
    STACK_ID: stackId || 'test-stack',
    TAGS: { Landscape: landscape || 'dev' },
    REGION: region || 'us-east-1'
  } as IContext;

  // Create minimal config
  const { ConfigManager } = await import('integration-huron-person');
  const configManager = ConfigManager.getInstance();
  const config = await configManager
    .reset()
    .fromEnvironment()
    .getConfigAsync('people');

  const mockTable = new MockTargetStateTable({
    config,
    context,
    tableName,
    syncRunId: new Date().toISOString()
  });

  const task = process.argv[2] || 'list';

  console.log(`\n=== MockTargetStateTable Harness: ${task} ===`);
  console.log(`Table: ${mockTable.getTableName()}`);
  console.log(`Region: ${region}\n`);

  switch (task) {
    case 'list':
      const records = await mockTable.listAll();
      console.log(`Found ${records.length} records:`);
      records.forEach((record, i) => {
        console.log(`\n[${i + 1}] ${record.personId}:`);
        console.log(`  Created: ${record.createdAt}`);
        console.log(`  Modified: ${record.lastModified}`);
        console.log(`  Sync Run: ${record.syncRunId}`);
        console.log(`  Data: ${JSON.stringify(record.data).substring(0, 100)}...`);
      });
      break;

    case 'truncate':
      await mockTable.truncate();
      break;

    case 'test-push':
      console.log('Testing CREATE operation...');
      const testPerson: FieldSet = {
        fieldValues: [
          { buid: 'U99999999' },
          { firstName: 'Test' },
          { lastName: 'Person' }
        ]
      };
      
      const createResult = await mockTable.pushOne({
        data: testPerson,
        crud: CrudOperation.CREATE
      });
      console.log(`✓ CREATE result: ${createResult.status} - ${createResult.message}`);

      console.log('\nTesting UPDATE operation...');
      const updatedPerson: FieldSet = {
        fieldValues: [
          { buid: 'U99999999' },
          { firstName: 'Updated' },
          { lastName: 'Person' }
        ]
      };
      
      const updateResult = await mockTable.pushOne({
        data: updatedPerson,
        crud: CrudOperation.UPDATE
      });
      console.log(`✓ UPDATE result: ${updateResult.status} - ${updateResult.message}`);

      console.log('\nVerifying record...');
      const allRecords = await mockTable.listAll();
      const testRecord = allRecords.find(r => r.personId === 'U99999999');
      if (testRecord) {
        console.log(`✓ Found record: ${JSON.stringify(testRecord.data)}`);
      }

      console.log('\nTesting DELETE operation...');
      const deleteResult = await mockTable.pushOne({
        data: updatedPerson,
        crud: CrudOperation.DELETE
      });
      console.log(`✓ DELETE result: ${deleteResult.status} - ${deleteResult.message}`);
      break;

    case 'validate':
      const exists = await mockTable.tableExists();
      if (exists) {
        console.log(`✓ Table exists: ${mockTable.getTableName()}`);
        const records = await mockTable.listAll();
        console.log(`  Contains ${records.length} records`);
      } else {
        console.log(`✗ Table does not exist: ${mockTable.getTableName()}`);
        process.exit(1);
      }
      break;

    default:
      console.error(`Unknown task: ${task}`);
      console.log('Available tasks: list, truncate, test-push, validate');
      process.exit(1);
  }

  console.log('\n✓ Harness complete');
}

if (require.main === module) {
  main().catch(error => {
    console.error('Harness failed:', error);
    process.exit(1);
  });
}
