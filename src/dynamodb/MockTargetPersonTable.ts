import { IContext } from '../../context/IContext';
import { DynamoDBClient, DescribeTableCommand } from '@aws-sdk/client-dynamodb';
import { DeleteCommand, DynamoDBDocumentClient, GetCommand, PutCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';

/**
 * Mock Target Person Table
 *
 * Purpose: The storage behind the target simulator (src/target-simulator/TargetSimulator.ts) - the
 * HTTP stand-in for the Huron person API that a mock landscape's ECS tasks call, unknowingly, in
 * place of the real target system. Exists only in a mock landscape (see isMockLandscape in
 * src/Utils.ts).
 *
 * NOTE: Deliberately has no dependency on integration-huron-person (or anything else heavy), since
 * it is bundled into the target simulator Lambda.
 *
 * Table Design:
 * - Partition Key (PK): `personId` - the person's sourceIdentifier (BUID, e.g. "U12345678")
 * - No Sort Key: One record per person (overwrite on update)
 *
 * Attributes:
 * - personId: string - sourceIdentifier (BUID)
 * - hrn: string - HRN the simulator assigned at creation (derived from sourceIdentifier)
 * - data: object - Full person record as the target system would return it (includes hrn, active)
 * - createdAt: string - ISO timestamp when the record was first created
 * - lastModified: string - ISO timestamp of the last create/update/deactivation
 * - deactivated: boolean - True if the person has been soft-deleted (Huron only supports soft deletes)
 * - deactivatedAt: string - ISO timestamp of the deactivation (absent once reactivated)
 */

export const DYNAMODB_TABLE_NAME = (context: IContext): string => {
  const { STACK_ID, TAGS: { Landscape } } = context;
  return `${STACK_ID}-mock-target-person-${Landscape.toLowerCase()}`;
};

export const DYNAMODB_PARTITION_KEY = 'personId';

/**
 * Record stored in MockTargetPersonTable
 */
export interface MockTargetPersonRecord {
  personId: string;
  hrn: string;
  data: Record<string, any>;
  createdAt: string;
  lastModified: string;
  deactivated?: boolean;
  deactivatedAt?: string;
}

/**
 * Minimal storage contract the target simulator needs - implemented by MockTargetPersonTable for
 * DynamoDB, and by in-memory stores in tests.
 */
export interface MockTargetPersonStore {
  get(personId: string): Promise<MockTargetPersonRecord | undefined>;
  put(record: MockTargetPersonRecord): Promise<void>;
  listAll(): Promise<MockTargetPersonRecord[]>;
}

export class MockTargetPersonTable implements MockTargetPersonStore {
  private client: DynamoDBDocumentClient;
  private rawClient: DynamoDBClient;
  private tableName: string;

  constructor(params: { context?: IContext; tableName?: string; region?: string }) {
    const { context, tableName, region } = params;

    this.tableName = tableName ||
      (context ? DYNAMODB_TABLE_NAME(context) : '') ||
      process.env.DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME ||
      '';

    if (!this.tableName) {
      throw new Error('MockTargetPersonTable requires tableName, context, or DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME');
    }

    this.rawClient = new DynamoDBClient({ region: region || context?.REGION || process.env.REGION });
    this.client = DynamoDBDocumentClient.from(this.rawClient, { marshallOptions: { removeUndefinedValues: true } });
  }

  public async get(personId: string): Promise<MockTargetPersonRecord | undefined> {
    const { Item } = await this.client.send(new GetCommand({
      TableName: this.tableName,
      Key: { [DYNAMODB_PARTITION_KEY]: personId }
    }));
    return Item as MockTargetPersonRecord | undefined;
  }

  public async put(record: MockTargetPersonRecord): Promise<void> {
    await this.client.send(new PutCommand({ TableName: this.tableName, Item: record }));
  }

  /**
   * List all records in the table (full scan).
   */
  public async listAll(): Promise<MockTargetPersonRecord[]> {
    const records: MockTargetPersonRecord[] = [];
    let lastEvaluatedKey: Record<string, any> | undefined = undefined;

    do {
      const response: any = await this.client.send(new ScanCommand({
        TableName: this.tableName,
        ExclusiveStartKey: lastEvaluatedKey
      }));
      records.push(...((response.Items ?? []) as MockTargetPersonRecord[]));
      lastEvaluatedKey = response.LastEvaluatedKey;
    } while (lastEvaluatedKey);

    return records;
  }

  /**
   * Truncate the table (delete all records).
   * WARNING: This is destructive and irreversible.
   * Use for resetting mock target state before a new test run.
   *
   * @param chunkSize - Number of concurrent deletes per batch (default: 25)
   */
  public async truncate(chunkSize: number = 25): Promise<void> {
    console.log(`[MockTargetPersonTable] Truncating table: ${this.tableName}`);

    const allRecords = await this.listAll();
    console.log(`[MockTargetPersonTable] Found ${allRecords.length} records to delete`);

    if (allRecords.length === 0) {
      console.log(`[MockTargetPersonTable] Table is already empty`);
      return;
    }

    let deleteCount = 0;
    for (let i = 0; i < allRecords.length; i += chunkSize) {
      const batch = allRecords.slice(i, i + chunkSize);

      await Promise.all(
        batch.map(record =>
          this.client.send(new DeleteCommand({
            TableName: this.tableName,
            Key: { [DYNAMODB_PARTITION_KEY]: record.personId }
          }))
        )
      );

      deleteCount += batch.length;
      console.log(`[MockTargetPersonTable] Deleted ${deleteCount}/${allRecords.length} records`);
    }

    console.log(`[MockTargetPersonTable] ✓ Truncation complete`);
  }

  /**
   * Check if the table exists in DynamoDB.
   * Used for validation before attempting operations.
   *
   * @returns true if table exists, false otherwise
   */
  public async tableExists(): Promise<boolean> {
    try {
      await this.rawClient.send(new DescribeTableCommand({
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
   * Get the table name.
   */
  public getTableName(): string {
    return this.tableName;
  }
}

// ==================== TEST HARNESS ====================

/**
 * Test harness for MockTargetPersonTable
 *
 * Environment Variables (with prefix MOCK_TARGET_PERSON_TABLE_):
 * - REGION: AWS region
 * - DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME: Table name
 * - STACK_ID: Stack identifier (for table name resolution)
 * - LANDSCAPE: Environment landscape (for table name resolution)
 * - MOCK_TARGET_PERSON_TABLE_TASK: Task to run (defaults to "list")
 *
 * Tasks:
 * - list: List all records
 * - truncate: Delete all records
 * - validate: Check if table exists
 */
export async function main() {
  const { TestEnvironment } = await import('integration-core');
  const testEnvironment = TestEnvironment('MOCK_TARGET_PERSON_TABLE');

  [
    'REGION',
    'DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME',
    'STACK_ID',
    'LANDSCAPE',
    'MOCK_TARGET_PERSON_TABLE_TASK'
  ].forEach(testEnvironment.getVarOrEmptyString);

  const {
    REGION: region,
    DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME: tableName,
    STACK_ID: stackId,
    LANDSCAPE: landscape,
    MOCK_TARGET_PERSON_TABLE_TASK: task = 'list'
  } = process.env;

  // Create mock context for table name resolution
  const context: IContext = {
    STACK_ID: stackId || 'test-stack',
    TAGS: { Landscape: landscape || 'dev' },
    REGION: region || 'us-east-1'
  } as IContext;

  const mockTable = new MockTargetPersonTable({ context, tableName, region });

  console.log(`\n=== MockTargetPersonTable Harness: ${task} ===`);
  console.log(`Table: ${mockTable.getTableName()}`);
  console.log(`Region: ${region}\n`);

  switch (task) {
    case 'list':
      const records = await mockTable.listAll();
      console.log(`Found ${records.length} records:`);
      records.forEach((record, i) => {
        console.log(`\n[${i + 1}] ${record.personId} (${record.hrn}):`);
        console.log(`  Created: ${record.createdAt}`);
        console.log(`  Modified: ${record.lastModified}`);
        console.log(`  Deactivated: ${record.deactivated ? record.deactivatedAt : 'no'}`);
        console.log(`  Data: ${JSON.stringify(record.data).substring(0, 100)}...`);
      });
      break;

    case 'truncate':
      await mockTable.truncate();
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
      console.log('Available tasks: list, truncate, validate');
      process.exit(1);
  }

  console.log('\n✓ Harness complete');
}

if (require.main === module) {
  main();
}
