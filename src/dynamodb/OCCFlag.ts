import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, GetCommand, UpdateCommand, UpdateCommandInput } from "@aws-sdk/lib-dynamodb";
import { TestEnvironment } from "integration-core";

/**
 * Generic optimistic concurrency control (OCC) primitive backed by a DynamoDB conditional write,
 * usable against any existing table's any attribute - no dedicated table/schema required.
 *
 * OCC is often confused with atomicity, but they solve different problems:
 * - Atomicity: whether a single operation completes wholly or not at all (no partial writes).
 * - OCC / mutual exclusion: among multiple concurrent contenders attempting the same operation,
 *   ensuring exactly one succeeds and all others are told they lost, so they can decide what to do
 *   next (retry, abort, defer). There's no upfront locking - all contenders attempt the write
 *   freely, and DynamoDB detects and rejects the losers at commit time via a ConditionExpression.
 *
 * Use cases:
 * - Ensuring only one process triggers a task (e.g., last processor triggers merger)
 * - Leader election / "first writer wins" claims in distributed systems
 * - One-time initialization guards
 * - Idempotent operation guards
 *
 * Example:
 * ```typescript
 * const claim = new OCCFlag({
 *   tableName: 'my-stack-statistics-preview',
 *   region: 'us-east-2',
 *   partitionKeyName: 'integrationTimestamp',
 *   partitionKeyValue: syncRunId,
 *   sortKeyName: 'eventType',
 *   sortKeyValue: 'METADATA',
 *   attributeName: 'finalOffsetProcessed'
 * });
 *
 * await claim.update(offset,
 *   async () => { // won the claim },
 *   async () => { // lost the claim - another task already set it }
 * );
 * ```
 */
export class OCCFlag {
  private client: DynamoDBDocumentClient;
  private readonly tableName: string;
  private readonly key: Record<string, string>;
  private readonly attributeName: string;

  constructor(params: {
    tableName: string,
    region?: string,
    partitionKeyName: string,
    partitionKeyValue: string,
    sortKeyName?: string,
    sortKeyValue?: string,
    attributeName: string
  }) {
    const { tableName, region, partitionKeyName, partitionKeyValue, sortKeyName, sortKeyValue, attributeName } = params;
    this.client = DynamoDBDocumentClient.from(new DynamoDBClient({ region }));
    this.tableName = tableName;
    this.attributeName = attributeName;
    this.key = { [partitionKeyName]: partitionKeyValue };
    if (sortKeyName) {
      if (sortKeyValue === undefined) {
        throw new Error('sortKeyValue is required when sortKeyName is provided');
      }
      this.key[sortKeyName] = sortKeyValue;
    }
  }

  /**
   * Attempt to set the guarded attribute. Uses a conditional write so exactly one concurrent
   * caller succeeds; all others are told they lost via onClaimFailure instead of silently
   * overwriting or racing on a read-then-write basis.
   *
   * @param value - The value to write to the guarded attribute
   * @param onClaimSuccess - Called after this caller wins the claim
   * @param onClaimFailure - Called when another caller already holds the claim
   * @throws Error if the DynamoDB operation fails for reasons other than losing the claim
   */
  public update = async (
    value: any,
    onClaimSuccess: () => Promise<void>,
    onClaimFailure: () => Promise<void>
  ): Promise<void> => {
    const { tableName, key, attributeName } = this;
    const input = {
      TableName: tableName,
      Key: key,
      UpdateExpression: 'SET #attr = :value',
      ConditionExpression: 'attribute_not_exists(#attr)',
      ExpressionAttributeNames: { '#attr': attributeName },
      ExpressionAttributeValues: { ':value': value },
    } satisfies UpdateCommandInput;

    try {
      await this.client.send(new UpdateCommand(input));
      console.log(`✅ OCCFlag '${attributeName}' claimed successfully with value:`, value);
      await onClaimSuccess();
    } catch (error: any) {
      if (error.name === 'ConditionalCheckFailedException') {
        console.log(`⚠️  OCCFlag '${attributeName}' already claimed by another caller`);
        await onClaimFailure();
      } else {
        throw error;
      }
    }
  }

  /**
   * Read the guarded attribute's current value, or undefined if not set. Useful for harness
   * debugging and for TTL/staleness checks layered on top of a claim (a separate concern from OCC
   * itself - OCC only resolves who wins among concurrent contenders, not what to do about a
   * winner that later disappears without releasing).
   */
  public getValue = async (): Promise<any> => {
    const { tableName, key, attributeName } = this;
    const result = await this.client.send(new GetCommand({ TableName: tableName, Key: key }));
    return result.Item?.[attributeName];
  }

  /**
   * Remove just the guarded attribute, allowing it to be claimed again. No-op if not currently
   * set. Useful for test reset and staleness recovery (see getValue's doc comment).
   */
  public unset = async (): Promise<void> => {
    const { tableName, key, attributeName } = this;
    try {
      await this.client.send(new UpdateCommand({
        TableName: tableName,
        Key: key,
        UpdateExpression: 'REMOVE #attr',
        ConditionExpression: 'attribute_exists(#attr)',
        ExpressionAttributeNames: { '#attr': attributeName }
      }));
      console.log(`🗑️  OCCFlag '${attributeName}' unset successfully`);
    } catch (error: any) {
      if (error.name !== 'ConditionalCheckFailedException') {
        throw error;
      }
    }
  }
}

enum TASK {
  UPDATE = 'update',
  UNSET = 'unset',
  GET_VALUE = 'get-value',
  TEST_RACE = 'test-race'
}

if (require.main === module) {
  const testEnvironment = TestEnvironment('OCC_FLAG');
  [
    'TASK',
    'TABLE_NAME',
    'REGION',
    'PARTITION_KEY_NAME',
    'PARTITION_KEY_VALUE',
    'SORT_KEY_NAME',
    'SORT_KEY_VALUE',
    'ATTRIBUTE_NAME',
    'VALUE'
  ].forEach(testEnvironment.getVarOrEmptyString);

  const { UPDATE, UNSET, GET_VALUE, TEST_RACE } = TASK;
  const {
    TASK: task = UPDATE,
    TABLE_NAME,
    REGION,
    PARTITION_KEY_NAME,
    PARTITION_KEY_VALUE,
    SORT_KEY_NAME,
    SORT_KEY_VALUE,
    ATTRIBUTE_NAME,
    VALUE
  } = process.env;

  if (!TABLE_NAME || !REGION || !PARTITION_KEY_NAME || !PARTITION_KEY_VALUE || !ATTRIBUTE_NAME) {
    console.error('TABLE_NAME, REGION, PARTITION_KEY_NAME, PARTITION_KEY_VALUE, and ATTRIBUTE_NAME environment variables are required');
    process.exit(1);
  }

  const flag = new OCCFlag({
    tableName: TABLE_NAME,
    region: REGION,
    partitionKeyName: PARTITION_KEY_NAME,
    partitionKeyValue: PARTITION_KEY_VALUE,
    sortKeyName: SORT_KEY_NAME,
    sortKeyValue: SORT_KEY_VALUE,
    attributeName: ATTRIBUTE_NAME
  });

  (async () => {
    switch (task as TASK) {
      case UPDATE: {
        const valueToSet = VALUE || 'test-value-' + Date.now();
        await flag.update(valueToSet,
          async () => console.log('Claim succeeded'),
          async () => {
            console.log('Claim failed - already set');
            console.log('Existing value:', await flag.getValue());
          }
        );
        break;
      }
      case UNSET:
        await flag.unset();
        console.log(`Value after unset: ${await flag.getValue()}`);
        break;
      case GET_VALUE:
        console.log(`Current value: ${await flag.getValue()}`);
        break;
      case TEST_RACE:
        console.log('Testing race condition with 5 concurrent update calls...');
        await Promise.all(Array.from({ length: 5 }, (_, i) =>
          flag.update(`client-${i}`,
            async () => console.log(`Client ${i} won the race`),
            async () => console.log(`Client ${i} lost the race`)
          )
        ));
        console.log(`Final value: ${await flag.getValue()}`);
        break;
      default:
        console.log(`Unknown task: ${task}`);
        process.exit(1);
    }
  })();
}