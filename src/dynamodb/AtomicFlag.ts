import { DescribeTableCommand, DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { IContext } from "../../context/IContext";
import { DynamoDBDocumentClient, GetCommand, UpdateCommand, UpdateCommandInput, DeleteCommand } from "@aws-sdk/lib-dynamodb";
import { TestEnvironment } from "integration-core";

export const DYNAMODB_TABLE_NAME = (context: IContext) => `${context.STACK_ID}-atomic-flag-${context.TAGS.Landscape.toLowerCase()}`;
export const DYNAMODB_PARTITION_KEY = 'flag_name';

/**
 * Utility class to manage an atomic flag stored in DynamoDB. Provides "can only be set once" 
 * behavior using conditional writes. When a flag is set with a condition that it doesn't exist,
 * DynamoDB's conditional write ensures only one client can successfully set it, preventing race 
 * conditions in distributed systems.
 * 
 * Use cases:
 * - Ensuring only one process triggers a task (e.g., last processor triggers merger)
 * - Leader election in distributed systems
 * - One-time initialization flags
 * - Idempotent operation guards
 * 
 * Example:
 * ```typescript
 * const flag = new class extends AbstractAtomicFlag {
 *   getFlagName() { return 'merger-triggered'; }
 * }({ stackId: 'my-stack', region: 'us-east-2', landscape: 'dev' });
 * 
 * await flag.setFlag('processor-0009', async () => {
 *   console.log('Another process already set this flag');
 * });
 * ```
 */
export abstract class AbstractAtomicFlag {
  private client: DynamoDBDocumentClient;

  constructor(private params: { stackId: string, region: string, landscape: string }) {
    this.client = DynamoDBDocumentClient.from(new DynamoDBClient({ region: params.region }));
  }

  public abstract getFlagName(): string;

  /**
   * Check if the DynamoDB table for the atomic flag exists. This is useful for determining if the flag has been initialized.
   * @returns A promise that resolves to true if the table exists, false otherwise.
   */
  public tableExists = async (tableName?: string): Promise<boolean> => {
    const { stackId: STACK_ID, landscape: Landscape } = this.params;
    try {
      await this.client.send(new DescribeTableCommand({ 
        TableName: tableName || DYNAMODB_TABLE_NAME({ STACK_ID, TAGS: { Landscape } } as IContext) }));
      return true;
    } catch (error) {
      const { name: errorName } = error as any;
      if (errorName === 'ResourceNotFoundException') {
        return false;
      }
      return false;
    }
  }

  /**
   * Set the flag value atomically. Uses conditional write to ensure the flag can only be set once.
   * If another client already set the flag, the condition fails and the fallback function is called.
   * 
   * @param value - The value to store in the flag (any JSON-serializable value)
   * @param fallback - Async function to call if the flag was already set by another client
   * @returns Promise that resolves when operation completes (either set or fallback)
   * @throws Error if DynamoDB operation fails for reasons other than condition violation
   */
  public setFlag = async (value: any, fallback: () => Promise<void>): Promise<void> => {
    const { stackId: STACK_ID, landscape: Landscape } = this.params;
    const input = {
      TableName: DYNAMODB_TABLE_NAME({ STACK_ID, TAGS: { Landscape } } as IContext),
      Key: { [DYNAMODB_PARTITION_KEY]: this.getFlagName() },
      UpdateExpression: 'SET flag_value = :value, SetAt = :timestamp',
      ConditionExpression: 'attribute_not_exists(flag_value)',
      ExpressionAttributeValues: {
        ':value': value,
        ':timestamp': new Date().toISOString()
      },
    } satisfies UpdateCommandInput;

    try {
      await this.client.send(new UpdateCommand(input));
      console.log(`✅ Flag '${this.getFlagName()}' set successfully to:`, value);
    } catch (error: any) {
      if (error.name === 'ConditionalCheckFailedException') {
        console.log(`⚠️  Flag '${this.getFlagName()}' was already set by another client`);
        await fallback();
      } else {
        throw error;
      }
    }
  }

  /**
   * Remove the flag from DynamoDB, allowing it to be set again.
   * This is useful for testing or when you need to reset the flag state.
   * 
   * @returns Promise that resolves when the flag is deleted
   */
  public unsetFlag = async (): Promise<void> => {
    const { stackId: STACK_ID, landscape: Landscape } = this.params;
    await this.client.send(new DeleteCommand({
      TableName: DYNAMODB_TABLE_NAME({ STACK_ID, TAGS: { Landscape } } as IContext),
      Key: { [DYNAMODB_PARTITION_KEY]: this.getFlagName() }
    }));
    console.log(`🗑️  Flag '${this.getFlagName()}' unset successfully`);
  }

  /**
   * Get the current value of the flag, or undefined if not set.
   * 
   * @returns Promise that resolves to the flag value or undefined
   */
  public getFlagValue = async (): Promise<any> => {
    const { stackId: STACK_ID, landscape: Landscape } = this.params;
    const result = await this.client.send(new GetCommand({
      TableName: DYNAMODB_TABLE_NAME({ STACK_ID, TAGS: { Landscape } } as IContext),
      Key: { [DYNAMODB_PARTITION_KEY]: this.getFlagName() }
    }));
    return result.Item?.flag_value;
  }
}

enum TASK {
  SET = 'set',
  UNSET = 'unset',
  GET_VALUE = 'get-value',
  EXISTS = 'exists',
  TEST_RACE = 'test-race'
}

if (require.main === module) {
  const testEnvironment = TestEnvironment('ATOMIC_FLAG');
  [
    'TASK',
    'STACK_ID',
    'REGION',
    'FLAG_VALUE'
  ].forEach(testEnvironment.getVarOrEmptyString);

  const { SET, UNSET, GET_VALUE, EXISTS, TEST_RACE } = TASK;
  let { TASK: task = SET, STACK_ID, REGION, FLAG_VALUE } = process.env;

  if(!STACK_ID) {
    console.error('STACK_ID environment variable is required');
    process.exit(1);
  }

  if(!REGION) {
    console.error('REGION environment variable is required');
    process.exit(1);
  }

  (async () => {

    const flag = new class extends AbstractAtomicFlag {
      getFlagName(): string {
        return 'test-flag';
      }
    }({ stackId: STACK_ID, region: REGION, landscape: 'dev' });

    switch (task as TASK) {
      case SET:
        const valueToSet = FLAG_VALUE || 'test-value-' + Date.now();
        await flag.setFlag(valueToSet, async () => {
          console.log('Fallback: Flag was already set');
          const existingValue = await flag.getFlagValue();
          console.log('Existing value:', existingValue);
        });
        break;
      case UNSET:
        await flag.unsetFlag();
        const valueAfterUnset = await flag.getFlagValue();
        console.log(`Flag value after unset: ${valueAfterUnset}`);
        break;
      case GET_VALUE:
        const value = await flag.getFlagValue();
        console.log(`Flag value: ${value}`);
        break;
      case EXISTS:
        const exists = await flag.tableExists();
        console.log(`Flag table exists: ${exists}`);
        break;
      case TEST_RACE:
        // Simulate race condition: try to set flag multiple times concurrently
        console.log('Testing race condition with 5 concurrent setFlag calls...');
        const promises = Array.from({ length: 5 }, (_, i) => 
          flag.setFlag(`client-${i}`, async () => {
            console.log(`Client ${i} lost the race`);
          })
        );
        await Promise.all(promises);
        const finalValue = await flag.getFlagValue();
        console.log(`Final flag value: ${finalValue}`);
        break;
      default:
        console.log(`Unknown task: ${task}`);
        process.exit(1);
    }
  })();
}