import { DynamoDBDocumentClient, GetCommand, UpdateCommand, UpdateCommandInput } from "@aws-sdk/lib-dynamodb";
import { IContext } from "../context/IContext";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { TestEnvironment } from "integration-core";

export const DYNAMODB_TABLE_NAME = (context: IContext) => `${context.STACK_ID}-actomic-counter`;
export const DYNAMODB_PARTITION_KEY = 'counter_name';

/**
 * Utility class to manage an atomic counter stored in DynamoDB. Provides methods to 
 * increment and retrieve the counter value. DynamoDB's UpdateItem with ADD operation is 
 * used to ensure atomic increments even under high concurrency, and by "atomic" is meant 
 * "All-or-nothing" - no partial updates, and combinded with optimistic locking, there is
 * a guarantee that concurrent increments will not overwrite each other, but all increments 
 * will be applied, and so no one incrementor will get the same value as another 
 * incrementor - no race conditions.
 */
export abstract class AbstractAtomicCounter {
  private client: DynamoDBDocumentClient; 

  constructor(private stackId: string, private region: string) {
    this.client = DynamoDBDocumentClient.from(new DynamoDBClient({ region }));
  }

  public abstract getCounterName(): string;

  public increment = async (incrementBy: number = 1): Promise<number> => {
    const input = {
      TableName: DYNAMODB_TABLE_NAME({ STACK_ID: this.stackId } as IContext),
      Key: { [DYNAMODB_PARTITION_KEY]: this.getCounterName() },
      UpdateExpression: 'ADD #counterValue :incrementBy SET LastUpdated = :timestamp',
      ExpressionAttributeNames: {
        '#counterValue': 'counter_value',
      },
      ExpressionAttributeValues: {
        ':incrementBy': incrementBy,
        ':timestamp': new Date().toISOString()
      },
      ReturnValues: 'UPDATED_NEW',
    } satisfies UpdateCommandInput;

    const result = await this.client.send(new UpdateCommand(input));
    return result.Attributes?.counter_value || 0;
  }

  public reset = async (): Promise<AbstractAtomicCounter> => {
    const input = {
      TableName: DYNAMODB_TABLE_NAME({ STACK_ID: this.stackId } as IContext),
      Key: { [DYNAMODB_PARTITION_KEY]: this.getCounterName() },
      UpdateExpression: 'SET #counterValue = :zero, LastUpdated = :timestamp',
      ExpressionAttributeNames: {
        '#counterValue': 'counter_value',
      },
      ExpressionAttributeValues: {
        ':zero': 0,
        ':timestamp': new Date().toISOString()
      },
    } satisfies UpdateCommandInput;

    await this.client.send(new UpdateCommand(input));
    return this;
  }

  public getValue = async (): Promise<number> => {
    const input = {
      TableName: DYNAMODB_TABLE_NAME({ STACK_ID: this.stackId } as IContext),
      Key: { [DYNAMODB_PARTITION_KEY]: this.getCounterName() },
    };

    const result = await this.client.send(new GetCommand(input));
    return result.Item?.counter_value || 0;
  }
}

enum TASK {
  INCREMENT = 'increment',
  RESET = 'reset',
  GET_VALUE = 'get-value'
}

if (require.main === module) {
  const testEnvironment = TestEnvironment('ATOMIC_COUNTER');
  [
    'TASK',
    'STACK_ID',
    'REGION',
  ].forEach(testEnvironment.getVar);

  const { GET_VALUE, INCREMENT, RESET } = TASK;
  let { TASK: task = INCREMENT, STACK_ID, REGION } = process.env;

  if(!STACK_ID) {
    console.error('STACK_ID environment variable is required');
    process.exit(1);
  }

  if(!REGION) {
    console.error('REGION environment variable is required');
    process.exit(1);
  }

  (async () => {

    const counter = new class extends AbstractAtomicCounter {
      getCounterName(): string {
        return 'test-counter';
      }
    }(STACK_ID, REGION);

    switch (task as TASK) {
      case INCREMENT:
        let counterValue = await counter.increment();
        console.log(`Counter incremented to: ${counterValue}`);
        let latestValue = await counter.getValue();
        console.log(`Latest counter value: ${latestValue}`);
        counterValue = await counter.increment(5);
        console.log(`Counter incremented by 5 to: ${counterValue}`);
        latestValue = await counter.getValue();
        console.log(`Latest counter value: ${latestValue}`);
        break;
      case RESET:
        await counter.reset();
        let latestValueAfterReset = await counter.getValue();
        console.log(`Counter value after reset: ${latestValueAfterReset}`);
        break;
      case GET_VALUE:
        let value = await counter.getValue();
        console.log(`Counter value: ${value}`);
        break;
      default:
        console.log(`Unknown task: ${task}`);
        process.exit(1);
    }
  })();
}