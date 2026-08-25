import { AbstractPersonTarget } from "./PersonTargetReal";
import { DynamoDBClient, ScanCommand } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import { HuronPerson } from "integration-huron-person";

/**
 * In mock target mode, we "pretend" DynamoDB is the Target API.
 * This class scans mockTargetPersonTable to get all person records that have been
 * previously written by MockPersonDataTarget.
 */
export class PersonTargetMocked implements AbstractPersonTarget {

  constructor() { }

  /**
   * Fetch full population from mockTargetPersonTable (DynamoDB).
   * 
   * @throws Error if DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME environment variable is not set
   */
  public async getFullPopulationFromTarget(): Promise<HuronPerson[]> {
    const tableName = process.env.DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME;
    
    if (!tableName) {
      throw new Error(
        'Cannot fetch population from mock target: ' +
        'DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME environment variable is not set. ' +
        'This variable is required when useMockTarget=true.'
      );
    }

    console.log(`\\n🔄 Fetching full population from mock target (DynamoDB table: ${tableName})...`);

    const dynamoDbClient = new DynamoDBClient({ region: process.env.AWS_REGION || process.env.REGION });
    const people: HuronPerson[] = [];
    let lastEvaluatedKey: Record<string, any> | undefined = undefined;

    try {
      do {
        const scanCommand: ScanCommand = new ScanCommand({
          TableName: tableName,
          ExclusiveStartKey: lastEvaluatedKey
        });

        const response = await dynamoDbClient.send(scanCommand);
        
        if (response.Items) {
          for (const item of response.Items) {
            const record = unmarshall(item);
            // mockTargetPersonTable stores: personId, data, createdAt, lastModified, syncRunId
            // Extract sourceIdentifier from personId field
            if (record.personId) {
              people.push({ sourceIdentifier: record.personId } as HuronPerson);
            }
          }
        }

        lastEvaluatedKey = response.LastEvaluatedKey;
      } while (lastEvaluatedKey);

      console.log(`  Retrieved ${people.length} people from mock target (DynamoDB)`);
      
      if (people.length === 0) {
        console.warn('  ⚠️  No people found in mock target table - cache will be empty');
      }

      return people;
    } catch (error) {
      console.error(`  ❌ Error scanning mockTargetPersonTable "${tableName}":`, error);
      throw new Error(`Failed to fetch population from mock target: ${error}`);
    }
  }
}