import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { BatchWriteCommand, DynamoDBDocumentClient, GetCommand, QueryCommand, QueryCommandOutput, ScanCommand, ScanCommandInput } from '@aws-sdk/lib-dynamodb';
import { IContext } from '../../context/IContext';
import { DYNAMODB_PARTITION_KEY, DYNAMODB_SECONDARY_PARTITION_KEY, DYNAMODB_SORT_KEY, DYNAMODB_GSI_INDEX_NAME, DYNAMODB_TABLE_NAME as getTableName } from '../../lib/DynamoDB';
import { StatisticsItem } from '../processing/ApiErrorTracking';

export class DynamoDBTable {
  private client: DynamoDBDocumentClient;
  
  constructor(private params: { 
    region: string, tableName: string, partitionKey: string, sortKey?: string 
  }) { 
    this.client = DynamoDBDocumentClient.from(new DynamoDBClient({ region: params.region }));
  }

  /**
   * Truncate the DynamoDB table by scanning all items and deleting them in batches.
   * This method handles pagination and batch deletion to efficiently clear the table.
   * Note: DynamoDB does not have a native truncate operation, so this is a workaround.
   * Alternatively, the table could be deleted and recreated, but that may have 
   * implications for table configuration and permissions, or stack drift.
   * 
   * Usage:
   * 1. Instantiate the DynamoDBTable class with the appropriate parameters.
   * 2. Call the `truncate` method to start the truncation process.
   * 3. Monitor the console output for progress updates and completion status.
   */
  public truncateTable = async (chunkSize: number = 25): Promise<void> => {
    const { client, params: { tableName, partitionKey, sortKey } } = this;
    console.log(`Truncating statistics table: ${tableName}`);
  
    let itemsDeleted = 0;
    let lastEvaluatedKey = undefined;
    
    do {
      const scanParams: ScanCommandInput = {
        TableName: tableName,
        ProjectionExpression: `${partitionKey}, ${sortKey}`, // Only get keys
        ExclusiveStartKey: lastEvaluatedKey
      };
      
      const scanResult = await client.send(new ScanCommand(scanParams));
      
      if (scanResult.Items && scanResult.Items.length > 0) {
        // Process in batches of chunkSize (DynamoDB limit)
        for (let i = 0; i < scanResult.Items.length; i += chunkSize) {
          const batch = scanResult.Items.slice(i, i + chunkSize);
          const getKey = (item: any) => ({
            [partitionKey]: item[partitionKey],
            ...(sortKey ? { [sortKey]: item[sortKey] } : {})
          });
          const deleteRequests = batch.map(item => ({
            DeleteRequest: {
              Key: getKey(item)
            }
          }));
          
          await client.send(new BatchWriteCommand({
            RequestItems: { [tableName]: deleteRequests }
          }));
          
          itemsDeleted += batch.length;
          console.log(`Deleted ${itemsDeleted} items so far...`);
        }
      }
      
      lastEvaluatedKey = scanResult.LastEvaluatedKey;
    } while (lastEvaluatedKey);
    
    console.log(`Truncation complete. Deleted ${itemsDeleted} total items.`);
  }

  /**
   * Get a single item from DynamoDB by partition key and sort key.
   * This is a generic method that can retrieve any item from the table.
   * 
   * @param partitionKeyValue - Value of the partition key
   * @param sortKeyValue - Value of the sort key
   * @returns The item if found, undefined otherwise
   */
  public async getItem(partitionKeyValue: string, sortKeyValue: string): Promise<any | undefined> {
    const { client, params: { tableName, partitionKey, sortKey } } = this;
    
    if (!sortKey) {
      throw new Error('getItem requires a sort key to be configured');
    }

    const command = new GetCommand({
      TableName: tableName,
      Key: {
        [partitionKey]: partitionKeyValue,
        [sortKey]: sortKeyValue
      }
    });

    try {
      const result = await client.send(command);
      return result.Item;
    } catch (error) {
      console.error(`Error getting item from ${tableName}:`, error);
      throw error;
    }
  }

  /**
   * Query a Global Secondary Index (GSI) by partition key.
   * Handles pagination automatically to retrieve all matching items.
   * 
   * @param indexName - Name of the GSI to query
   * @param gsiPartitionKey - The partition key attribute name for the GSI
   * @param partitionKeyValue - Value of the GSI partition key
   * @returns Array of all items matching the query
   */
  public async queryGSI(indexName: string, gsiPartitionKey: string, partitionKeyValue: string): Promise<any[]> {
    const { client, params: { tableName } } = this;
    const items: any[] = [];
    let lastEvaluatedKey = undefined;

    try {
      do {
        const command = new QueryCommand({
          TableName: tableName,
          IndexName: indexName,
          KeyConditionExpression: `#pk = :pkValue`,
          ExpressionAttributeNames: {
            '#pk': gsiPartitionKey
          },
          ExpressionAttributeValues: {
            ':pkValue': partitionKeyValue
          },
          ExclusiveStartKey: lastEvaluatedKey
        });

        const result: QueryCommandOutput = await client.send(command);
        
        if (result.Items) {
          items.push(...result.Items);
        }

        lastEvaluatedKey = result.LastEvaluatedKey;
      } while (lastEvaluatedKey);

      return items;
    } catch (error) {
      console.error(`Error querying GSI ${indexName} on ${tableName}:`, error);
      throw error;
    }
  }
}

export class StatisticsTable {
  private table: DynamoDBTable;

  constructor(private context: IContext) {
    const region = context.REGION;
    const tableName = getTableName(context);
    const partitionKey = DYNAMODB_PARTITION_KEY;
    const sortKey = DYNAMODB_SORT_KEY;
    this.table = new DynamoDBTable({ region, tableName, partitionKey, sortKey });
  }

  public truncate = async (chunkSize?: number): Promise<void> => {
    await this.table.truncateTable(chunkSize);
  }

  /**
   * Fetch statistics for a specific integration run.
   * Retrieves the statistics record using the integrationTimestamp as the partition key
   * and "STATISTICS" as the sort key.
   * 
   * @param integrationTimestamp - ISO timestamp of the integration run
   * @returns The statistics item for that run, or undefined if not found
   */
  public getStatistics = async (integrationTimestamp: string): Promise<StatisticsItem | undefined> => {
    const eventType = 'STATISTICS';
    const item = await this.table.getItem(integrationTimestamp, eventType);
    return item as StatisticsItem | undefined;
  }

  /**
   * Get a list of sync operations, uniquely identified by their integration timestamps.
   * This method queries the existing errorType-timestamp-index GSI to efficiently retrieve
   * all statistics records (one per integration run).
   * 
   * @returns An array of unique integration timestamps representing different sync runs,
   *          sorted chronologically (ascending)
   */
  public getSyncList = async (): Promise<string[]> => {
    // Query the GSI with errorType = "STATISTICS" to get all statistics records
    // Each integration run has exactly one statistics record
    const items = await this.table.queryGSI(
      DYNAMODB_GSI_INDEX_NAME,
      DYNAMODB_SECONDARY_PARTITION_KEY,
      'STATISTICS'
    );

    // Extract and return the integrationTimestamp from each item
    // Results are already sorted chronologically by the GSI's sort key
    return items.map(item => item.integrationTimestamp as string);
  }
}


if(require.main === module) {
  const { 
    STATISTICS_TABLE_TASK: task, STATISTICS_TABLE_INTEGRATION_TIMESTAMP: timestamp,
    TRUNCATE_CHUNK_SIZE
  } = process.env;

  (async () => {
    const context = require('../../context/context.json') as IContext;
    const statisticsTable = new StatisticsTable(context);
    switch(task) {
      case 'truncate':
        const chunkSize = TRUNCATE_CHUNK_SIZE ? parseInt(TRUNCATE_CHUNK_SIZE, 10) : undefined;
        await statisticsTable.truncate(chunkSize);
        break;
      case 'statistics':
        if(!timestamp) {
          console.error('Missing required STATISTICS_TABLE_INTEGRATION_TIMESTAMP environment variable for statistics task!');
          process.exit(1);
        }
        const stats = await statisticsTable.getStatistics(timestamp);
        if(stats) {
          console.log(`Statistics for integration run at ${timestamp}:`, JSON.stringify(stats, null, 2));
        } else {
          console.log(`No statistics found for integration run at ${timestamp}`);
        }
        break;
      case 'list':
        const syncList = await statisticsTable.getSyncList();
        console.log(`Found ${syncList.length} integration run(s):`);
        console.log(JSON.stringify(syncList, null, 2));
        break;
      default:
        console.error(`Unknown task: ${task}. Supported tasks: truncate, statistics, list`);
    }
  })();
}

