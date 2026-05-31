import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { BatchWriteCommand, DynamoDBDocumentClient, GetCommand, QueryCommand, QueryCommandOutput, ScanCommand, ScanCommandInput } from '@aws-sdk/lib-dynamodb';
import { TestEnvironment } from 'integration-core';
import { IContext } from '../../context/IContext';
import { StatisticsItem } from '../processing/ApiErrorTracking';

export const DYNAMODB_TABLE_NAME = (context: IContext) => `${context.STACK_ID}-statistics`;
export const DYNAMODB_PARTITION_KEY = 'integrationTimestamp';
export const DYNAMODB_SECONDARY_PARTITION_KEY = 'errorType';
export const DYNAMODB_SORT_KEY = 'eventType';
export const DYNAMODB_GSI_INDEX_NAME = 'errorType-timestamp-index';

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

  /**
   * Query items by partition key with optional sort key prefix.
   * Useful for querying all items with a specific PK and SK pattern.
   * 
   * @param partitionKeyValue - Value of the partition key
   * @param sortKeyPrefix - Optional prefix for sort key (uses begins_with)
   * @returns Array of all items matching the query
   */
  public async queryByPartitionKey(partitionKeyValue: string, sortKeyPrefix?: string): Promise<any[]> {
    const { client, params: { tableName, partitionKey, sortKey } } = this;
    
    if (!sortKey) {
      throw new Error('queryByPartitionKey requires a sort key to be configured');
    }

    const items: any[] = [];
    let lastEvaluatedKey = undefined;

    try {
      do {
        const command = new QueryCommand({
          TableName: tableName,
          KeyConditionExpression: sortKeyPrefix 
            ? `#pk = :pkValue AND begins_with(#sk, :skPrefix)`
            : `#pk = :pkValue`,
          ExpressionAttributeNames: {
            '#pk': partitionKey,
            ...(sortKeyPrefix && { '#sk': sortKey })
          },
          ExpressionAttributeValues: {
            ':pkValue': partitionKeyValue,
            ...(sortKeyPrefix && { ':skPrefix': sortKeyPrefix })
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
      console.error(`Error querying ${tableName}:`, error);
      throw error;
    }
  }
}

export class StatisticsTable {
  private table: DynamoDBTable;

  constructor(private context: IContext) {
    const region = context.REGION;
    const tableName = DYNAMODB_TABLE_NAME(context);
    const partitionKey = DYNAMODB_PARTITION_KEY;
    const sortKey = DYNAMODB_SORT_KEY;
    this.table = new DynamoDBTable({ region, tableName, partitionKey, sortKey });
  }

  public truncate = async (chunkSize?: number): Promise<void> => {
    await this.table.truncateTable(chunkSize);
  }

  /**
   * Fetch aggregated statistics for a specific integration run.
   * Retrieves the statistics record using the integrationTimestamp as the partition key
   * and "STATISTICS" as the sort key.
   * 
   * Note: This only retrieves the aggregated statistics record (SK = 'STATISTICS').
   * For chunk-specific statistics, use getChunkStatistics() or getAllChunkStatistics().
   * 
   * @param integrationTimestamp - ISO timestamp of the integration run
   * @returns The aggregated statistics item for that run, or undefined if not found
   */
  public getStatistics = async (integrationTimestamp: string): Promise<StatisticsItem | undefined> => {
    const eventType = 'STATISTICS';
    const item = await this.table.getItem(integrationTimestamp, eventType);
    return item as StatisticsItem | undefined;
  }

  /**
   * Fetch statistics for a specific chunk within an integration run.
   * 
   * @param integrationTimestamp - ISO timestamp of the integration run
   * @param chunkId - The chunk identifier (e.g., 'chunk-0009')
   * @returns The statistics item for that chunk, or undefined if not found
   */
  public getChunkStatistics = async (integrationTimestamp: string, chunkId: string): Promise<StatisticsItem | undefined> => {
    const eventType = `STATISTICS-${chunkId}`;
    const item = await this.table.getItem(integrationTimestamp, eventType);
    return item as StatisticsItem | undefined;
  }

  /**
   * Fetch statistics for all chunks within an integration run.
   * Queries all records with sort key beginning with 'STATISTICS-chunk-'.
   * 
   * @param integrationTimestamp - ISO timestamp of the integration run
   * @returns Array of statistics items for all chunks, sorted by sort key
   */
  public getAllChunkStatistics = async (integrationTimestamp: string): Promise<StatisticsItem[]> => {
    const items = await this.table.queryByPartitionKey(integrationTimestamp, 'STATISTICS-chunk-');
    return items as StatisticsItem[];
  }

  /**
   * Get a list of sync operations, uniquely identified by their integration timestamps.
   * This method queries the existing errorType-timestamp-index GSI to efficiently retrieve
   * all statistics records (both aggregated and chunk-specific).
   * 
   * Note: Returns all records with errorType = 'STATISTICS', which includes both
   * aggregated records (SK = 'STATISTICS') and chunk-specific records (SK = 'STATISTICS-chunk-XXX').
   * To get only unique integration runs, use getUniqueIntegrationTimestamps().
   * 
   * @returns An array of integration timestamps (may include duplicates if chunks exist),
   *          sorted chronologically (ascending)
   */
  public getSyncList = async (): Promise<string[]> => {
    // Query the GSI with errorType = "STATISTICS" to get all statistics records
    const items = await this.table.queryGSI(
      DYNAMODB_GSI_INDEX_NAME,
      DYNAMODB_SECONDARY_PARTITION_KEY,
      'STATISTICS'
    );

    // Extract and return the integrationTimestamp from each item
    // Results are already sorted chronologically by the GSI's sort key
    return items.map(item => item.integrationTimestamp as string);
  }

  /**
   * Get a list of unique integration timestamps across all sync operations.
   * This returns deduplicated timestamps, useful for listing distinct integration runs
   * when chunk-specific statistics exist.
   * 
   * @returns An array of unique integration timestamps, sorted chronologically (ascending)
   */
  public getUniqueIntegrationTimestamps = async (): Promise<string[]> => {
    const allTimestamps = await this.getSyncList();
    // Deduplicate using Set, then sort chronologically
    return Array.from(new Set(allTimestamps)).sort();
  }
}


if(require.main === module) {
  const testEnvironment = TestEnvironment('STATISTICS_TABLE');
  [
    'STATISTICS_TABLE_TASK',
    'STATISTICS_TABLE_INTEGRATION_TIMESTAMP',
    'TRUNCATE_CHUNK_SIZE'
  ].forEach(testEnvironment.getVar);

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
        const syncList = await statisticsTable.getUniqueIntegrationTimestamps();
        console.log(`Found ${syncList.length} unique integration run(s):`);
        console.log(JSON.stringify(syncList, null, 2));
        break;
      case 'chunks':
        if(!timestamp) {
          console.error('Missing required STATISTICS_TABLE_INTEGRATION_TIMESTAMP environment variable for chunks task!');
          process.exit(1);
        }
        const chunks = await statisticsTable.getAllChunkStatistics(timestamp);
        console.log(`Found ${chunks.length} chunk(s) for integration run at ${timestamp}:`);
        console.log(JSON.stringify(chunks, null, 2));
        break;
      default:
        console.error(`Unknown task: ${task}. Supported tasks: truncate, statistics, list, chunks`);
    }
  })();
}

