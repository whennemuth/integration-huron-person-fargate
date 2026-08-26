import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { 
  BatchWriteCommand, 
  DynamoDBDocumentClient, 
  GetCommand, 
  QueryCommand, 
  QueryCommandOutput, 
  ScanCommand, 
  ScanCommandInput 
} from '@aws-sdk/lib-dynamodb';

export type AbstractDynamoDbTable = {
  truncateTable: (chunkSize?: number) => Promise<void>;
  deleteByPartitionKey: (
    { partitionKeyValue, chunkSize }: 
    { partitionKeyValue: string, chunkSize?: number }) => Promise<number>;
  getItem: (
    { partitionKeyValue, sortKeyValue }: 
    { partitionKeyValue: string, sortKeyValue?: string }) => Promise<any | undefined>;
  queryGSI: (
    { 
      indexName, 
      gsiPartitionKey, 
      partitionKeyValue, 
      gsiSortKey, 
      sortKeyValue,
      sortKeyOperator
    }: {
      indexName: string, 
      gsiPartitionKey: string, 
      partitionKeyValue: string,
      gsiSortKey?: string,
      sortKeyValue?: string,
      sortKeyOperator?: '=' | 'begins_with' | '<' | '>' | '<=' | '>=' | 'between',
    }
  ) => Promise<any[]>;
  queryByPartitionKey: (
    { partitionKeyValue, sortKeyPrefix }: 
    { partitionKeyValue: string, sortKeyPrefix?: string }
  ) => Promise<any[]>;
  batchWrite: ({ items, operation }: { items: any[], operation?: 'put' | 'delete' }) => Promise<void>;
  putItem: (item: any) => Promise<void>;
}

/**
 * Generic DynamoDB table wrapper providing common operations.
 * 
 * This class abstracts DynamoDB operations and can be used by any table-specific
 * wrapper class. It handles:
 * - Batch operations (scan, delete)
 * - Single-item operations (get)
 * - Query operations (by partition key, by GSI)
 * - Pagination automatically
 * 
 * Usage:
 * ```typescript
 * const table = new DynamoDBTable({
 *   region: 'us-east-2',
 *   tableName: 'my-table',
 *   partitionKey: 'id',
 *   sortKey: 'timestamp' // optional
 * });
 * 
 * const items = await table.queryByPartitionKey('user-123');
 * ```
 */
export class DynamoDBTable implements AbstractDynamoDbTable {
  private client: DynamoDBDocumentClient;
  
  constructor(private params: { 
    region: string;
    tableName: string;
    partitionKey: string;
    sortKey?: string;
  }) { 
    this.client = DynamoDBDocumentClient.from(new DynamoDBClient({ region: params.region }));
  }

  /**
   * Truncate the DynamoDB table by scanning all items and deleting them in batches.
   * This method handles pagination and batch deletion to efficiently clear the table.
   * 
   * Note: DynamoDB does not have a native truncate operation, so this is a workaround.
   * Alternatively, the table could be deleted and recreated, but that may have 
   * implications for table configuration and permissions, or stack drift.
   * 
   * Usage:
   * ```typescript
   * await table.truncateTable(25); // Delete in batches of 25 (DynamoDB limit)
   * ```
   * 
   * @param chunkSize - Number of items to delete per batch (default: 25, max: 25)
   */
  public truncateTable = async (chunkSize: number = 25): Promise<void> => {
    const { client, params: { tableName, partitionKey, sortKey } } = this;
    console.log(`Truncating table: ${tableName}`);
  
    let itemsDeleted = 0;
    let lastEvaluatedKey = undefined;
    
    do {
      const scanParams: ScanCommandInput = {
        TableName: tableName,
        ProjectionExpression: sortKey 
          ? `${partitionKey}, ${sortKey}` 
          : partitionKey,
        ExclusiveStartKey: lastEvaluatedKey
      };
      
      const scanResult = await client.send(new ScanCommand(scanParams));
      
      if (scanResult.Items && scanResult.Items.length > 0) {
        // Process in batches of chunkSize (DynamoDB limit is 25)
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
   * Delete all items with a specific partition key value.
   * This method queries all items matching the partition key and deletes them in batches.
   * Useful for removing all records associated with a specific entity (e.g., a sync run).
   * 
   * Note: This uses Query (efficient) rather than Scan, making it suitable for
   * partition-specific cleanup operations.
   * 
   * Usage:
   * ```typescript
   * const deletedCount = await table.deleteByPartitionKey('2026-03-03T19:58:41.277Z');
   * console.log(`Deleted ${deletedCount} items`);
   * ```
   * 
   * @param partitionKeyValue - Value of the partition key to delete
   * @param chunkSize - Number of items to delete per batch (default: 25, max: 25)
   * @returns Number of items deleted
   */
  public async deleteByPartitionKey(parms: { partitionKeyValue: string, chunkSize?: number } ): Promise<number> {
    const { client, params: { tableName, partitionKey, sortKey } } = this;
    const { partitionKeyValue, chunkSize = 25 } = parms;
    console.log(`Deleting items with ${partitionKey} = ${partitionKeyValue} from table: ${tableName}`);
  
    let itemsDeleted = 0;
    
    // Query all items with this partition key
    const items = await this.queryByPartitionKey({ partitionKeyValue });
    
    if (items.length === 0) {
      console.log('No items found to delete.');
      return 0;
    }
    
    console.log(`Found ${items.length} items to delete.`);
    
    // Process in batches of chunkSize (DynamoDB limit is 25)
    for (let i = 0; i < items.length; i += chunkSize) {
      const batch = items.slice(i, i + chunkSize);
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
    
    console.log(`Deletion complete. Deleted ${itemsDeleted} total items.`);
    return itemsDeleted;
  }

  /**
   * Get a single item from DynamoDB by partition key and sort key.
   * This is a generic method that can retrieve any item from the table.
   * 
   * @param partitionKeyValue - Value of the partition key
   * @param sortKeyValue - Value of the sort key (required if table has sort key)
   * @returns The item if found, undefined otherwise
   */
  public async getItem(parms: { partitionKeyValue: string, sortKeyValue?: string }): Promise<any | undefined> {
    const { client, params: { tableName, partitionKey, sortKey } } = this;
    const { partitionKeyValue, sortKeyValue } = parms;
    
    if (sortKey && !sortKeyValue) {
      throw new Error('getItem requires sortKeyValue when table has a sort key');
    }

    const command = new GetCommand({
      TableName: tableName,
      Key: {
        [partitionKey]: partitionKeyValue,
        ...(sortKey && sortKeyValue ? { [sortKey]: sortKeyValue } : {})
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
   * @param gsiSortKey - Optional sort key attribute name for the GSI
   * @param sortKeyValue - Optional sort key value (exact match or used with operator)
   * @param sortKeyOperator - Optional operator for sort key condition ('=', 'begins_with', '<', '>', '<=', '>=', 'between')
   * @returns Array of all items matching the query
   */
  public async queryGSI(parms: {
    indexName: string, 
    gsiPartitionKey: string, 
    partitionKeyValue: string,
    gsiSortKey?: string,
    sortKeyValue?: string,
    sortKeyOperator?: '=' | 'begins_with' | '<' | '>' | '<=' | '>=' | 'between',
  }): Promise<any[]> {
    const { client, params: { tableName } } = this;
    const { 
      indexName, gsiPartitionKey, partitionKeyValue, 
      sortKeyOperator = '=', gsiSortKey, sortKeyValue 
    } = parms;
    const items: any[] = [];
    let lastEvaluatedKey = undefined;

    try {
      do {
        let keyConditionExpression = `#pk = :pkValue`;
        const expressionAttributeNames: any = { '#pk': gsiPartitionKey };
        const expressionAttributeValues: any = { ':pkValue': partitionKeyValue };

        if (gsiSortKey && sortKeyValue) {
          expressionAttributeNames['#sk'] = gsiSortKey;
          
          if (sortKeyOperator === 'begins_with') {
            keyConditionExpression += ` AND begins_with(#sk, :skValue)`;
            expressionAttributeValues[':skValue'] = sortKeyValue;
          } else {
            keyConditionExpression += ` AND #sk ${sortKeyOperator} :skValue`;
            expressionAttributeValues[':skValue'] = sortKeyValue;
          }
        }

        const command = new QueryCommand({
          TableName: tableName,
          IndexName: indexName,
          KeyConditionExpression: keyConditionExpression,
          ExpressionAttributeNames: expressionAttributeNames,
          ExpressionAttributeValues: expressionAttributeValues,
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
   * Query items by partition key with optional sort key prefix or condition.
   * Useful for querying all items with a specific PK and SK pattern.
   * 
   * @param partitionKeyValue - Value of the partition key
   * @param sortKeyPrefix - Optional prefix for sort key (uses begins_with)
   * @returns Array of all items matching the query
   */
  public async queryByPartitionKey(parms: { partitionKeyValue: string, sortKeyPrefix?: string }): Promise<any[]> {
    const { client, params: { tableName, partitionKey, sortKey } } = this;
    
    const { partitionKeyValue, sortKeyPrefix } = parms;

    if (!sortKey && sortKeyPrefix) {
      throw new Error('Cannot use sortKeyPrefix when table has no sort key');
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
            ...(sortKeyPrefix && sortKey && { '#sk': sortKey })
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

  /**
   * Batch write items to the table.
   * Handles batching automatically (max 25 items per request).
   * 
   * @param items - Array of items to write
   * @param operation - 'put' or 'delete' (default: 'put')
   */
  public async batchWrite(parms: { items: any[], operation?: 'put' | 'delete' } ): Promise<void> {
    const { client, params: { tableName } } = this;
    const batchSize = 25; // DynamoDB limit

    const { items, operation = 'put' } = parms;
    for (let i = 0; i < items.length; i += batchSize) {
      const batch = items.slice(i, i + batchSize);
      const requests = batch.map(item => 
        operation === 'put'
          ? { PutRequest: { Item: item } }
          : { DeleteRequest: { Key: item } }
      );

      await client.send(new BatchWriteCommand({
        RequestItems: { [tableName]: requests }
      }));
    }
  }

  /**
   * Put a single item into the table.
   * 
   * @param item - The item to write
   */
  public async putItem(item: any): Promise<void> {
    await this.batchWrite({ items: [item], operation: 'put' });
  }
}
