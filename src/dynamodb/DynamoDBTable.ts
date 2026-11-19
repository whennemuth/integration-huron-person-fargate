import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { 
  BatchWriteCommand,
  BatchWriteCommandOutput,
  DynamoDBDocumentClient,
  GetCommand, 
  QueryCommand, 
  QueryCommandOutput, 
  ScanCommand, 
  ScanCommandInput 
} from '@aws-sdk/lib-dynamodb';

export type QueryPage = { items: any[], lastEvaluatedKey?: Record<string, any> };

/** How a GSI query compares the sort key with sortKeyValue */
export type SortKeyComparison = '=' | '<' | '<=' | '>' | '>=';

export type QueryGSIPageParams = {
  indexName: string, gsiPartitionKey: string, partitionKeyValue: string,
  gsiSortKey?: string, sortKeyValue?: string, sortKeyComparison?: SortKeyComparison,
  /** false reads in descending sort key order */
  scanIndexForward?: boolean,
  limit: number, exclusiveStartKey?: Record<string, any>
};

/** How a truncation spreads its work (DynamoDBTable.truncateTable()) */
export type TruncateOptions = {
  /** Segments of the parallel scan (default TRUNCATE_SEGMENTS) */
  segments?: number;
  /** Delete batches each segment keeps in flight (default TRUNCATE_BATCHES_IN_FLIGHT) */
  batchesInFlight?: number;
};

export type AbstractDynamoDbTable = {
  truncateTable: (chunkSize?: number, options?: TruncateOptions) => Promise<number>;
  scanAll: () => Promise<any[]>;
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
  scanIndex: ({ indexName }: { indexName: string }) => Promise<any[]>;
  queryPage: (parms: {
    partitionKeyValue: string, sortKeyPrefix?: string, limit: number, exclusiveStartKey?: Record<string, any>
  }) => Promise<QueryPage>;
  queryGSIPage: (parms: QueryGSIPageParams) => Promise<QueryPage>;
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
 * const items = await table.queryByPartitionKey({ partitionKeyValue: 'user-123' });
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
    this.client = DynamoDBDocumentClient.from(
      new DynamoDBClient({ region: params.region }),
      { marshallOptions: { removeUndefinedValues: true } } // defense-in-depth: any future caller may pass optional fields left undefined
    );
  }

  /** A truncation's parallel scan: segments scanned at once (DynamoDB's Segment / TotalSegments) */
  static readonly TRUNCATE_SEGMENTS = 8;
  /** Delete batches (BatchWriteItem, 25 keys each) each segment keeps in flight */
  static readonly TRUNCATE_BATCHES_IN_FLIGHT = 4;
  /** Attempts per delete batch: more than other writes, since a truncation's deletes in parallel invite throttling */
  static readonly TRUNCATE_ATTEMPTS = 8;

  /**
   * Truncate the table: delete every item. DynamoDB has no truncate, so this scans the keys and deletes them in
   * batches (the alternative, deleting and recreating the table, would drift from its stack). The scan is parallel
   * (TRUNCATE_SEGMENTS segments at once), and each segment keeps TRUNCATE_BATCHES_IN_FLIGHT delete batches in flight,
   * so a table of some 146,000 items (a mock landscape's after a hybrid full sync) empties within a Lambda's minutes
   * rather than the tens of minutes one batch at a time took. Throttled deletes are retried with backoff
   * (sendBatchWithRetry); a batch still unprocessed after its attempts fails the truncation.
   *
   * Usage:
   * ```typescript
   * const deleted = await table.truncateTable();
   * ```
   *
   * @param chunkSize - Keys per delete batch (default and most: 25, DynamoDB's limit)
   * @param options - Segments and batches in flight (defaults: TRUNCATE_SEGMENTS, TRUNCATE_BATCHES_IN_FLIGHT)
   * @returns The number of items deleted
   */
  public truncateTable = async (chunkSize: number = 25, options: TruncateOptions = {}): Promise<number> => {
    const { params: { tableName } } = this;
    const segments = Math.max(1, Math.floor(options.segments ?? DynamoDBTable.TRUNCATE_SEGMENTS));
    const inFlight = Math.max(1, Math.floor(options.batchesInFlight ?? DynamoDBTable.TRUNCATE_BATCHES_IN_FLIGHT));
    const batchSize = Math.min(25, Math.max(1, Math.floor(chunkSize || 25)));
    console.log(`Truncating table: ${tableName} (${segments} segments, ${inFlight} batches of ${batchSize} in flight each)`);

    const progress = { deleted: 0 };
    const counts = await Promise.all(Array.from({ length: segments }, (_, segment) =>
      this.truncateSegment(segment, segments, batchSize, inFlight, progress)));
    const deleted = counts.reduce((sum, count) => sum + count, 0);
    console.log(`Truncation complete. Deleted ${deleted} total items from ${tableName}.`);
    return deleted;
  }

  /** One segment of truncateTable()'s parallel scan: its keys, page by page, deleted inFlight batches at a time */
  private async truncateSegment(
    segment: number, totalSegments: number, batchSize: number, inFlight: number, progress: { deleted: number }
  ): Promise<number> {
    const { client, params: { tableName, partitionKey, sortKey } } = this;
    const keyOf = (item: any) => ({ [partitionKey]: item[partitionKey], ...(sortKey ? { [sortKey]: item[sortKey] } : {}) });
    let deleted = 0;
    let lastEvaluatedKey: Record<string, any> | undefined = undefined;
    do {
      const scanParams: ScanCommandInput = {
        TableName: tableName,
        // Keys only, under placeholders (a key's name may be a reserved word)
        ProjectionExpression: sortKey ? '#pk, #sk' : '#pk',
        ExpressionAttributeNames: sortKey ? { '#pk': partitionKey, '#sk': sortKey } : { '#pk': partitionKey },
        ...(totalSegments > 1 ? { Segment: segment, TotalSegments: totalSegments } : {}),
        ExclusiveStartKey: lastEvaluatedKey
      };
      const scanResult: any = await client.send(new ScanCommand(scanParams));
      const keys = (scanResult.Items ?? []).map(keyOf);
      const batches: any[][] = [];
      for (let i = 0; i < keys.length; i += batchSize) {
        batches.push(keys.slice(i, i + batchSize).map((Key: any) => ({ DeleteRequest: { Key } })));
      }
      // inFlight workers share the page's batches
      let next = 0;
      await Promise.all(Array.from({ length: Math.min(inFlight, batches.length) }, async () => {
        for (let batch = batches[next++]; batch; batch = batches[next++]) {
          await this.sendBatchWithRetry(batch, DynamoDBTable.TRUNCATE_ATTEMPTS);
        }
      }));
      deleted += keys.length;
      progress.deleted += keys.length;
      if (keys.length > 0) {
        console.log(`Deleted ${progress.deleted} items from ${tableName} so far...`);
      }
      lastEvaluatedKey = scanResult.LastEvaluatedKey;
    } while (lastEvaluatedKey);
    return deleted;
  }

  /**
   * Non-destructive full table scan. Handles pagination and returns every item.
   * 
   * Use sparingly - a full scan reads the entire table regardless of size. Intended for
   * once-per-run operations (e.g. post-processor-phase deletion detection), not per-chunk/per-record use.
   * 
   * @returns All items currently in the table
   */
  public scanAll = async (): Promise<any[]> => {
    const { client, params: { tableName } } = this;
    const items: any[] = [];
    let lastEvaluatedKey = undefined;

    do {
      const scanParams: ScanCommandInput = {
        TableName: tableName,
        ExclusiveStartKey: lastEvaluatedKey
      };

      const scanResult = await client.send(new ScanCommand(scanParams));
      if (scanResult.Items) {
        items.push(...scanResult.Items);
      }
      lastEvaluatedKey = scanResult.LastEvaluatedKey;
    } while (lastEvaluatedKey);

    return items;
  }

  /**
   * Full scan of a secondary index (all pages). Only cheap for small or sparse indexes.
   */
  public scanIndex = async (parms: { indexName: string }): Promise<any[]> => {
    const { client, params: { tableName } } = this;
    const items: any[] = [];
    let lastEvaluatedKey = undefined;

    do {
      const scanParams: ScanCommandInput = {
        TableName: tableName,
        IndexName: parms.indexName,
        ExclusiveStartKey: lastEvaluatedKey
      };
      const scanResult = await client.send(new ScanCommand(scanParams));
      if (scanResult.Items) {
        items.push(...scanResult.Items);
      }
      lastEvaluatedKey = scanResult.LastEvaluatedKey;
    } while (lastEvaluatedKey);

    return items;
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
      
      await this.sendBatchWithRetry(deleteRequests);

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
   * Query one page of items by partition key (optional sort key prefix). Pass the returned
   * lastEvaluatedKey back as exclusiveStartKey for the next page; it is absent on the last page.
   */
  public async queryPage(parms: {
    partitionKeyValue: string, sortKeyPrefix?: string, limit: number, exclusiveStartKey?: Record<string, any>
  }): Promise<QueryPage> {
    const { client, params: { tableName, partitionKey, sortKey } } = this;
    const { partitionKeyValue, sortKeyPrefix, limit, exclusiveStartKey } = parms;

    if (!sortKey && sortKeyPrefix) {
      throw new Error('Cannot use sortKeyPrefix when table has no sort key');
    }

    const result: QueryCommandOutput = await client.send(new QueryCommand({
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
      Limit: limit,
      ExclusiveStartKey: exclusiveStartKey
    }));
    return { items: result.Items ?? [], lastEvaluatedKey: result.LastEvaluatedKey };
  }

  /**
   * Query one page of a GSI by partition key, optionally with a condition on its sort key: the sort key
   * compared with sortKeyValue by sortKeyComparison (default `=`). Items come in sort key order, ascending
   * unless scanIndexForward is false. Paging works as in queryPage().
   */
  public async queryGSIPage(parms: QueryGSIPageParams): Promise<QueryPage> {
    const { client, params: { tableName } } = this;
    const {
      indexName, gsiPartitionKey, partitionKeyValue, gsiSortKey, sortKeyValue, sortKeyComparison = '=',
      scanIndexForward, limit, exclusiveStartKey
    } = parms;
    const useSortKey = !!(gsiSortKey && sortKeyValue);

    const result: QueryCommandOutput = await client.send(new QueryCommand({
      TableName: tableName,
      IndexName: indexName,
      KeyConditionExpression: useSortKey ? `#pk = :pkValue AND #sk ${sortKeyComparison} :skValue` : `#pk = :pkValue`,
      ExpressionAttributeNames: {
        '#pk': gsiPartitionKey,
        ...(useSortKey && { '#sk': gsiSortKey })
      },
      ExpressionAttributeValues: {
        ':pkValue': partitionKeyValue,
        ...(useSortKey && { ':skValue': sortKeyValue })
      },
      ...(scanIndexForward !== undefined && { ScanIndexForward: scanIndexForward }),
      Limit: limit,
      ExclusiveStartKey: exclusiveStartKey
    }));
    return { items: result.Items ?? [], lastEvaluatedKey: result.LastEvaluatedKey };
  }

  /**
   * Batch write items to the table.
   * Handles batching automatically (max 25 items per request).
   *
   * @param items - Array of items to write
   * @param operation - 'put' or 'delete' (default: 'put')
   */
  public async batchWrite(parms: { items: any[], operation?: 'put' | 'delete' } ): Promise<void> {
    const batchSize = 25; // DynamoDB limit

    const { items, operation = 'put' } = parms;
    for (let i = 0; i < items.length; i += batchSize) {
      const batch = items.slice(i, i + batchSize);
      const requests = batch.map(item =>
        operation === 'put'
          ? { PutRequest: { Item: item } }
          : { DeleteRequest: { Key: item } }
      );

      await this.sendBatchWithRetry(requests);
    }
  }

  /**
   * Send one BatchWriteItem request (at most 25 put/delete requests).
   * BatchWriteItem does not throw when throttled - it returns the requests it skipped as
   * UnprocessedItems - so those are retried with exponential backoff, and an error is thrown if
   * any remain after the final attempt, rather than letting writes be silently dropped.
   *
   * @param requests - PutRequest/DeleteRequest objects for this table
   * @param maxAttempts - Attempts before giving up (default 5)
   */
  private async sendBatchWithRetry(requests: any[], maxAttempts: number = 5): Promise<void> {
    const { client, params: { tableName } } = this;
    let pending: any[] | undefined = requests;

    for (let attempt = 1; pending && pending.length > 0; attempt++) {
      if (attempt > maxAttempts) {
        throw new Error(`Failed to write to ${tableName}: ${pending.length} request(s) still unprocessed after ${maxAttempts} attempts`);
      }
      if (attempt > 1) {
        await new Promise(resolve => setTimeout(resolve, 100 * 2 ** (attempt - 2)));
      }

      const response: BatchWriteCommandOutput = await client.send(new BatchWriteCommand({
        RequestItems: { [tableName]: pending }
      }));

      pending = response.UnprocessedItems?.[tableName];
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
