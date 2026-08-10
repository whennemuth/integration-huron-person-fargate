import { IContext } from '../context/IContext';

/**
 * PersonCurrentState DynamoDB Table Constants
 * 
 * Table Design:
 * - Purpose: Track current hash state for each person (one record per person)
 * - PK: personId - Unique identifier for each person
 * - No SK: Single record per person (overwrite on change)
 * 
 * Attributes:
 * - hash: string - Current computed hash
 * - syncRunId: string - ISO timestamp of last sync that modified this person
 * 
 * Access Patterns:
 * 1. Batch fetch by personId: Used by processors to get previous hashes for delta computation
 * 2. Query by syncRunId via GSI: Used by merger for deletion detection
 * 
 * Usage:
 * - Processors: BatchGetItem to fetch previous state for chunk
 * - Processors: BatchWriteItem to update/create records (skip UNCHANGED)
 * - Merger: Query GSI to find all persons seen in current sync
 */

/**
 * Generate table name following existing naming convention
 * @param context - IContext with STACK_ID and TAGS.Landscape
 * @returns Table name: ${STACK_ID}-person-current-state-${landscape}
 */
export const DYNAMODB_TABLE_NAME = (context: IContext) => 
  `${context.STACK_ID}-person-current-state-${context.TAGS.Landscape.toLowerCase()}`;

/**
 * Partition key: personId
 * Example: "U12345678"
 */
export const DYNAMODB_PARTITION_KEY = 'personId';

/**
 * GSI for querying by sync run
 * PK: syncRunId (ISO timestamp)
 * SK: personId
 * Use case: "Get all persons seen in sync run 2026-03-03T19:58:41.277Z"
 */
export const DYNAMODB_GSI_INDEX_NAME = 'syncRunId-personId-index';
export const DYNAMODB_GSI_PARTITION_KEY = 'syncRunId';
export const DYNAMODB_GSI_SORT_KEY = 'personId';
