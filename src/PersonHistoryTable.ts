import { IContext } from '../context/IContext';

/**
 * PersonHistory DynamoDB Table Constants
 * 
 * Table Design:
 * - Purpose: Append-only audit trail of person state changes
 * - PK: personId - Unique identifier for each person
 * - SK: syncRunId - ISO timestamp of sync run that created this record
 * 
 * Attributes:
 * - hash: string - Hash value at this point in time
 * - changeType: 'NEW' | 'UPDATED' | 'DELETED' - Type of change
 * - previousHash?: string - Previous hash value (for UPDATED only)
 * 
 * Write Policy:
 * - NEW: First time person appears in source
 * - UPDATED: Hash changed from previous sync
 * - DELETED: Person removed from source (detected by merger)
 * - UNCHANGED: DO NOT WRITE (skipped entirely)
 * 
 * Access Patterns:
 * 1. Get person's complete history: Query by personId
 * 2. Get all changes in a sync run: Query GSI1 by syncRunId
 * 3. Get all changes of a specific type: Query GSI2 by changeType
 * 
 * Usage:
 * - Processors: PutItem for NEW and UPDATED persons
 * - Merger: PutItem for DELETED persons
 * - Reporting: Query for audit trails and analytics
 */

/**
 * Generate table name following existing naming convention
 * @param context - IContext with STACK_ID and TAGS.Landscape
 * @returns Table name: ${STACK_ID}-person-history-${landscape}
 */
export const DYNAMODB_TABLE_NAME = (context: IContext) => 
  `${context.STACK_ID}-person-history-${context.TAGS.Landscape.toLowerCase()}`;

/**
 * Partition key: personId
 * Example: "U12345678"
 */
export const DYNAMODB_PARTITION_KEY = 'personId';

/**
 * Sort key: syncRunId (ISO timestamp)
 * Example: "2026-03-03T19:58:41.277Z"
 * Enables chronological ordering of history records
 */
export const DYNAMODB_SORT_KEY = 'syncRunId';

/**
 * GSI1: Query all changes in a specific sync run
 * PK: syncRunId
 * SK: changeType_personId (composite for filtering)
 * Use case: "Get all NEW persons in sync 2026-03-03T19:58:41.277Z"
 */
export const DYNAMODB_GSI1_INDEX_NAME = 'syncRunId-changeType-index';
export const DYNAMODB_GSI1_PARTITION_KEY = 'syncRunId';
export const DYNAMODB_GSI1_SORT_KEY = 'changeType_personId';

/**
 * GSI2: Query all changes of a specific type across runs
 * PK: changeType
 * SK: syncRunId
 * Use case: "Get all DELETED persons in the last 30 days"
 */
export const DYNAMODB_GSI2_INDEX_NAME = 'changeType-syncRunId-index';
export const DYNAMODB_GSI2_PARTITION_KEY = 'changeType';
export const DYNAMODB_GSI2_SORT_KEY = 'syncRunId';
