
import { IContext } from '../../context/IContext';
import { PersonCurrentStateTable } from './PersonCurrentStateTable';
import { PersonHistoryTable } from './PersonHistoryTable';
import { StatisticsTable } from './StatisticsTable';

export const ISO_TIMESTAMP_REGEX = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/;

/**
 * Utility class for pruning DynamoDB tables for a specific integration run.
 * 
 * This class orchestrates the deletion of all records associated with a specific
 * integration run (identified by ISO timestamp) across three tables:
 * - StatisticsTable: Run metadata, flags, errors, chunk statuses
 * - PersonHistoryTable: Audit trail of person changes
 * - PersonCurrentStateTable: Current person state (with restoration from previous run)
 * 
 * The pruning operation restores the database to the state it was in BEFORE the
 * specified integration run by:
 * 1. Removing all statistics/metadata for the run
 * 2. Removing all history records for the run
 * 3. Removing current state records and restoring from the previous run
 * 
 * Usage:
 * ```typescript
 * const pruner = new IntegrationRunPruner('2026-03-03T19:58:41.277Z', context);
 * const result = await pruner.prune();
 * console.log(result);
 * // {
 * //   statisticsDeleted: 42,
 * //   historyDeleted: 150,
 * //   currentStateDeleted: 150,
 * //   currentStateRestored: 148
 * // }
 * ```
 */
export class IntegrationRunPruner {

  constructor(
    private isoTimestamp: string,
    private context: IContext
  ) {
    if (!ISO_TIMESTAMP_REGEX.test(isoTimestamp)) {
      throw new Error(`The value "${isoTimestamp}" is not a valid ISO timestamp`);
    }
  }

  /**
   * Prune all records for the specified integration run across all three tables.
   * 
   * Execution order:
   * 1. StatisticsTable: Remove run metadata, flags, errors, chunk statuses
   * 2. PersonHistoryTable: Remove audit trail records for this run
   * 3. PersonCurrentStateTable: Remove current state and restore from previous run
   * 
   * This order ensures that:
   * - Statistics/metadata are cleaned first (no dependencies)
   * - History is available when restoring PersonCurrentState
   * - Current state is restored to maintain data integrity
   * 
   * @returns Summary of deletion counts for each table
   */
  public prune = async (): Promise<{
    statisticsDeleted: number;
    historyDeleted: number;
    currentStateDeleted: number;
    currentStateRestored: number;
  }> => {
    console.log(`\n=== Pruning Integration Run: ${this.isoTimestamp} ===\n`);

    // Initialize table instances
    const statisticsTable = new StatisticsTable(this.context);
    const historyTable = new PersonHistoryTable(this.context);
    const currentStateTable = new PersonCurrentStateTable(this.context);

    // Step 1: Delete from StatisticsTable
    console.log('Step 1: Deleting from StatisticsTable...');
    const statisticsDeleted = await statisticsTable.deleteByPartitionKey(this.isoTimestamp);
    console.log(`✓ StatisticsTable: ${statisticsDeleted} records deleted\n`);

    // Step 2: Delete from PersonHistoryTable
    console.log('Step 2: Deleting from PersonHistoryTable...');
    const historyDeleted = await historyTable.deleteByPartitionKey(this.isoTimestamp);
    console.log(`✓ PersonHistoryTable: ${historyDeleted} records deleted\n`);

    // Step 3: Delete from PersonCurrentStateTable with restoration
    console.log('Step 3: Deleting from PersonCurrentStateTable and restoring previous state...');
    const { deletedCount, restoredCount } = await currentStateTable.deleteByPartitionKeyAndRestore(
      this.isoTimestamp,
      historyTable
    );
    console.log(`✓ PersonCurrentStateTable: ${deletedCount} records deleted, ${restoredCount} restored\n`);

    const result = {
      statisticsDeleted,
      historyDeleted,
      currentStateDeleted: deletedCount,
      currentStateRestored: restoredCount
    };

    console.log('=== Pruning Complete ===');
    console.log(JSON.stringify(result, null, 2));

    return result;
  }
}


if (require.main === module) {
  const { TestEnvironment } = require('integration-core');
  const testEnvironment = TestEnvironment('INTEGRATION_RUN_PRUNER');
  [
    'INTEGRATION_RUN_PRUNER_TIMESTAMP'
  ].forEach(testEnvironment.getVar);

  const { INTEGRATION_RUN_PRUNER_TIMESTAMP: timestamp } = process.env;

  (async () => {
    if (!timestamp) {
      console.error('Missing required INTEGRATION_RUN_PRUNER_TIMESTAMP environment variable!');
      console.error('Usage: INTEGRATION_RUN_PRUNER_TIMESTAMP=2026-03-03T19:58:41.277Z npx ts-node src/dynamodb/IntegrationRun.ts');
      process.exit(1);
    }

    const context = require('../../context/context.json') as IContext;
    
    try {
      const pruner = new IntegrationRunPruner(timestamp, context);
      const result = await pruner.prune();
      
      console.log('\n=== Pruning Summary ===');
      console.log(`Integration Run: ${timestamp}`);
      console.log(`Statistics Deleted: ${result.statisticsDeleted}`);
      console.log(`History Deleted: ${result.historyDeleted}`);
      console.log(`Current State Deleted: ${result.currentStateDeleted}`);
      console.log(`Current State Restored: ${result.currentStateRestored}`);
      console.log('======================\n');
      
      process.exit(0);
    } catch (error) {
      console.error('Error during pruning:', error);
      process.exit(1);
    }
  })();
}
