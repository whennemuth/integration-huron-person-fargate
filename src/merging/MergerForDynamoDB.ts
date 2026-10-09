/**
 * DynamoDB-based Merger Implementation
 * 
 * Extends AbstractMerger to implement DynamoDB-specific merge logic.
 * 
 * In DynamoDB mode, processors write directly to shared PersonCurrentStateTable
 * and PersonHistoryTable using atomic DynamoDB operations. This means:
 * - No file consolidation is needed (unlike S3 mode)
 * - No baseline merging is needed (state is already in DynamoDB)
 * - No cleanup of chunk files is needed
 * 
 * However, the merger service is still needed for:
 * - DeferredDeleteHandler invocation (soft-deleting removed records)
 * - Consistent pipeline completion signaling
 * - Error handling and logging
 * 
 * merge() still returns a (non-null) result so AbstractMerger.main()'s Step 4 gate
 * (`if (result)`) runs runDeferredDeletes() - returning null here would silently skip deletion
 * handling entirely, which is the original bug this class exists to fix.
 */

import { FieldDefinitions } from 'integration-huron-person';
import { extractChunkDirectory } from '../chunking/filedrop/ChunkPathUtils';
import { MetadataFactoryForBootstrap, StandardMetadataUtils } from '../chunking/metadata';
import { SyncPopulation } from '../../docker/chunkTypes';
import { AbstractMerger, MergeContext, MergeResult, TaskParameters } from './AbstractMerger';
import { DeferredDeleteHandlerForDynamoDB, DeferredDeleteHandlerForDynamoDBParams } from './DeferredDeleteHandlerForDynamoDB';

export type TaskParametersForDynamoDB = TaskParameters & {
  chunksBucket: string;
  chunkDirectory: string | null;
};

export type MergeContextForDynamoDB = MergeContext & {
  bucketName: string;
  chunkDir: string;
  syncRunId: string;
  primaryKeyFieldNames: string[];
};

export type MergeResultForDynamoDB = MergeResult & {
  chunkDir: string;
  syncRunId: string;
};


export class MergerForDynamoDB extends AbstractMerger {

  public async getTaskParameters(): Promise<TaskParametersForDynamoDB | null> {
    if (this.taskParameters) {
      return this.taskParameters as TaskParametersForDynamoDB;
    }

    const { SQS_QUEUE_URL, CHUNKS_BUCKET, CHUNK_DIRECTORY, INPUT_KEY } = process.env;
    let taskParams: TaskParametersForDynamoDB | null = null;

    if (SQS_QUEUE_URL) {
      const msgBody = await this.getMessageFromSQS();
      if (msgBody) {
        taskParams = {
          chunksBucket: msgBody.chunksBucket,
          chunkDirectory: msgBody.chunkDirectory || null,
          createdAt: msgBody.createdAt,
        };
      }
    }
    else {
      if (CHUNKS_BUCKET && (CHUNK_DIRECTORY || INPUT_KEY)) {
        console.log('Running in local context - reading task parameters from environment variables');
        taskParams = {
          chunksBucket: CHUNKS_BUCKET,
          chunkDirectory: CHUNK_DIRECTORY || (INPUT_KEY ? extractChunkDirectory(INPUT_KEY) : null),
        };
      }
    }

    if (taskParams && taskParams.chunkDirectory) {
      this.taskParameters = taskParams;
      return taskParams;
    }

    console.error('ERROR: No task parameters available (checked SQS queue and environment variables)');
    return null;
  }

  public async getMergeContext(taskParams?: TaskParametersForDynamoDB): Promise<MergeContextForDynamoDB | null> {
    if (this.mergeContext) {
      return this.mergeContext as MergeContextForDynamoDB;
    }

    if (!taskParams) {
      taskParams = await this.getTaskParameters() as TaskParametersForDynamoDB;
    }
    const { chunksBucket: bucketName, chunkDirectory: chunkDir } = taskParams;

    const { REGION: region, DRY_RUN = 'false' } = process.env;
    const dryRun = `${DRY_RUN}`.trim().toLowerCase() === 'true';

    const syncRunId = new StandardMetadataUtils({ chunkDirectory: chunkDir! }).extractSyncRunId();
    const primaryKeyFieldNames = FieldDefinitions.filter(fd => fd.isPrimaryKey).map(fd => fd.name);

    this.mergeContext = {
      bucketName,
      chunkDir: chunkDir!,
      region,
      dryRun,
      syncRunId,
      primaryKeyFieldNames,
    } as MergeContextForDynamoDB;

    return this.mergeContext as MergeContextForDynamoDB;
  }

  /**
   * DynamoDB-specific merge implementation (minimal/no-op).
   * 
   * In DynamoDB mode:
   * - Processors already wrote to PersonCurrentStateTable/PersonHistoryTable
   * - No file consolidation or baseline merging needed
   * 
   * @returns A non-null marker result so main()'s deferred-deletes step still runs
   */
  protected async merge(taskParams?: TaskParametersForDynamoDB): Promise<MergeResultForDynamoDB | null> {
    const mergeContext = await this.getMergeContext(taskParams) as MergeContextForDynamoDB;
    const { chunkDir, syncRunId } = mergeContext;

    console.log(`\nDynamoDB Merge Mode:`);
    console.log(`  Storage type: DynamoDB`);
    console.log(`  State already in PersonCurrentStateTable (no file consolidation needed)`);
    console.log(`  History already in PersonHistoryTable (no baseline merging needed)`);
    console.log(`  Processors wrote directly to shared tables using atomic DynamoDB operations`);

    console.log(`\nDynamoDB Merge Complete (no-op):`);
    console.log(`  File consolidation: Not needed (DynamoDB atomic writes)`);
    console.log(`  Baseline merging: Not needed (state in PersonCurrentStateTable)`);
    console.log(`  Cleanup: Not needed (no temporary files)`);

    return { chunkDir, syncRunId };
  }

  protected async runDeferredDeletes(result: MergeResultForDynamoDB): Promise<void> {
    const mergeContext = await this.getMergeContext() as MergeContextForDynamoDB;
    const { bucketName, chunkDir, region, syncRunId, primaryKeyFieldNames } = mergeContext;

    // First check if the population type for this sync is compatible with deletion processing.
    const { metadata, flags } = await new MetadataFactoryForBootstrap().readFlagsForBootstrap({
      bucketName, chunkDirectory: chunkDir, region
    });
    const { syncPopulation } = flags;

    if (await metadata.terminalErrorExists({ bucketName, chunkDirectory: chunkDir, region })) {
      console.error(`  ⛔ Run is marked failed (TERMINAL_ERROR) - deletion handling skipped.`);
      return;
    }

    if (syncPopulation === SyncPopulation.PersonDelta) {
      console.log(`  Sync population type is PersonDelta - Deletion handling does NOT apply.`);
      return;
    }

    const personCurrentStateTableName = process.env.DYNAMODB_PERSON_CURRENT_STATE_TABLE_NAME;
    const personHistoryTableName = process.env.DYNAMODB_PERSON_HISTORY_TABLE_NAME;

    if (!personCurrentStateTableName || !personHistoryTableName) {
      console.warn(`  Missing DynamoDB table name(s) for deletion processing. Skipping.`);
      return;
    }

    console.log(`\nStep 3.5: Processing deletions`);
    try {
      const deleteHandler = new DeferredDeleteHandlerForDynamoDB({
        bucketName,
        chunkDirectory: chunkDir,
        personCurrentStateTableName,
        personHistoryTableName,
        syncRunId,
        primaryKeyFieldNames,
        region,
      } as DeferredDeleteHandlerForDynamoDBParams);

      const deletionResult = await deleteHandler.processDeletes();
      console.log(`  ${deletionResult.message}`);
      if (deletionResult.totalProcessed > 0) {
        console.log(`  Deleted: ${deletionResult.deletedCount} of ${deletionResult.totalProcessed}`);
        if (deletionResult.failedCount > 0) {
          console.warn(`  Failed: ${deletionResult.failedCount} deletions failed`);
        }
      }
    } catch (deleteError: any) {
      console.error(`  Failed to process deletions: ${deleteError.message}`);
      // Don't fail the entire merge if deletions fail - log and continue
      console.warn(`  Continuing with merge despite deletion failure`);
    }
  }
}

