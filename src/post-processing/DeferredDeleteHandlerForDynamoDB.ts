import { FieldSet } from "integration-core";
import { AbstractDeferredDeleteHandler, DeferredDeleteHandlerCoreParams } from "./AbstractDeferredDeleteHandler";
import { ChunkPopulationReader } from "./ChunkPopulationReader";
import { isDeletedHash } from "./DeletedHashMarker";
import { PersonCurrentStateTable } from "../dynamodb/PersonCurrentStateTable";
import { PersonHistoryTable, PersonHistoryRecord } from "../dynamodb/PersonHistoryTable";

export type DeferredDeleteHandlerForDynamoDBParams = DeferredDeleteHandlerCoreParams & {
  bucketName: string;
  chunkDirectory: string;              // Chunk directory for this sync run (current population source)
  personCurrentStateTableName: string; // Baseline: every person ever seen (single row each)
  personHistoryTableName: string;      // Audit trail destination for DELETED entries
  syncRunId: string;
  personIdField?: string;              // Raw field name identifying a person in chunk records
};

/**
 * Baseline (PersonCurrentStateTable) is compared against current (raw chunk files, not
 * PersonCurrentStateTable) because PersonCurrentStateTable's syncRunId only advances for persons
 * whose hash actually changed - UNCHANGED persons are never written, so it can't answer "who was
 * seen this run" on its own. See ChunkPopulationReader for the full rationale.
 */
export class DeferredDeleteHandlerForDynamoDB extends AbstractDeferredDeleteHandler {

  constructor(params: DeferredDeleteHandlerForDynamoDBParams) {
    super(params);
  }

  private readCurrentPopulation = async (): Promise<FieldSet[]> => {
    const { bucketName, chunkDirectory, region, personIdField } = this.params as DeferredDeleteHandlerForDynamoDBParams;
    const reader = new ChunkPopulationReader({ bucketName, chunkDirectory, region, personIdField });
    return await reader.getCurrentPopulation();
  }

  private readBaselinePopulation = async (): Promise<FieldSet[]> => {
    const { personCurrentStateTableName, region } = this.params as DeferredDeleteHandlerForDynamoDBParams;
    const stateTable = PersonCurrentStateTable.fromTableName(personCurrentStateTableName, region);
    const persons = await stateTable.getAllPersons();

    // Persons already soft-deleted by a previous run are not deletion candidates again.
    const notYetDeleted = persons.filter(({ hash }) => !isDeletedHash(hash));
    const alreadyDeletedCount = persons.length - notYetDeleted.length;
    if (alreadyDeletedCount > 0) {
      console.log(`  Excluded ${alreadyDeletedCount} already soft-deleted record(s) from baseline population`);
    }

    return notYetDeleted.map(({ personId, hash }) => (
      { fieldValues: [{ sourceIdentifier: personId }], hash } satisfies FieldSet
    ));
  }

  public getRemovedRecords = async (): Promise<FieldSet[]> => {
    const { findRemovedRecords, enrichRemovedRecordsWithHrn, readCurrentPopulation, readBaselinePopulation } = this;

    console.log(`Reading current population from chunk files`);
    const current = await readCurrentPopulation();
    console.log(`  Parsed ${current.length} record(s) from current population`);

    console.log(`Reading baseline population from PersonCurrentStateTable`);
    const baseline = await readBaselinePopulation();
    console.log(`  Parsed ${baseline.length} record(s) from baseline population`);

    // Step 3: Find records in baseline but NOT in current population (true removals)
    const removedRecords = findRemovedRecords(baseline, current);

    // Step 4: Enrich removed records with HRN if missing (lookup via sourceIdentifier).
    const enrichedRecords = await enrichRemovedRecordsWithHrn(removedRecords);

    return enrichedRecords;
  }

  /**
   * Mark the soft-deleted persons in PersonCurrentStateTable so later runs don't select them for
   * deletion again (see DeletedHashMarker.ts), and complete PersonHistoryTable's documented
   * NEW/UPDATED/DELETED audit vocabulary - a removal that succeeds against the target is otherwise
   * the one changeType with no history trail.
   */
  protected onSoftDeleteSuccess = async (successfulRecords: FieldSet[]): Promise<void> => {
    if (successfulRecords.length === 0) {
      return;
    }

    const { personCurrentStateTableName, personHistoryTableName, syncRunId, region } = this.params as DeferredDeleteHandlerForDynamoDBParams;
    const stateTable = PersonCurrentStateTable.fromTableName(personCurrentStateTableName, region);
    const historyTable = PersonHistoryTable.fromTableName(personHistoryTableName, region);

    const records: PersonHistoryRecord[] = [];
    for (const record of successfulRecords) {
      const sourceIdentifier = record.fieldValues.find((fv: any) => fv.sourceIdentifier)?.sourceIdentifier as string | undefined;
      if (!sourceIdentifier || !record.hash) {
        console.warn(`  ⚠ Missing sourceIdentifier or hash - skipping DELETED history entry: ${JSON.stringify(record.fieldValues)}`);
        continue;
      }
      records.push({ personId: sourceIdentifier, syncRunId, hash: record.hash, changeType: 'DELETED', previousHash: record.hash });
    }

    if (records.length === 0) {
      return;
    }

    console.log(`Marking ${records.length} record(s) as soft-deleted in PersonCurrentStateTable`);
    await stateTable.markDeleted(records.map(({ personId, hash }) => ({ personId, hash })), syncRunId);

    console.log(`Writing ${records.length} DELETED history entrie(s) to PersonHistoryTable`);
    await historyTable.batchWriteHistory(records);
  }

}
