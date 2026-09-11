import { IContext } from '../../context/IContext';
import { AbstractDynamoDbTable, DynamoDBTable } from './DynamoDBTable';

/**
 * Shared log table for ALL personRecordProcessor customizations (see
 * src/processing/custom/AbstractCustomPersonProcessor.ts). Each customization's log entries are
 * partitioned by its own Customization enum value, with a generic `data` attribute holding
 * whatever JSON shape that customization needs - keeping this table reusable for future
 * customizations without CDK/schema changes.
 */
export const DYNAMODB_TABLE_NAME = (context: IContext) => `${context.STACK_ID}-person-record-processor-log-${context.TAGS.Landscape.toLowerCase()}`;
export const DYNAMODB_PARTITION_KEY = 'customization';
export const DYNAMODB_SORT_KEY = 'sortKey';

export class PersonRecordProcessorLogTable {
  private table: AbstractDynamoDbTable;

  constructor(private context: IContext, table?: AbstractDynamoDbTable) {
    if (table) {
      this.table = table;
    } else {
      const region = context.REGION;
      const tableName = DYNAMODB_TABLE_NAME(context);
      this.table = new DynamoDBTable({ region, tableName, partitionKey: DYNAMODB_PARTITION_KEY, sortKey: DYNAMODB_SORT_KEY });
    }
  }

  /**
   * Build a PersonRecordProcessorLogTable from an explicit table name instead of an IContext,
   * for runtime paths (processor Docker entry points) where a full IContext isn't available.
   */
  public static fromTableName(tableName: string, region?: string): PersonRecordProcessorLogTable {
    const table = new DynamoDBTable({
      region: region || process.env.REGION || 'us-east-1',
      tableName, partitionKey: DYNAMODB_PARTITION_KEY, sortKey: DYNAMODB_SORT_KEY
    });
    return new PersonRecordProcessorLogTable({} as IContext, table);
  }

  /**
   * Write a single customization-tagged log entry. sortKey combines timestamp + personid so
   * entries are both chronologically browsable and never collide for the same person.
   */
  public putEntry = async (customization: string, personid: string, data: Record<string, any>): Promise<void> => {
    const sortKey = `${new Date().toISOString()}#${personid}`;
    await this.table.putItem({ customization, sortKey, personid, data });
  }
}
