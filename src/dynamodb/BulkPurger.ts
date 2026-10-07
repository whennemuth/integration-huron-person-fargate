import { TestEnvironment } from "integration-core";
import { Config } from "integration-huron-person";
import {
  MockTargetPersonTable,
  DYNAMODB_TABLE_NAME as mockTargetPersonTableName} from "./MockTargetPersonTable";
import {
  PersonCurrentStateTable,
  DYNAMODB_TABLE_NAME as personCurrentStateTableName
} from "./PersonCurrentStateTable";
import {
  PersonHistoryTable,
  DYNAMODB_TABLE_NAME as personHistoryTableName
} from "./PersonHistoryTable";
import {
  DYNAMODB_TABLE_NAME as statisticsTableName,
  StatisticsTable
} from "./StatisticsTable";
import { IContext } from "../../context/IContext";
import { getConfig, isMockLandscape, MOCK_LANDSCAPE_PATTERN } from "../Utils";

export enum BulkPurgerMode {
  DELETE = 'delete',
  TRUNCATE = 'truncate',
}

export enum BulkPurgerTables {
  MOCK_TARGET_PERSON = 'mock_target_person',
  PERSON_CURRENT_STATE = 'person_current_state',
  PERSON_HISTORY = 'person_history',
  STATISTICS = 'statistics'
}

export type BulkPurgerConfig = {
  mode: BulkPurgerMode;
  tables?: BulkPurgerTables[];
  syncRunId?: string;
  chunkSize?: number;
  dryRun?: boolean;
  /** The stack/landscape whose tables are purged. Defaults to context/context.json. */
  context?: IContext;
  /**
   * Purging is refused unless the landscape is a mock landscape (see isMockLandscape), whose
   * tables only ever hold mock-run data. Set to true to deliberately purge a non-mock landscape.
   */
  force?: boolean;
  /** Only needed to truncate the mock target table - loaded via getConfig() if omitted. */
  integrationConfig?: Config;
};

/**
 * Bulk purges the standard tables of one landscape - used to reset a mock landscape's run state
 * (see MockLandscapeRunnerDecorator), since a mock landscape uses the same tables as any other.
 */
export class BulkPurger {
  private tables: BulkPurgerTables[] = [];
  private mode: BulkPurgerMode;
  private dryRun:boolean = true;
  private force:boolean = false;
  private chunkSize?: number;
  private syncRunId?: string;

  constructor(private config: BulkPurgerConfig) {
    const { context, integrationConfig, ...loggable } = this.config;
    console.log(`BulkPurger initialized with: ${JSON.stringify(loggable, null, 2)}`);

    this.mode = config.mode;
    this.syncRunId = config.syncRunId;
    this.chunkSize = config.chunkSize ?? 25;

    if(this.mode === BulkPurgerMode.DELETE && !this.syncRunId) {
      throw new Error('syncRunId is required when mode is DELETE');
    }

    if(typeof config.dryRun === 'boolean') {
      this.dryRun = config.dryRun;
    }
    if(typeof config.force === 'boolean') {
      this.force = config.force;
    }

    if((config.tables ?? []).length === 0) {
      // Assume all tables if not specified
      // this.tables.push(BulkPurgerTables.MOCK_TARGET_PERSON);
      this.tables.push(BulkPurgerTables.PERSON_CURRENT_STATE);
      this.tables.push(BulkPurgerTables.PERSON_HISTORY);
      this.tables.push(BulkPurgerTables.STATISTICS);
    }
    else {
      this.tables = config.tables ?? [];
    }

    console.log(`BulkPurger config resolved to: ${JSON.stringify({
      tables: this.tables,
      mode: this.mode,
      dryRun: this.dryRun,
      force: this.force,
      syncRunId: this.syncRunId,
      chunkSize: this.chunkSize
    }, null, 2)}`);
  }

  protected async loadContext(): Promise<IContext> {
    if (this.config.context) {
      return this.config.context;
    }
    const context = await require('../../context/context.json') as IContext;
    return context;
  }

  public purge = async (): Promise<void> => {
    const { mode, loadContext, Delete, truncate, force } = this;
    const context = await loadContext.call(this);
    const landscape = context.TAGS.Landscape.toLowerCase();
    if (!isMockLandscape(landscape)) {
      if (!force) {
        throw new Error(`Refusing to purge tables of landscape "${landscape}": it is not a mock landscape ` +
          `(must match ${MOCK_LANDSCAPE_PATTERN}). Set force=true to purge it anyway.`);
      }
      console.warn(`⚠️  force=true: purging tables of NON-mock landscape "${landscape}"`);
    }
    const { DELETE, TRUNCATE } = BulkPurgerMode;
    switch(mode) {
      case DELETE:
        await Delete(context);
        break;
      case TRUNCATE:
        await truncate(context);
        break;
    }
  }

  private Delete = async (context: IContext): Promise<void> => {
    const { mode, syncRunId, tables, dryRun } = this;
    let tableName: string;
    let msg: string;

    if(!syncRunId) {
      throw new Error('syncRunId is required when mode is DELETE');
    }
    for (const table of tables) {
      switch(table) {
        case BulkPurgerTables.STATISTICS:
          tableName = statisticsTableName(context);
          process.env.STATISTICS_TABLE_STATISTICS_TABLE_NAME_OVERRIDE = tableName;
          if(syncRunId) {
            process.env.STATISTICS_TABLE_STATISTICS_TABLE_INTEGRATION_TIMESTAMP = syncRunId;
          }
          process.env.STATISTICS_TABLE_INTEGRATION_TIMESTAMP = syncRunId;
          process.env.STATISTICS_TABLE_TASK = 'delete';
          msg = `Running ${mode} against ${tableName} with syncRunId: ${syncRunId}`
          if(dryRun) {
            console.log(`DRYRUN: ${msg}`);
            break;
          }
          console.log(msg);
          break;
        default:
          console.warn(`DELETE mode is not implemented for table: ${table}, skipping...`);
          continue;
      }
    }
  }

  private truncate = async (context: IContext): Promise<void> => {
    const { mode, tables, chunkSize, dryRun } = this;
    const { MOCK_TARGET_PERSON, PERSON_CURRENT_STATE, PERSON_HISTORY, STATISTICS } = BulkPurgerTables;
    const { REGION: region } = context;

    for (const table of tables) {
      let tableName: string;
      let purge: () => Promise<void>;

      switch(table) {
        case MOCK_TARGET_PERSON:
          tableName = mockTargetPersonTableName(context);
          purge = async () => {
            const config = this.config.integrationConfig ?? await getConfig();
            await new MockTargetPersonTable({ config, tableName }).truncate(chunkSize);
          };
          break;
        case PERSON_CURRENT_STATE:
          tableName = personCurrentStateTableName(context);
          purge = () => PersonCurrentStateTable.fromTableName(tableName, region).truncate(chunkSize);
          break;
        case PERSON_HISTORY:
          tableName = personHistoryTableName(context);
          purge = () => PersonHistoryTable.fromTableName(tableName, region).truncate(chunkSize);
          break;
        case STATISTICS:
          tableName = statisticsTableName(context);
          purge = () => StatisticsTable.fromTableName(tableName, region).truncate(chunkSize);
          break;
        default:
          console.warn(`Unknown table: ${table}, skipping...`);
          continue;
      }

      const msg = `Running ${mode} against ${tableName}`;
      if(dryRun) {
        console.log(`DRYRUN: ${msg}`);
        continue;
      }
      console.log(msg);
      await purge();
    }
  }
}


if(require.main === module) {
  const testEnvironment = TestEnvironment('BULK_PURGER');
  [
    'MODE',
    'TABLES',
    'FORCE',
    'TRUNCATE_CHUNK_SIZE',
    'SYNC_RUN_ID',
    'DRYRUN'
  ].forEach(testEnvironment.getVar);

  const config: BulkPurgerConfig = {
    mode: testEnvironment.getVar('MODE') as BulkPurgerMode,
    tables: (testEnvironment.getVar('TABLES') ?? '').split(',').map(table => table.trim()).filter(table => table) as BulkPurgerTables[],
    force: testEnvironment.getVar('FORCE') === 'true',
    syncRunId: testEnvironment.getVar('SYNC_RUN_ID'),
    chunkSize: parseInt(testEnvironment.getVar('TRUNCATE_CHUNK_SIZE') ?? '25', 10),
    dryRun: testEnvironment.getVar('DRYRUN') !== 'false'
  };
  const bulkPurger = new BulkPurger(config);

  bulkPurger.purge();
}
