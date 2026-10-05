import { TestEnvironment } from "integration-core";
import { 
  DYNAMODB_TABLE_NAME as personRecordProcessorLogTableName 
} from "./PersonRecordProcessorLogTable";
import { 
  main as mockTargetPersonTableMain,
  DYNAMODB_TABLE_NAME as mockTargetPersonTableName} from "./MockTargetPersonTable";
import { 
  main as personCurrentStateTableMain,
  DYNAMODB_TABLE_NAME as personCurrentStateTableName,
  DYNAMODB_MOCK_TABLE_NAME as personCurrentStateMockTableName
} from "./PersonCurrentStateTable";
import { 
  main as personHistoryTableMain,
  DYNAMODB_TABLE_NAME as personHistoryTableName,
  DYNAMODB_MOCK_TABLE_NAME as personHistoryMockTableName 
} from "./PersonHistoryTable";
import { 
  main as statisticsTableMain,
  DYNAMODB_TABLE_NAME as statisticsTableName,
  DYNAMODB_MOCK_TABLE_NAME as statisticsMockTableName
} from "./StatisticsTable";
import { IContext } from "../../context/IContext";

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
  mock?: boolean;
  syncRunId?: string;
  chunkSize?: number;
  dryRun?: boolean;
};

export class BulkPurger {
  private tables: BulkPurgerTables[] = [];
  private mode: BulkPurgerMode;
  private mock:boolean = true;
  private dryRun:boolean = true;
  private chunkSize?: number;
  private syncRunId?: string;

  constructor(private config: BulkPurgerConfig) {
    console.log(`BulkPurger initialized with: ${JSON.stringify(this.config, null, 2)}`);

    this.mode = config.mode;
    this.syncRunId = config.syncRunId;
    this.chunkSize = config.chunkSize ?? 25;

    if(this.mode === BulkPurgerMode.DELETE && !this.syncRunId) {
      throw new Error('syncRunId is required when mode is DELETE');
    }

    // Default mock to true, and only set to false if explicitly false in the config.
    if(typeof config.mock === 'boolean') {
      this.mock = config.mock;
    }
    if(typeof config.dryRun === 'boolean') {
      this.dryRun = config.dryRun;
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
      mock: this.mock,
      dryRun: this.dryRun,
      syncRunId: this.syncRunId,
      chunkSize: this.chunkSize
    }, null, 2)}`);
  }

  protected async loadContext(): Promise<IContext> {
    const context = await require('../../context/context.json') as IContext;
    return context;
  }

  public purge = async (): Promise<void> => {
    const { mode, loadContext, Delete, truncate} = this;
    const context = await loadContext();
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
    const { mode, syncRunId, tables, mock, dryRun } = this;
    let tableName: string;
    let msg: string;

    if(!syncRunId) {
      throw new Error('syncRunId is required when mode is DELETE');
    }
    for (const table of tables) {
      switch(table) {
        case BulkPurgerTables.STATISTICS:
          tableName = statisticsTableName(context);
          if(mock) {
            tableName = statisticsMockTableName(context);
            process.env.STATISTICS_TABLE_STATISTICS_TABLE_NAME_OVERRIDE = tableName;
          }
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
    const { mock, mode, tables, chunkSize, dryRun } = this;
    const { MOCK_TARGET_PERSON, PERSON_CURRENT_STATE, PERSON_HISTORY, STATISTICS } = BulkPurgerTables;
    let tableName: string;
    let msg: string;

    for (const table of tables) {
      // Set the environment variables for each table before purging
      switch(table) {

        case MOCK_TARGET_PERSON:
          process.env.MOCK_TARGET_PERSON_TABLE_MOCK_TARGET_PERSON_TABLE_TASK = mode;
          tableName = mockTargetPersonTableName(context);
          process.env.DYNAMODB_MOCK_TARGET_PERSON_TABLE_NAME = tableName;
          msg = `Running ${mode} against ${tableName}`
          if(dryRun) {
            console.log(`DRYRUN: ${msg}`);
            break;
          }
          console.log(msg);
          await mockTargetPersonTableMain();
          break;

        case PERSON_CURRENT_STATE:
          tableName = personCurrentStateTableName(context);
          if(mock) {
            tableName = personCurrentStateMockTableName(context);
            process.env.PERSON_CURRENT_STATE_TABLE_PERSON_CURRENT_STATE_TABLE_NAME_OVERRIDE = personCurrentStateMockTableName(context);
          }
          process.env.PERSON_CURRENT_STATE_TABLE_PERSON_CURRENT_STATE_TABLE_TASK = mode;
          process.env.PERSON_CURRENT_STATE_TABLE_TRUNCATE_CHUNK_SIZE = chunkSize?.toString();
          msg = `Running ${mode} against ${tableName}`
          if(dryRun) {
            console.log(`DRYRUN: ${msg}`);
            break;
          }
          console.log(msg);
          await personCurrentStateTableMain();
          break;

        case PERSON_HISTORY:
          tableName = personHistoryTableName(context);
          if(mock) {
            tableName = personHistoryMockTableName(context);
            process.env.PERSON_HISTORY_TABLE_PERSON_HISTORY_TABLE_NAME_OVERRIDE = tableName;
          }
          process.env.PERSON_HISTORY_TABLE_PERSON_HISTORY_TABLE_TASK = mode;
          process.env.PERSON_HISTORY_TABLE_TRUNCATE_CHUNK_SIZE = chunkSize?.toString();
          msg = `Running ${mode} against ${tableName}`
          if(dryRun) {
            console.log(`DRYRUN: ${msg}`);
            break;
          }
          console.log(msg);
          await personHistoryTableMain();
          break;

        case STATISTICS:
          tableName = statisticsTableName(context);
          if(mock) {
            tableName = statisticsMockTableName(context);
            process.env.STATISTICS_TABLE_STATISTICS_TABLE_NAME_OVERRIDE = tableName;
          }
          process.env.STATISTICS_TABLE_STATISTICS_TABLE_TASK = mode;
          process.env.STATISTICS_TABLE_TRUNCATE_CHUNK_SIZE = chunkSize?.toString();
          msg = `Running ${mode} against ${tableName}`
          if(dryRun) {
            console.log(`DRYRUN: ${msg}`);
            break;
          }
          console.log(msg);
          await statisticsTableMain();
          break;
      }
    }
  }
}


if(require.main === module) {
  const testEnvironment = TestEnvironment('BULK_PURGER');
  [ 
    'MODE', 
    'TABLES', 
    'MOCK', 
    'TRUNCATE_CHUNK_SIZE',
    'SYNC_RUN_ID',
    'DRYRUN'
  ].forEach(testEnvironment.getVar);
  
  const config: BulkPurgerConfig = {
    mode: testEnvironment.getVar('MODE') as BulkPurgerMode,
    tables: (testEnvironment.getVar('TABLES') ?? '').split(',').map(table => table.trim()).filter(table => table) as BulkPurgerTables[],
    mock: testEnvironment.getVar('MOCK') !== 'false',
    syncRunId: testEnvironment.getVar('SYNC_RUN_ID'),
    chunkSize: parseInt(testEnvironment.getVar('TRUNCATE_CHUNK_SIZE') ?? '25', 10),
    dryRun: testEnvironment.getVar('DRYRUN') !== 'false'
  };
  const bulkPurger = new BulkPurger(config);

  bulkPurger.purge();
}