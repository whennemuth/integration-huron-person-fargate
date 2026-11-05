import {
  PersonLookupService,
  PersonSyncService,
  BulkSyncService,
  HistoryService,
  SystemStatusService,
  RunReportingService,
  LandscapeListingService,
  PersonLookupParams
} from './ServiceTypes';

// Import mock implementations
import {
  MockPersonLookupService,
  MockPersonSyncService,
  MockBulkSyncService,
  MockHistoryService,
  MockSystemStatusService,
  MockRunReportingService,
  MockLandscapeListingService
} from '../handlers/DashboardMocks';

// Import concrete implementations
import { PersonLookup } from './PersonLookup';
import { PersonSync } from './PersonSync';
import { BulkSync } from './BulkSync';
import { History } from './History';
import { SystemStatus } from './SystemStatus';
import { runReportingFromEnvironment } from './RunReports';
import { LandscapeRegistry } from './LandscapeRegistry';
import { Config } from 'integration-huron-person';
import { CrudOperation, TestEnvironment } from 'integration-core';
import { getConfig } from '../../../src/Utils';
import { ChangeType } from '../../../src/reporting/RunReporting';

/**
 * Service provider that provides mock or concrete implementations
 * based on environment configuration
 */
export class ServiceProvider {
  // Kept for the life of the Lambda instance: the registry caches what it reads from SSM
  private static runReporting?: RunReportingService;
  private static landscapeListing?: LandscapeListingService;
  
  /**
   * Determine if we should use mock implementations. The mocks ignore the integration config, so
   * callers that only read it to hand to a service can skip reading it when this is true.
   */
  static shouldUseMocks(): boolean {
    // In Lambda environment, always use concrete implementations
    if (process.env.AWS_LAMBDA_FUNCTION_NAME) {
      return false;
    }
    
    // In local environment, use mocks if explicitly requested
    return process.env.LOCALHOST_WITH_MOCKS === 'true';
  }

  /**
   * Get person lookup service implementation
   */
  static getPersonLookupService(config:Config): PersonLookupService {
    if (this.shouldUseMocks()) {
      return new MockPersonLookupService();
    } else {
      return new PersonLookup(config);
    }
  }

  /**
   * Get person sync service implementation
   */
  static getPersonSyncService(config:Config): PersonSyncService {
    if (this.shouldUseMocks()) {
      return new MockPersonSyncService();
    } else {
      return new PersonSync(config);
    }
  }

  /**
   * Get bulk sync service implementation
   */
  static getBulkSyncService(config:Config): BulkSyncService {
    if (this.shouldUseMocks()) {
      return new MockBulkSyncService();
    } else {
      return new BulkSync(config);
    }
  }

  /**
   * Get history service implementation
   */
  static getHistoryService(): HistoryService {
    if (this.shouldUseMocks()) {
      return new MockHistoryService();
    } else {
      return new History();
    }
  }

  /**
   * Get system status service implementation
   */
  static getSystemStatusService(): SystemStatusService {
    if (this.shouldUseMocks()) {
      return new MockSystemStatusService();
    } else {
      return new SystemStatus();
    }
  }

  /**
   * Get run reporting service implementation (this landscape's run data)
   */
  static getRunReportingService(): RunReportingService {
    if (this.shouldUseMocks()) {
      return new MockRunReportingService();
    }
    return this.runReporting ??= runReportingFromEnvironment();
  }

  /**
   * Get landscape listing service implementation (the landscape picker's data)
   */
  static getLandscapeListingService(): LandscapeListingService {
    if (this.shouldUseMocks()) {
      return new MockLandscapeListingService();
    }
    return this.landscapeListing ??= LandscapeRegistry.fromEnvironment();
  }
}


/**
 * Harness: get a service from the provider and call it the way DashboardHandler does, without the
 * Lambda, CloudFront or authentication in front of it. Real services read the landscape named by
 * the environment (the same variables the dashboard stack gives the Lambda); set
 * LOCALHOST_WITH_MOCKS=true for the mocks. Read only: person sync is previewed, never pushed.
 *
 * Tasks:
 *   capabilities | runs | summary | chunks | errors | changes  (run reporting; SYNC_RUN_ID for a run,
 *                                                                default: the latest run)
 *   landscapes                                                  (landscape registry in SSM)
 *   lookup | preview                                            (person lookup / sync preview by PERSON_ID,
 *                                                                with the config from getConfig(), unless mocked)
 *   history | status                                            (stubs: only the mocks answer)
 */
if (require.main === module) {
  const testEnvironment = TestEnvironment('DASHBOARD_SERVICE_PROVIDER');
  [
    'TASK',
    'LOCALHOST_WITH_MOCKS',
    // The dashboard Lambda's environment, for run reporting and the landscape registry
    'LANDSCAPE',
    'REGION',
    'PREVIOUS_STORAGE_TYPE',
    'DYNAMODB_STATISTICS_TABLE_NAME',
    'DYNAMODB_PERSON_HISTORY_TABLE_NAME',
    'CHUNKS_BUCKET',
    'LANDSCAPES_PARAMETER_PATH',
    'DASHBOARD_DOMAIN',
    // Run reporting task parameters
    'SYNC_RUN_ID',
    'CHANGE_TYPE',
    'STATUS_CODE',
    'LIMIT',
    'CURSOR',
    // Person lookup / sync preview task parameters, and where getConfig() finds the config
    'PERSON_ID',
    'SYSTEM',
    'SEARCH_TYPE',
    'OPERATION',
    'SECRET_ARN',
    'HURON_PERSON_CONFIG_PATH'
  ].forEach(testEnvironment.getVar);

  const task = testEnvironment.getVar('TASK');
  const optionalNumber = (key: string): number | undefined => {
    const value = testEnvironment.getVar(key);
    return value ? Number(value) : undefined;
  };
  const requireVar = (key: string): string => {
    const value = testEnvironment.getVar(key);
    if (!value) {
      throw new Error(`Missing required DASHBOARD_SERVICE_PROVIDER_${key} environment variable for the ${task} task!`);
    }
    return value;
  };

  (async () => {
    const show = (label: string, result: any) => console.log(`${label}:`, JSON.stringify(result, null, 2));

    // A run for the per-run tasks: SYNC_RUN_ID, else the latest run
    const syncRunId = async (): Promise<string> => {
      const explicit = testEnvironment.getVar('SYNC_RUN_ID');
      if (explicit) return explicit;
      const runs = await ServiceProvider.getRunReportingService().listRuns();
      if (runs.length === 0) {
        throw new Error('No runs found: set DASHBOARD_SERVICE_PROVIDER_SYNC_RUN_ID');
      }
      console.log(`DASHBOARD_SERVICE_PROVIDER_SYNC_RUN_ID not set, using the latest run: ${runs[runs.length - 1]}`);
      return runs[runs.length - 1];
    };
    const page = () => ({ limit: optionalNumber('LIMIT'), cursor: testEnvironment.getVar('CURSOR') });
    // The mocks ignore the config, so don't load it (no Secrets Manager or file access)
    const config = async (): Promise<Config> => ServiceProvider.shouldUseMocks() ? {} as Config : getConfig();

    switch (task) {
      case 'capabilities':
        show('Capabilities', ServiceProvider.getRunReportingService().getCapabilities());
        break;
      case 'runs':
        show('Runs (oldest first)', await ServiceProvider.getRunReportingService().listRuns());
        break;
      case 'summary': {
        const id = await syncRunId();
        show(`Report for run ${id}`, await ServiceProvider.getRunReportingService().getRunReport(id) ?? 'Nothing recorded for this run');
        break;
      }
      case 'chunks': {
        const id = await syncRunId();
        show(`Chunk statistics for run ${id}`, await ServiceProvider.getRunReportingService().getRunChunkStatistics(id));
        break;
      }
      case 'errors': {
        const id = await syncRunId();
        show(`Errors for run ${id}`, await ServiceProvider.getRunReportingService().getRunErrors(id, {
          ...page(), statusCode: optionalNumber('STATUS_CODE')
        }));
        break;
      }
      case 'changes': {
        const id = await syncRunId();
        const changeType = (testEnvironment.getVar('CHANGE_TYPE') ?? 'NEW') as ChangeType;
        show(`${changeType} changes for run ${id}`, await ServiceProvider.getRunReportingService().getRunChanges(id, changeType, page()));
        break;
      }
      case 'landscapes':
        show('Landscapes', await ServiceProvider.getLandscapeListingService().getLandscapes());
        break;
      case 'lookup': {
        const lookupConfig = await config();
        // As the handler does: a full person, with no fields filtered out
        if (lookupConfig.dataSource?.person) {
          delete lookupConfig.dataSource.person.fieldsOfInterest;
        }
        show('Lookup result', await ServiceProvider.getPersonLookupService(lookupConfig).lookup({
          personId: requireVar('PERSON_ID'),
          system: (testEnvironment.getVar('SYSTEM') ?? 'source') as PersonLookupParams['system'],
          searchType: (testEnvironment.getVar('SEARCH_TYPE') ?? 'buid') as PersonLookupParams['searchType']
        }));
        break;
      }
      case 'preview': {
        const operation = testEnvironment.getVar('OPERATION') as CrudOperation | undefined;
        show('Sync preview', await ServiceProvider.getPersonSyncService(await config()).preview(requireVar('PERSON_ID'), operation));
        break;
      }
      case 'history':
        show('History', await ServiceProvider.getHistoryService().getHistory(undefined, undefined, optionalNumber('LIMIT')));
        break;
      case 'status':
        show('System status', await ServiceProvider.getSystemStatusService().getStatus());
        break;
      default:
        console.error(`Unknown task: ${task}. Supported tasks: capabilities, runs, summary, chunks, errors, changes, landscapes, lookup, preview, history, status`);
    }
  })().catch(error => {
    console.error('ServiceProvider harness failed:', error);
    process.exit(1);
  });
}
