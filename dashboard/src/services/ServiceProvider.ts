import {
  PersonLookupService,
  PersonSyncService,
  BulkSyncService,
  SystemStatusService,
  RunReportingService,
  LandscapeListingService,
  PersonLookupParams
} from './ServiceTypes';

// The offline services: built-in data, no AWS access (a local run with LOCAL_OFFLINE=true)
import {
  OfflinePersonLookupService,
  OfflinePersonSyncService,
  OfflineBulkSyncService,
  OfflineSystemStatusService,
  OfflineRunReportingService,
  OfflineLandscapeListingService
} from '../handlers/OfflineServices';

// Import concrete implementations
import { PersonLookup } from './PersonLookup';
import { PersonSync } from './PersonSync';
import { BulkSync } from './BulkSync';
import { SystemStatus } from './SystemStatus';
import { runReportingFromEnvironment } from './RunReports';
import { LandscapeRegistry, LOCAL_LANDSCAPE } from './LandscapeRegistry';
import { LaunchFlag } from './LaunchFlag';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DynamoDbRoleAssignmentStore, InMemoryRoleAssignmentStore, RoleAssignmentStore
} from '../authorization/RoleAssignmentStore';
import { PrincipalResolver } from '../authorization/Principal';
import { RoleCatalog, StaticRoleCatalog } from '../authorization/RoleCatalog';
import { AuditStore, DynamoDbAuditStore, InMemoryAuditStore } from '../audit/AuditStore';
import { Config } from 'integration-huron-person';
import { CrudOperation, TestEnvironment } from 'integration-core';
import { getConfig } from '../../../src/Utils';
import { ChangeType } from '../../../src/reporting/RunReporting';

/**
 * Service provider that provides the offline or the real implementations
 * based on environment configuration
 */
export class ServiceProvider {
  // Kept for the life of the Lambda instance: the registry caches what it reads from SSM
  private static runReporting?: RunReportingService;
  private static landscapeListing?: LandscapeListingService;
  /** The offline Bulk sync service, for the landscape it plays */
  private static offlineBulkSync?: { landscape: string; service: OfflineBulkSyncService };
  private static launchFlags = new Map<string, LaunchFlag>();
  /** Who holds which roles: the landscape's access table, or offline the local user's USER_ROLES */
  private static roleAssignments?: { key: string; store: RoleAssignmentStore };
  private static readonly roleCatalog = new StaticRoleCatalog();
  /** The audit trail: the landscape's audit table, or offline kept in memory */
  private static auditStore?: { key: string; store: AuditStore };
  
  /**
   * Whether to use the offline services (a local run with LOCAL_OFFLINE=true; never in Lambda). They ignore the
   * integration config, so callers that only read it to hand to a service can skip reading it when this is true.
   * LOCALHOST_WITH_MOCKS, its name before 2026-11-15, still works, with a warning.
   */
  static isOffline(): boolean {
    // In Lambda environment, always use concrete implementations
    if (process.env.AWS_LAMBDA_FUNCTION_NAME) {
      return false;
    }
    
    // In local environment, offline if explicitly requested
    if (process.env.LOCAL_OFFLINE === undefined && process.env.LOCALHOST_WITH_MOCKS !== undefined) {
      if (!ServiceProvider.renameWarned) {
        ServiceProvider.renameWarned = true;
        console.warn('Renamed: LOCALHOST_WITH_MOCKS is now LOCAL_OFFLINE');
      }
      return process.env.LOCALHOST_WITH_MOCKS === 'true';
    }
    return process.env.LOCAL_OFFLINE === 'true';
  }

  private static renameWarned = false;

  /**
   * Get person lookup service implementation
   */
  static getPersonLookupService(config:Config): PersonLookupService {
    if (this.isOffline()) {
      return new OfflinePersonLookupService();
    } else {
      return new PersonLookup(config);
    }
  }

  /**
   * Get person sync service implementation
   */
  static getPersonSyncService(config:Config): PersonSyncService {
    if (this.isOffline()) {
      return new OfflinePersonSyncService();
    } else {
      return new PersonSync(config);
    }
  }

  /**
   * Get bulk sync service implementation
   */
  static getBulkSyncService(config:Config): BulkSyncService {
    if (this.isOffline()) {
      // One per landscape played, so its state (the last mock landscape reset) lasts from request to request
      const landscape = process.env.LANDSCAPE || '';
      if (!this.offlineBulkSync || this.offlineBulkSync.landscape !== landscape) {
        this.offlineBulkSync = {
          landscape, service: new OfflineBulkSyncService({ runs: this.getRunReportingService(), launchFlag: this.getLaunchFlag() })
        };
      }
      return this.offlineBulkSync.service;
    } else {
      return BulkSync.fromEnvironment(config, this.getRunReportingService());
    }
  }

  /**
   * Forget the offline services' state (the last mock landscape reset, a launch flag): for tests, which share one
   * module instance, so that a run started in one doesn't hold back the next
   */
  static resetOfflineState(): void {
    this.offlineBulkSync = undefined;
    this.launchFlags.clear();
    this.roleAssignments = undefined;
    this.auditStore = undefined;
  }

  /**
   * The landscape's audit trail: its audit table (AUDIT_TABLE_NAME, in REGION). Offline, kept in memory for the life of
   * the server. Where the table isn't named, records are kept in memory and lost, with a warning.
   */
  static getAuditStore(): AuditStore {
    const offline = this.isOffline();
    const { AUDIT_TABLE_NAME, REGION } = process.env;
    const key = offline ? 'offline' : `table:${AUDIT_TABLE_NAME}:${REGION}`;
    if (this.auditStore?.key !== key) {
      let store: AuditStore;
      if (offline) {
        store = new InMemoryAuditStore();
      } else if (AUDIT_TABLE_NAME) {
        store = new DynamoDbAuditStore(AUDIT_TABLE_NAME, new DynamoDBClient(REGION ? { region: REGION } : {}));
      } else {
        console.warn('AUDIT_TABLE_NAME is not set: audit records are kept in memory only, and lost');
        store = new InMemoryAuditStore();
      }
      this.auditStore = { key, store };
    }
    return this.auditStore.store;
  }

  /** The roles, policies and actions (authorization-plan.md, decision 5) */
  static getRoleCatalog(): RoleCatalog {
    return this.roleCatalog;
  }

  /**
   * Who holds which roles in this landscape: its access table (ACCESS_TABLE_NAME, in REGION). Offline, kept in memory
   * for the life of the server, seeded with the local user's roles: USER_ROLES (comma-delimited) for USER_PRINCIPAL,
   * both passed by the local launcher. Where the table isn't named, no one holds a role.
   */
  static getRoleAssignmentStore(): RoleAssignmentStore {
    const offline = this.isOffline();
    const { ACCESS_TABLE_NAME, REGION, USER_PRINCIPAL = '', USER_ROLES = '' } = process.env;
    const key = offline ? `offline:${USER_PRINCIPAL}:${USER_ROLES}` : `table:${ACCESS_TABLE_NAME}:${REGION}`;
    if (this.roleAssignments?.key !== key) {
      this.roleAssignments = { key, store: this.newRoleAssignmentStore(offline) };
    }
    return this.roleAssignments.store;
  }

  private static newRoleAssignmentStore(offline: boolean): RoleAssignmentStore {
    const { ACCESS_TABLE_NAME, REGION, USER_PRINCIPAL, USER_ROLES } = process.env;
    if (offline) {
      const user = PrincipalResolver.resolve(USER_PRINCIPAL);
      const roles = (USER_ROLES ?? '').split(',').map(role => role.trim()).filter(Boolean);
      return new InMemoryRoleAssignmentStore(user.ok && roles.length ? { [user.buPrincipal]: roles } : {});
    }
    if (!ACCESS_TABLE_NAME) {
      console.warn('ACCESS_TABLE_NAME is not set: no one holds a role, so every API request of a signed-in user is denied');
      return new InMemoryRoleAssignmentStore();
    }
    return new DynamoDbRoleAssignmentStore(ACCESS_TABLE_NAME, new DynamoDBClient(REGION ? { region: REGION } : {}));
  }

  /**
   * The landscape's launch flag (a run launched moments ago from Bulk sync, not yet listed): its SSM parameter, or
   * offline one kept in memory, shared with the offline Bulk sync. Undefined where LANDSCAPE or REGION isn't set.
   */
  static getLaunchFlag(): LaunchFlag | undefined {
    const offline = this.isOffline();
    const landscape = offline ? (process.env.LANDSCAPE || LOCAL_LANDSCAPE) : process.env.LANDSCAPE;
    const region = process.env.REGION;
    if (landscape === undefined || (!offline && (!landscape || !region))) {
      return undefined;
    }
    const key = `${offline ? 'offline' : 'ssm'}:${landscape}`;
    if (!this.launchFlags.has(key)) {
      this.launchFlags.set(key, offline ? LaunchFlag.inMemory(landscape) : LaunchFlag.forLandscape({ landscape, region: region! }));
    }
    return this.launchFlags.get(key);
  }

  /**
   * Get system status service implementation
   */
  static getSystemStatusService(): SystemStatusService {
    if (this.isOffline()) {
      return new OfflineSystemStatusService();
    } else {
      return new SystemStatus();
    }
  }

  /**
   * Get run reporting service implementation (this landscape's run data)
   */
  static getRunReportingService(): RunReportingService {
    if (this.isOffline()) {
      return new OfflineRunReportingService();
    }
    return this.runReporting ??= runReportingFromEnvironment();
  }

  /**
   * Get landscape listing service implementation (the landscape picker's data)
   */
  static getLandscapeListingService(): LandscapeListingService {
    if (this.isOffline()) {
      return new OfflineLandscapeListingService();
    }
    return this.landscapeListing ??= LandscapeRegistry.fromEnvironment();
  }
}


/**
 * Harness: get a service from the provider and call it the way DashboardHandler does, without the
 * Lambda, CloudFront or authentication in front of it. Real services read the landscape named by
 * the environment (the same variables the dashboard stack gives the Lambda); set
 * LOCAL_OFFLINE=true for the offline services. Read only: person sync is previewed, never pushed.
 *
 * Tasks:
 *   capabilities | runs | summary | chunks | errors | changes  (run reporting; SYNC_RUN_ID for a run,
 *                                                                default: the latest run)
 *   landscapes                                                  (landscape registry in SSM)
 *   lookup | preview                                            (person lookup / sync preview by PERSON_ID,
 *                                                                with the config from getConfig(), unless offline)
 *   status                                                      (a stub: only the offline services answer)
 */
if (require.main === module) {
  const testEnvironment = TestEnvironment('DASHBOARD_SERVICE_PROVIDER');
  [
    'TASK',
    'LOCAL_OFFLINE',
    // LOCAL_OFFLINE's name before 2026-11-15, still read (with a warning)
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
    // The offline services ignore the config, so don't load it (no Secrets Manager or file access)
    const config = async (): Promise<Config> => ServiceProvider.isOffline() ? {} as Config : getConfig();

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
      case 'status':
        show('System status', await ServiceProvider.getSystemStatusService().getStatus());
        break;
      default:
        console.error(`Unknown task: ${task}. Supported tasks: capabilities, runs, summary, chunks, errors, changes, landscapes, lookup, preview, status`);
    }
  })().catch(error => {
    console.error('ServiceProvider harness failed:', error);
    process.exit(1);
  });
}
