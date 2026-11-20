// The offline services: a local run's stand-ins for the dashboard's services, answering from built-in data with no
// AWS access (LOCAL_OFFLINE=true, the launcher's OFFLINE switch). Not to be confused with a mock landscape, whose
// pipeline's target is mocked: --landscape mock plays one.
import { LaunchFlag } from '../services/LaunchFlag';
import type { RequestAuthorizer } from '../authorization/Authorizer';
import {
  PersonLookupService,
  PersonSyncService,
  BulkSyncService,
  SystemStatusService,
  PersonLookupResult,
  PersonSyncResult,
  BulkSyncPlan,
  BulkSyncResult,
  BulkSyncRequestBody,
  BulkSyncState,
  SystemStatusResult,
  PersonLookupParams,
  LandscapeListingService,
  LandscapeListing,
  RunReportingService
} from '../services/ServiceTypes';
import { CrudOperation } from 'integration-core';
import { Config, PersonPushRequest } from 'integration-huron-person';
import type { ApiChunkerEvent } from '../../../src/chunking/ChunkerSubscriber';
import { BulkSync, BulkSyncSettings, SourceSimulatorService } from '../services/BulkSync';
import type { MockLandscapeResetRecord } from '../../../src/chunking/MockLandscapeReset';
import { MockLandscapeResetRequested, MockLandscapeResetState } from '../services/ServiceTypes';
import { isMockLandscape } from '../../../src/Utils';
import type { StartedBy } from '../../../src/chunking/StartedBy';
import type { UpcomingStart } from '../../../src/schedule/ScheduledRun';
import { LOCAL_LANDSCAPE } from '../services/LandscapeRegistry';
import { OfflineRunReportingService } from './OfflineRunReporting';

export class OfflinePersonLookupService implements PersonLookupService {
  async lookup(params: PersonLookupParams): Promise<PersonLookupResult> {
    const { personId='', system } = params;
    const buId = system === 'source' ? personId : 'U1234567';
    const hrn = system === 'target' ? personId : 'HRN123456';
    return {
      personId,
      sourceData: [{
        buId,
        name: 'John Doe',
        email: 'john.doe@example.com',
        department: 'Engineering',
        lastModified: new Date().toISOString()
      }],
      targetData: [{
        hrn,
        status: 'Active',
        lastSync: new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString()
      }]
    };
  }
}

export class OfflinePersonSyncService implements PersonSyncService {
  async preview(personId: string, operation: CrudOperation): Promise<PersonPushRequest> {
    return {
      operation: operation as 'create' | 'update' | 'delete',
      data: {
        personId,
        fieldSets: [
          {
            fieldValues: [
              { id: personId },
              { firstName: 'Sample' },
              { lastName: 'Person' },
              { email: 'sample@example.com' }
            ]
          }
        ]
      }
    };
  }

  async previewExperiment(personId: string, rawData: any[], operation?: CrudOperation): Promise<PersonPushRequest> {
    // Same as preview(), but without operation field
    // Uses provided rawData instead of looking it up
    const samplePersonId = personId || rawData[0]?.personid || 'U1234567';
    return {
      data: {
        personId: samplePersonId,
        fieldSets: [
          {
            fieldValues: [
              { id: samplePersonId },
              { firstName: rawData[0]?.personBasic?.names?.[0]?.firstName || 'Sample' },
              { lastName: rawData[0]?.personBasic?.names?.[0]?.lastName || 'Person' },
              { email: rawData[0]?.personContact?.emails?.[0]?.emailAddress || 'sample@example.com' }
            ]
          }
        ]
      }
    } as PersonPushRequest;
  }

  async sync(personId: string, operation: CrudOperation): Promise<PersonSyncResult> {
    return {
      personId,
      operation,
      status: 'success',
      timestamp: new Date().toISOString(),
      recordsProcessed: 1,
      details: `Successfully ${operation}d person ${personId}`
    };
  }
}

/** The source endpoints an offline start reads from the (offline) integration config */
export const OFFLINE_SOURCE_CONFIG = {
  dataSource: {
    people: { endpointConfig: { baseUrl: 'https://cdm.example.bu.edu' }, fetchPath: '/api/people' },
    person: { endpointConfig: { baseUrl: 'https://cdm.example.bu.edu' }, fetchPath: '/api/person' }
  }
} as unknown as Config;

/**
 * The real BulkSync over the offline run history, with an in-memory stand-in for the chunker Lambda:
 * the request is validated and the guards are checked as in a landscape, and a start records the chunker event it
 * would have sent (invoked) instead of starting anything.
 *
 * Its landscape is the one the offline services play (LANDSCAPE, which the launcher sets: `local`, or --landscape's, as
 * `npm run dashboard:dev:offline:mock` sets `mock`). A mock landscape's name shows Bulk sync's mock landscape options, with a
 * stand-in source simulator (STAND_IN_SIMULATOR) at an example URL with mock's deployed settings.
 */
export class OfflineBulkSyncService implements BulkSyncService {
  /** The offline chunker service's most tasks, which bounds Parallel start */
  static readonly CHUNKER_MAX_TASKS = 15;
  /** A mock landscape's stand-in source simulator: found, at an example Function URL, with mock's deployed settings */
  static readonly STAND_IN_SIMULATOR: SourceSimulatorService = {
    lookup: async () => ({ url: 'https://example-simulator.lambda-url.us-east-2.on.aws/' }),
    settings: async () => ({ totalPopulation: 1950, simulatedDelaySeconds: 3, errorRate: 0 })
  };
  /** The chunker events this instance's starts would have sent, oldest first */
  readonly invoked: (ApiChunkerEvent | { mockLandscapeReset: true })[] = [];
  /**
   * A mock landscape's last reset, as the chunker subscriber would have recorded it: one from Overview that emptied
   * an old hybrid run's tables; a reset requested here replaces it with one that found them empty
   */
  private resetRecord: MockLandscapeResetRecord = {
    status: 'succeeded', via: 'overview', startedAt: '2026-11-04T08:00:00.000Z', finishedAt: '2026-11-04T08:01:42.000Z',
    durationMs: 102_000, startedBy: { by: 'dashboard', principal: 'jdoe' } as any, storageMode: 'dynamodb',
    tables: [
      { table: 'mock_target_person' as any, tableName: 'huron-person-fargate-processor-mock-target-person-mock', itemsDeleted: 146554, durationMs: 101_000 },
      { table: 'person_current_state' as any, tableName: 'huron-person-fargate-processor-person-current-state-mock', itemsDeleted: 145793, durationMs: 97_000 },
      { table: 'person_history' as any, tableName: 'huron-person-fargate-processor-person-history-mock', itemsDeleted: 145795, durationMs: 98_000 },
      { table: 'statistics' as any, tableName: 'huron-person-fargate-processor-statistics-mock', itemsDeleted: 1489, durationMs: 4_000 }
    ]
  };
  private readonly service: BulkSync;
  readonly launchFlag: LaunchFlag;

  constructor(params: {
    runs?: Pick<RunReportingService, 'listRunPage' | 'getRunReport'>;
    settings?: Partial<BulkSyncSettings>;
    dryRun?: string[];
    /** A mock landscape's source simulator (default: STAND_IN_SIMULATOR in a mock landscape, none elsewhere) */
    simulator?: SourceSimulatorService | null;
    /** The launch flag, kept in memory (default: one of its own) */
    launchFlag?: LaunchFlag;
    /** The bulk sync schedules starting within the hour (default: none asked for, so no warning) */
    upcomingSchedules?: () => Promise<UpcomingStart[]>;
    now?: () => number;
  } = {}) {
    const { runs = new OfflineRunReportingService(), settings = {}, dryRun = [], now } = params;
    const landscape = settings.landscape ?? (process.env.LANDSCAPE || LOCAL_LANDSCAPE);
    this.launchFlag = params.launchFlag ?? LaunchFlag.inMemory(landscape, now);
    const simulator = params.simulator === undefined
      ? (isMockLandscape(landscape) ? OfflineBulkSyncService.STAND_IN_SIMULATOR : undefined)
      : params.simulator ?? undefined;
    this.service = new BulkSync({
      config: OFFLINE_SOURCE_CONFIG,
      settings: {
        landscape,
        chunkerFunctionName: `chunker-subscriber-${landscape}`,
        processorSubscriberFunctionName: `processor-subscriber-${landscape}`,
        region: 'us-east-2',
        stackId: 'huron-person',
        secretName: `huron-person-integration-config-${landscape}`,
        itemsPerChunk: 200,
        processorMaxScalingCapacity: 3,
        chunkerClusterName: `huron-person-cluster-${landscape}`,
        chunkerServiceName: 'Chunker',
        ...settings
      },
      runs,
      functions: {
        isDryRun: async functionName => dryRun.includes(functionName),
        invokeAsync: async (_functionName, event) => {
          this.invoked.push(event as any);
          if ((event as any).mockLandscapeReset) {
            // The tables are empty offline: the reset finds nothing to delete, at once, so it starts and ends when it was
            // requested (one clock reading: two could differ by a millisecond under load)
            const at = (event as any).requestedAt ?? new Date((now ?? Date.now)()).toISOString();
            this.resetRecord = {
              status: 'succeeded', via: 'overview', startedAt: at, finishedAt: at, durationMs: 0, startedBy: (event as any).startedBy, storageMode: 'dynamodb',
              tables: (this.resetRecord.tables ?? []).map(table => ({ ...table, itemsDeleted: 0, durationMs: 0 }))
            };
          }
        }
      },
      // As preview's chunker service: at most 15 tasks
      chunkerService: { maxTasks: async () => OfflineBulkSyncService.CHUNKER_MAX_TASKS },
      ...(simulator ? { simulator } : {}),
      resetRecords: { read: async () => this.resetRecord },
      // Offline no run ever appears, so a launch shows until its flag expires
      launchFlag: this.launchFlag,
      ...(params.upcomingSchedules ? { upcomingSchedules: params.upcomingSchedules } : {}),
      now
    });
  }

  /** The real BulkSync underneath, which the offline Bulk sync schedule builds its runs with */
  get bulkSync(): BulkSync {
    return this.service;
  }

  describe(): Promise<BulkSyncState> {
    return this.service.describe();
  }

  plan(request: BulkSyncRequestBody, startedBy?: StartedBy, authorizer?: RequestAuthorizer): Promise<BulkSyncPlan> {
    return this.service.plan(request, startedBy, authorizer);
  }

  start(request: BulkSyncRequestBody, startedBy?: StartedBy, authorizer?: RequestAuthorizer): Promise<BulkSyncResult> {
    return this.service.start(request, startedBy, authorizer);
  }

  resetState(): Promise<MockLandscapeResetState> {
    return this.service.resetState();
  }

  requestReset(startedBy?: StartedBy): Promise<MockLandscapeResetRequested> {
    return this.service.requestReset(startedBy);
  }
}

export class OfflineSystemStatusService implements SystemStatusService {
  async getStatus(): Promise<SystemStatusResult> {
    return {
      timestamp: new Date().toISOString(),
      overall: 'healthy',
      services: {
        sourceApi: { status: 'healthy', responseTime: 120, lastCheck: new Date().toISOString() },
        targetApi: { status: 'healthy', responseTime: 85, lastCheck: new Date().toISOString() },
        database: { status: 'healthy', responseTime: 15, lastCheck: new Date().toISOString() },
        s3Storage: { status: 'healthy', responseTime: 45, lastCheck: new Date().toISOString() }
      },
      metrics: {
        dailySyncs: 45,
        weeklyErrors: 2,
        avgProcessingTime: '00:00:08'
      }
    };
  }
}

// Run reporting: a deterministic history of runs in every state (see OfflineRunReporting.ts)
export { OfflineRunReportingService } from './OfflineRunReporting';

export class OfflineLandscapeListingService implements LandscapeListingService {
  async getLandscapes(): Promise<LandscapeListing> {
    // The landscape the offline services play (LANDSCAPE: local, or the launcher's --landscape, e.g. mock) is the current one
    const current = process.env.LANDSCAPE || LOCAL_LANDSCAPE;
    return {
      current,
      landscapes: [
        { landscape: 'mock', label: 'mock', url: 'https://mock.example.com', isDefault: false },
        { landscape: 'staging', label: 'Staging', url: 'https://staging.example.com', isDefault: true }
      ].map(entry => ({ ...entry, current: entry.landscape === current }))
    };
  }
}
