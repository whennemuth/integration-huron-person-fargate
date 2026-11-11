// Mock implementations for local development
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
import { BulkSync, BulkSyncSettings } from '../services/BulkSync';
import { LOCAL_LANDSCAPE } from '../services/LandscapeRegistry';
import { MockRunReportingService } from './MockRunReporting';

export class MockPersonLookupService implements PersonLookupService {
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

export class MockPersonSyncService implements PersonSyncService {
  async preview(personId: string, operation: CrudOperation): Promise<PersonPushRequest> {
    return {
      operation: operation as 'create' | 'update' | 'delete',
      data: {
        personId,
        fieldSets: [
          {
            fieldValues: [
              { id: personId },
              { firstName: 'Mock' },
              { lastName: 'Person' },
              { email: 'mock@example.com' }
            ]
          }
        ]
      }
    };
  }

  async previewExperiment(personId: string, rawData: any[], operation?: CrudOperation): Promise<PersonPushRequest> {
    // Same as preview(), but without operation field
    // Uses provided rawData instead of looking it up
    const mockPersonId = personId || rawData[0]?.personid || 'U1234567';
    return {
      data: {
        personId: mockPersonId,
        fieldSets: [
          {
            fieldValues: [
              { id: mockPersonId },
              { firstName: rawData[0]?.personBasic?.names?.[0]?.firstName || 'Mock' },
              { lastName: rawData[0]?.personBasic?.names?.[0]?.lastName || 'Person' },
              { email: rawData[0]?.personContact?.emails?.[0]?.emailAddress || 'mock@example.com' }
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

/** The source endpoints a mock start reads from the (mock) integration config */
export const MOCK_SOURCE_CONFIG = {
  dataSource: {
    people: { endpointConfig: { baseUrl: 'https://cdm.example.bu.edu' }, fetchPath: '/api/people' },
    person: { endpointConfig: { baseUrl: 'https://cdm.example.bu.edu' }, fetchPath: '/api/person' }
  }
} as unknown as Config;

/**
 * The real BulkSync over the mock run history, with an in-memory stand-in for the chunker Lambda:
 * the request is validated and the guards are checked as in a landscape, and a start records the chunker event it
 * would have sent (invoked) instead of starting anything.
 */
export class MockBulkSyncService implements BulkSyncService {
  /** The chunker events this instance's starts would have sent, oldest first */
  readonly invoked: ApiChunkerEvent[] = [];
  private readonly service: BulkSync;

  constructor(params: {
    runs?: Pick<RunReportingService, 'listRunPage' | 'getRunReport'>;
    settings?: Partial<BulkSyncSettings>;
    dryRun?: string[];
    now?: () => number;
  } = {}) {
    const { runs = new MockRunReportingService(), settings = {}, dryRun = [], now } = params;
    this.service = new BulkSync({
      config: MOCK_SOURCE_CONFIG,
      settings: {
        landscape: LOCAL_LANDSCAPE,
        chunkerFunctionName: `chunker-subscriber-${LOCAL_LANDSCAPE}`,
        processorSubscriberFunctionName: `processor-subscriber-${LOCAL_LANDSCAPE}`,
        region: 'us-east-2',
        stackId: 'huron-person',
        secretName: `huron-person-integration-config-${LOCAL_LANDSCAPE}`,
        itemsPerChunk: 200,
        ...settings
      },
      runs,
      functions: {
        isDryRun: async functionName => dryRun.includes(functionName),
        invokeAsync: async (_functionName, event) => { this.invoked.push(event); }
      },
      now
    });
  }

  describe(): Promise<BulkSyncState> {
    return this.service.describe();
  }

  plan(request: BulkSyncRequestBody): Promise<BulkSyncPlan> {
    return this.service.plan(request);
  }

  start(request: BulkSyncRequestBody): Promise<BulkSyncResult> {
    return this.service.start(request);
  }
}

export class MockSystemStatusService implements SystemStatusService {
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

// Run reporting: a deterministic history of runs in every state (see MockRunReporting.ts)
export { MockRunReportingService } from './MockRunReporting';

export class MockLandscapeListingService implements LandscapeListingService {
  async getLandscapes(): Promise<LandscapeListing> {
    return {
      current: LOCAL_LANDSCAPE,
      landscapes: [
        { landscape: 'mock', label: 'mock', url: 'https://mock.example.com', isDefault: false, current: false },
        { landscape: 'staging', label: 'Staging', url: 'https://staging.example.com', isDefault: true, current: false }
      ]
    };
  }
}

// Legacy exports for backward compatibility (can be removed later)
export class MockSinglePersonSync {
  static async sync(personId: string, operation: string) {
    const service = new MockPersonSyncService();
    return await service.sync(personId, CrudOperation.CREATE);
  }
}

export class MockConfigManager {
  static getConfig() {
    return {
      sourceApi: { url: 'https://api.example.com' },
      targetApi: { url: 'https://target.example.com' }
    };
  }
}

export class MockPersonLookup {
  static lookup(personId: string, system: 'source' | 'target') {
    const service = new MockPersonLookupService();
    return service.lookup({ personId, system } as PersonLookupParams);
  }
}

export class MockSystemStatus {
  static getStatus() {
    const service = new MockSystemStatusService();
    return service.getStatus();
  }
}
