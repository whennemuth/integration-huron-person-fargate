// Mock implementations for local development
import {
  PersonLookupService,
  PersonSyncService,
  BulkSyncService,
  HistoryService,
  SystemStatusService,
  PersonLookupResult,
  PersonSyncResult,
  BulkSyncResult,
  HistoryResult,
  SystemStatusResult,
  PersonLookupParams,
  RunReportingService,
  LandscapeListingService,
  LandscapeListing
} from '../services/ServiceTypes';
import { CrudOperation } from 'integration-core';
import { PersonPushRequest } from 'integration-huron-person';
import { Capabilities, ChangeType, Page, RunReport } from '../../../src/reporting/RunReporting';
import { RunSummary } from '../../../src/dynamodb/StatisticsTable';
import { StatisticsItem } from '../../../src/ApiErrorTracking';
import { PersonHistoryRecord } from '../../../src/dynamodb/PersonHistoryTable';
import { LOCAL_LANDSCAPE } from '../services/LandscapeRegistry';

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

export class MockBulkSyncService implements BulkSyncService {
  async start(operation: string, batchSize?: number, filters?: any): Promise<BulkSyncResult> {
    return {
      operation,
      status: 'started',
      batchSize: batchSize || 100,
      filters: filters || {},
      estimatedRecords: 1500,
      startTime: new Date().toISOString(),
      jobId: `bulk-${Date.now()}`
    };
  }
}

export class MockHistoryService implements HistoryService {
  async getHistory(startDate?: string, endDate?: string, limit: number = 50): Promise<HistoryResult> {
    const mockHistory = [
      {
        id: '1',
        timestamp: new Date().toISOString(),
        operation: 'bulk-sync',
        status: 'completed',
        recordsProcessed: 1200,
        duration: '00:05:30',
        errors: 0
      },
      {
        id: '2',
        timestamp: new Date(Date.now() - 60 * 60 * 1000).toISOString(),
        operation: 'individual-sync',
        status: 'completed',
        recordsProcessed: 1,
        duration: '00:00:15',
        errors: 0
      }
    ];

    return {
      history: mockHistory,
      totalCount: mockHistory.length
    };
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

const MOCK_RUN_IDS = ['2026-10-01T02:00:00.000Z', '2026-10-02T02:00:00.000Z'];

export class MockRunReportingService implements RunReportingService {
  getCapabilities(): Capabilities {
    return { landscape: LOCAL_LANDSCAPE, storageMode: 'dynamodb', changesSupported: true };
  }

  async listRuns(): Promise<string[]> {
    return [...MOCK_RUN_IDS];
  }

  async getRunReport(syncRunId: string): Promise<RunReport | undefined> {
    if (!MOCK_RUN_IDS.includes(syncRunId)) {
      return undefined;
    }
    const summary: RunSummary = {
      syncRunId,
      status: 'SUCCEEDED',
      storageMode: 'dynamodb',
      dryRun: false,
      createdAt: syncRunId,
      completedAt: new Date(Date.parse(syncRunId) + 20 * 60 * 1000).toISOString(),
      syncPopulation: 'person-full',
      bulkReset: false,
      chunkCount: 2,
      totalRecords: 1500,
      chunksReported: 2,
      results: { processed: 1500, succeeded: 1497, failed: 3, skipped: 0, added: 12, updated: 40, removed: 0 },
      totalErrors: 3,
      throttleCount: 0,
      errorsByStatus: { '500': 3 },
      deletion: { status: 'COMPLETED', deletedCount: 2, failedCount: 0, totalProcessed: 2, message: 'Mock deletions' }
    };
    return { syncRunId, status: summary.status, summary };
  }

  async getRunChunkStatistics(syncRunId: string): Promise<StatisticsItem[]> {
    return [0, 1].map((chunk): StatisticsItem => ({
      integrationTimestamp: syncRunId,
      eventType: `STATISTICS-chunk-000${chunk}`,
      errorType: 'STATISTICS',
      chunkId: `000${chunk}`,
      startTimestamp: syncRunId,
      endTimestamp: syncRunId,
      chunkCount: 2,
      chunkSize: 750,
      totalRecords: 750,
      sourceDescription: 'Mock source',
      successCount: chunk === 0 ? 750 : 747,
      failureCount: chunk === 0 ? 0 : 3,
      skippedCount: 0,
      totalErrors: chunk === 0 ? 0 : 3,
      throttleCount: 0,
      errorsByStatus: chunk === 0 ? {} : { 500: 3 } as Record<number, number>
    }));
  }

  async getRunErrors(syncRunId: string, page: { statusCode?: number } = {}): Promise<Page<Record<string, any>>> {
    const errors = MOCK_RUN_IDS.includes(syncRunId) ? [1, 2, 3].map(n => ({
      integrationTimestamp: syncRunId,
      eventType: `ERROR:500:${syncRunId}-${n}`,
      statusCode: 500,
      message: 'Mock target error'
    })) : [];
    return { items: errors.filter(e => page.statusCode === undefined || e.statusCode === page.statusCode) };
  }

  async getRunChanges(syncRunId: string, changeType: ChangeType): Promise<Page<PersonHistoryRecord>> {
    const items: PersonHistoryRecord[] = MOCK_RUN_IDS.includes(syncRunId)
      ? [{ personId: 'U00000001', syncRunId, hash: 'mock-hash', changeType }]
      : [];
    return { items };
  }
}

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

export class MockBulkSync {
  static start(operation: string, batchSize?: number, filters?: any) {
    const service = new MockBulkSyncService();
    return service.start(operation, batchSize, filters);
  }
}

export class MockHistory {
  static getHistory(startDate?: string, endDate?: string, limit: number = 50) {
    const service = new MockHistoryService();
    return service.getHistory(startDate, endDate, limit);
  }
}

export class MockSystemStatus {
  static getStatus() {
    const service = new MockSystemStatusService();
    return service.getStatus();
  }
}
