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
  PersonLookupParams
} from '../services/ServiceTypes';
import { CrudOperation } from 'integration-core';
import { PersonPushRequest } from 'integration-huron-person';

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
