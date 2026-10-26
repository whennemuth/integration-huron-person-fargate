import { CrudOperation } from 'integration-core';
import { PersonPushRequest } from "integration-huron-person";

// Service Interfaces
export type PersonLookupParams = {
  system: 'source' | 'target' | 'source-only' | 'target-only';
  searchType: 'buid' | 'name' | 'email' | 'hrn' | 'sid' | 'uid';
  personId: string;
  firstName?: string;
  lastName?: string;
};
export interface PersonLookupService {
  lookup(params: PersonLookupParams): Promise<PersonLookupResult>;
}

export interface PersonSyncService {
  previewExperiment(personId: string, rawData: any[], operation?: CrudOperation): Promise<PersonPushRequest>;
  preview(personId: string, operation?: CrudOperation): Promise<PersonPushRequest>;
  sync(personId: string, operation: CrudOperation, hrn?: string): Promise<PersonSyncResult>;
}

export interface BulkSyncService {
  start(operation: string, batchSize?: number, filters?: any): Promise<BulkSyncResult>;
}

export interface HistoryService {
  getHistory(startDate?: string, endDate?: string, limit?: number): Promise<HistoryResult>;
}

export interface SystemStatusService {
  getStatus(): Promise<SystemStatusResult>;
}

// Service Result Types
export interface PersonLookupResult {
  personId: string;
  sourceData: [{
    buId: string;
    name: string;
    email: string;
    department: string;
    lastModified: string;
  }];
  targetData: [{
    hrn: string;
    status: string;
    lastSync: string;
  }];
}

export interface PersonSyncResult {
  personId: string;
  operation: string;
  status: string;
  timestamp: string;
  recordsProcessed: number;
  details: string;
}

export interface BulkSyncResult {
  operation: string;
  status: string;
  batchSize: number;
  filters: any;
  estimatedRecords: number;
  startTime: string;
  jobId: string;
}

export interface HistoryResult {
  history: HistoryRecord[];
  totalCount: number;
}

export interface HistoryRecord {
  id: string;
  timestamp: string;
  operation: string;
  status: string;
  recordsProcessed: number;
  duration: string;
  errors: number;
}

export interface SystemStatusResult {
  timestamp: string;
  overall: string;
  services: {
    sourceApi: ServiceHealthCheck;
    targetApi: ServiceHealthCheck;
    database: ServiceHealthCheck;
    s3Storage: ServiceHealthCheck;
  };
  metrics: {
    dailySyncs: number;
    weeklyErrors: number;
    avgProcessingTime: string;
  };
}

export interface ServiceHealthCheck {
  status: string;
  responseTime: number;
  lastCheck: string;
}