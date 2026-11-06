import { CrudOperation } from 'integration-core';
import { PersonPushRequest } from "integration-huron-person";
import { StatisticsItem } from '../../../src/ApiErrorTracking';
import { PersonHistoryRecord } from '../../../src/dynamodb/PersonHistoryTable';
import { Capabilities, ChangeType, Page, RunReport } from '../../../src/reporting/RunReporting';

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

export interface SystemStatusService {
  getStatus(): Promise<SystemStatusResult>;
}

/**
 * Reporting on this landscape's sync runs (fargate's src/reporting/RunReporting.ts, which
 * implements it). Page cursors are opaque; limits and cursors are validated by the implementation,
 * which throws a ReportingError with a code the routes map to a 400.
 */
export interface RunReportingService {
  getCapabilities(): Capabilities;
  listRuns(): Promise<string[]>;
  getRunReport(syncRunId: string): Promise<RunReport | undefined>;
  getRunChunkStatistics(syncRunId: string): Promise<StatisticsItem[]>;
  getRunErrors(syncRunId: string, page?: { statusCode?: number; limit?: number; cursor?: string }): Promise<Page<Record<string, any>>>;
  getRunChanges(syncRunId: string, changeType: ChangeType, page?: { limit?: number; cursor?: string }): Promise<Page<PersonHistoryRecord>>;
}

/**
 * The landscapes whose dashboards are registered in SSM (see LandscapeRegistry.ts), for a
 * landscape picker.
 */
export interface LandscapeListingService {
  getLandscapes(): Promise<LandscapeListing>;
}

export type LandscapeListing = {
  /** This dashboard's landscape ('local' when run locally) */
  current: string;
  landscapes: PickerLandscape[];
};

export type PickerLandscape = {
  landscape: string;
  label: string;
  url?: string;
  isDefault: boolean;
  /** True for this dashboard's own landscape */
  current: boolean;
};

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