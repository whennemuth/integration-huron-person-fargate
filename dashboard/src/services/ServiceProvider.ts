import {
  PersonLookupService,
  PersonSyncService,
  BulkSyncService,
  HistoryService,
  SystemStatusService,
  RunReportingService,
  LandscapeListingService
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

/**
 * Service provider that provides mock or concrete implementations
 * based on environment configuration
 */
export class ServiceProvider {
  // Kept for the life of the Lambda instance: the registry caches what it reads from SSM
  private static runReporting?: RunReportingService;
  private static landscapeListing?: LandscapeListingService;
  
  /**
   * Determine if we should use mock implementations
   */
  private static shouldUseMocks(): boolean {
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
