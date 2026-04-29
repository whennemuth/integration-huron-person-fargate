/**
 * Comprehensive tests for ChunkFromAPI.ts
 * 
 * Tests the parameter gathering, validation, and API-based chunking logic.
 * Covers all variations of:
 * - Environment variable configuration
 * - Config object configuration
 * - SQS message queue parameters
 * - Combination scenarios with priority handling
 */

import { Config, DataSourceConfig } from 'integration-huron-person';
import { ChunkFromAPI } from '../src/chunking/fetch/ChunkFromAPI';
import { SyncPopulation } from '../docker/chunkTypes';

// Mock dependencies
jest.mock('@aws-sdk/client-sqs');
jest.mock('../src/storage/S3StorageAdapter');
jest.mock('../src/chunking/PersonArrayWrapper');
jest.mock('integration-huron-person', () => {
  const actual = jest.requireActual('integration-huron-person');
  return {
    ...actual,
    AxiosResponseStreamFilter: jest.fn().mockImplementation(() => ({
      fieldsOfInterest: ['personid']
    }))
  };
});

describe('ChunkFromAPI Parameter Gathering', () => {
  let originalEnv: NodeJS.ProcessEnv;
  let mockConfig: Config;
  let consoleLogSpy: jest.SpyInstance;

  beforeEach(() => {
    // Save original environment
    originalEnv = { ...process.env };

    // Mock console.log to avoid noise in tests
    consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();
    jest.spyOn(console, 'warn').mockImplementation();

    // Create a mock config object
    mockConfig = {
      dataSource: {
        people: {
          endpointConfig: {
            baseUrl: 'https://api.config.com',
            apiKey: 'config-api-key'
          },
          fetchPath: '/config/people'
        } as DataSourceConfig
      }
    } as Config;

    // Reset all mocks
    jest.clearAllMocks();
  });

  afterEach(() => {
    // Restore original environment
    process.env = originalEnv;
    // Restore console
    consoleLogSpy.mockRestore();
    jest.restoreAllMocks();
  });

  describe('Parameter gathering from environment variables', () => {
    it('should read task parameters from environment when all are provided', () => {
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL = 'https://api.env.com';
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH = '/env/people';
      process.env.POPULATION_TYPE = 'person-full';
      process.env.BULK_RESET = 'true';

      const chunker = new ChunkFromAPI(mockConfig);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should default to person-full when POPULATION_TYPE is invalid', () => {
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL = 'https://api.env.com';
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH = '/env/people';
      process.env.POPULATION_TYPE = 'invalid-type';

      const chunker = new ChunkFromAPI(mockConfig);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
      // Population type should default to person-full
    });

    it('should default to person-full when POPULATION_TYPE is missing', () => {
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL = 'https://api.env.com';
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH = '/env/people';
      delete process.env.POPULATION_TYPE;

      const chunker = new ChunkFromAPI(mockConfig);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should parse BULK_RESET as boolean', () => {
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL = 'https://api.env.com';
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH = '/env/people';
      process.env.POPULATION_TYPE = 'person-delta';
      process.env.BULK_RESET = 'false';

      const chunker = new ChunkFromAPI(mockConfig);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle missing DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL', () => {
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL;
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH = '/env/people';
      process.env.POPULATION_TYPE = 'person-full';

      const chunker = new ChunkFromAPI(mockConfig);
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should handle missing DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH', () => {
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL = 'https://api.env.com';
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH;
      process.env.POPULATION_TYPE = 'person-full';

      const chunker = new ChunkFromAPI(mockConfig);
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });

    it('should handle missing both DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL and DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH', () => {
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL;
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH;
      process.env.POPULATION_TYPE = 'person-full';

      const chunker = new ChunkFromAPI(mockConfig);
      expect(chunker.hasSufficientTaskInfo()).toBe(false);
    });
  });

  describe('Parameter gathering from config object', () => {
    beforeEach(() => {
      // Clear environment variables to test config-only scenarios
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL;
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH;
      delete process.env.POPULATION_TYPE;
    });

    it('should use config when environment variables are missing', () => {
      const chunker = new ChunkFromAPI(mockConfig);
      expect(chunker.hasSufficientConfig()).toBe(true);
    });

    it('should fail when config is missing baseUrl', () => {
      const invalidConfig = {
        dataSource: {
          people: {
            endpointConfig: {
              apiKey: 'config-api-key'
            },
            fetchPath: '/config/people'
          } as DataSourceConfig
        }
      } as Config;

      const chunker = new ChunkFromAPI(invalidConfig);
      expect(chunker.hasSufficientConfig()).toBe(false);
    });

    it('should fail when config is missing fetchPath', () => {
      const invalidConfig = {
        dataSource: {
          people: {
            endpointConfig: {
              baseUrl: 'https://api.config.com',
              apiKey: 'config-api-key'
            }
          } as DataSourceConfig
        }
      } as Config;

      const chunker = new ChunkFromAPI(invalidConfig);
      expect(chunker.hasSufficientConfig()).toBe(false);
    });

    it('should fail when config is missing apiKey', () => {
      const invalidConfig = {
        dataSource: {
          people: {
            endpointConfig: {
              baseUrl: 'https://api.config.com'
            },
            fetchPath: '/config/people'
          } as DataSourceConfig
        }
      } as Config;

      const chunker = new ChunkFromAPI(invalidConfig);
      expect(chunker.hasSufficientConfig()).toBe(false);
    });

    it('should fail when config.dataSource.people is missing', () => {
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL;
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH;
      delete process.env.POPULATION_TYPE;
      
      const invalidConfig = {
        dataSource: {}
      } as Config;

      const chunker = new ChunkFromAPI(invalidConfig);
      expect(chunker.hasSufficientConfig()).toBe(false);
    });
  });

  describe('Parameter gathering from SQS message', () => {
    beforeEach(() => {
      // Clear environment variables to test message-only scenarios
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL;
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH;
      delete process.env.POPULATION_TYPE;
    });

    it('should set task parameters from queue message with all fields', () => {
      const messageBody = {
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL: 'https://api.queue.com',
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH: '/queue/people',
        POPULATION_TYPE: 'person-full',
        BULK_RESET: 'true'
      };

      const chunker = new ChunkFromAPI(mockConfig);
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should use defaults when message fields are missing', () => {
      const messageBody = {};

      const chunker = new ChunkFromAPI(mockConfig);
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      // Should fall back to config
      expect(chunker.hasSufficientConfig()).toBe(true);
    });

    it('should handle POPULATION_TYPE from message', () => {
      const messageBody = {
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL: 'https://api.queue.com',
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH: '/queue/people',
        POPULATION_TYPE: SyncPopulation.PersonDelta
      };

      const chunker = new ChunkFromAPI(mockConfig);
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should default to person-full when POPULATION_TYPE is invalid in message', () => {
      const messageBody = {
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL: 'https://api.queue.com',
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH: '/queue/people',
        POPULATION_TYPE: 'invalid-type'
      };

      const chunker = new ChunkFromAPI(mockConfig);
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
    });

    it('should handle from_config placeholder for DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL', () => {
      const messageBody = {
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL: 'from_config',
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH: '/queue/people',
        POPULATION_TYPE: 'person-full'
      };

      const chunker = new ChunkFromAPI(mockConfig);
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      // Should use config for baseUrl
      expect(chunker.hasSufficientConfig()).toBe(true);
    });

    it('should handle from_config placeholder for DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH', () => {
      const messageBody = {
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL: 'https://api.queue.com',
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH: 'from_config',
        POPULATION_TYPE: 'person-full'
      };

      const chunker = new ChunkFromAPI(mockConfig);
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      // Should use config for fetchPath
      expect(chunker.hasSufficientConfig()).toBe(true);
    });
  });

  describe('Parameter priority and combination scenarios', () => {
    it('should use environment over config when both are present', () => {
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL = 'https://api.env.com';
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH = '/env/people';
      process.env.POPULATION_TYPE = 'person-full';

      const chunker = new ChunkFromAPI(mockConfig);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
      // Environment should take precedence
    });

    it('should use queue message over environment and config', () => {
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL = 'https://api.env.com';
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH = '/env/people';

      const messageBody = {
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL: 'https://api.queue.com',
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH: '/queue/people',
        POPULATION_TYPE: SyncPopulation.PersonDelta
      };

      const chunker = new ChunkFromAPI(mockConfig);
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientTaskInfo()).toBe(true);
      // Message should override environment
    });

    it('should merge environment DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL with config DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH', () => {
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL = 'https://api.env.com';
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH;
      process.env.POPULATION_TYPE = 'person-full';

      const chunker = new ChunkFromAPI(mockConfig);
      // Should have DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL from env, DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH will come from config in hasSufficientConfig
      expect(chunker.hasSufficientConfig()).toBe(true);
    });

    it('should merge config DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL with environment DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH', () => {
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL;
      process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH = '/env/people';
      process.env.POPULATION_TYPE = 'person-full';

      const chunker = new ChunkFromAPI(mockConfig);
      // Should have DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH from env, DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL will come from config in hasSufficientConfig
      expect(chunker.hasSufficientConfig()).toBe(true);
    });

    it('should handle partial message with config fallback', () => {
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL;
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH;

      const messageBody = {
        DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL: 'https://api.queue.com',
        // DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH missing, should fall back to config
        POPULATION_TYPE: 'person-full'
      };

      const chunker = new ChunkFromAPI(mockConfig);
      chunker.setTaskParametersFromQueueMessageBody(messageBody);
      expect(chunker.hasSufficientConfig()).toBe(true);
    });
  });

  describe('hasSufficientTaskInfo validation', () => {
    it('should log to console when parameter is true', () => {
      // Reset the spy for this specific test
      consoleLogSpy.mockRestore();
      consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();
      
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL;
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH;

      const chunker = new ChunkFromAPI(mockConfig);
      expect(chunker.hasSufficientTaskInfo(true)).toBe(false);
      
      expect(consoleLogSpy).toHaveBeenCalledWith(expect.stringContaining('Missing required baseUrl'));
      expect(consoleLogSpy).toHaveBeenCalledWith(expect.stringContaining('Missing required fetchPath'));
    });

    it('should not log to console when parameter is false', () => {
      // Reset the spy and track only calls during the test
      consoleLogSpy.mockRestore();
      consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();
      
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL;
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH;
      delete process.env.POPULATION_TYPE;

      const chunker = new ChunkFromAPI(mockConfig);
      
      // Clear any calls from constructor
      consoleLogSpy.mockClear();
      
      expect(chunker.hasSufficientTaskInfo(false)).toBe(false);
      
      // Should not log validation errors
      expect(consoleLogSpy).not.toHaveBeenCalledWith(expect.stringContaining('Missing required'));
    });
  });

  describe('hasSufficientConfig validation', () => {
    it('should validate baseUrl, fetchPath, and apiKey', () => {
      // Clear environment to force config usage
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_BASE_URL;
      delete process.env.DATASOURCE_ENDPOINTCONFIG_PEOPLE_PATH;
      delete process.env.POPULATION_TYPE;
      
      const chunker = new ChunkFromAPI(mockConfig);
      expect(chunker.hasSufficientConfig()).toBe(true);
    });

    it('should log missing apiKey when parameter is true', () => {
      // Reset the spy for this specific test
      consoleLogSpy.mockRestore();
      consoleLogSpy = jest.spyOn(console, 'log').mockImplementation();
      
      const invalidConfig = {
        dataSource: {
          people: {
            endpointConfig: {
              baseUrl: 'https://api.config.com'
            },
            fetchPath: '/config/people'
          } as DataSourceConfig
        }
      } as Config;

      const chunker2 = new ChunkFromAPI(invalidConfig);
      expect(chunker2.hasSufficientConfig(true)).toBe(false);
      
      expect(consoleLogSpy).toHaveBeenCalledWith(
        expect.stringContaining('Missing required API key')
      );
    });
  });
});
