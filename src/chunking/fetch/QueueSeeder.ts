
import { TestEnvironment } from 'integration-core';
import { handleApiEvent } from './ChunkerApiSubscriber';
import { ApiChunkerEvent } from '../ChunkerSubscriber';
import { SyncPopulation } from '../../../docker/chunkTypes';
import { extractChunkDirectory } from '../filedrop/ChunkPathUtils';
import { AbstractAtomicCounter } from '../../AtomicCounter';
import { CHUNKER_COUNTER_NAME } from '../ChunkerQueue';

export type QueueSeederParams = {
  stackId?: string, 
  region?: string
  baseUrl: string;
  fetchPath: string;
  populationType: SyncPopulation;
  bulkReset?: boolean;
  trustPreviousStorage?: boolean;
  limit: number;
  messagesToSeed: number;
  queueUrl: string;
  dryRun?: boolean;
};

/**
 * QueueSeeder: Pre-populate SQS queue with initial messages to enable "hitting the ground running"
 * 
 * Purpose:
 * When starting the chunker service, instead of creating messages one-at-a-time through the
 * "one-in/one-out" pattern, this seeder pre-populates the queue with N messages matching
 * the maxScalingCapacity. This allows you to manually set desiredCount to a high value
 * (e.g., 15) and have all tasks start immediately with work to do.
 * 
 * Pattern:
 * - Seeds N messages using atomic counter for offset generation
 * - Each seeded task creates the next message using the atomic counter
 * - Atomic counter prevents offset collisions between seeded and subsequent messages
 * 
 * Example:
 * - messagesToSeed: 15 (matching maxScalingCapacity)
 * - limit: 100 (process 100 records per task)
 * - Result: 15 messages with offsets [0, 100, 200, 300, ..., 1400]
 * - All tasks create next messages; atomic counter ensures offsets [1500, 1600, ...]
 * 
 * Usage (test harness or manual trigger):
 * ```typescript
 * const seeder = new QueueSeeder();
 * await seeder.seedQueue({
 *   baseUrl: 'https://api.bu.edu',
 *   fetchPath: '/people/v1',
 *   populationType: SyncPopulation.PersonFull,
 *   limit: 100,
 *   messagesToSeed: 15,
 *   queueUrl: 'https://sqs.us-east-1.amazonaws.com/123456789/chunker-queue'
 * });
 * ```
 */
export class QueueSeeder {
  private atomicCounter?: AbstractAtomicCounter;
  private chunkDirectory?: string;

  constructor(private params: QueueSeederParams) {
    const { REGION, STACK_ID } = process.env;
    const { stackId=STACK_ID, region=REGION } = params || {};
    if (stackId && region) {
      console.log('Atomic counter detected in QueueSeeder constructor');
      this.atomicCounter = new class extends AbstractAtomicCounter {
        getCounterName(): string {
          return CHUNKER_COUNTER_NAME;
        }
      }(stackId, region);
    } else {
      throw new Error('No stackId or region provided to QueueSeeder constructor - cannot instantiate atomic counter');
    }
  }

  public resetAtomicCounter = async (): Promise<void> => {
    if (!this.atomicCounter) {
      throw new Error('Atomic counter not initialized - cannot reset');
    }
    await this.atomicCounter.reset();
  }

  private getNextOffset = async (currentOffset: number, limit: number): Promise<number> => {
    const { atomicCounter } = this;
    if(atomicCounter) {
      return await atomicCounter.increment(limit);
    }
    else {
      return currentOffset + limit;
    }
  }

  /**
   * Generate a single chunkDirectory for all seeded messages using the same pattern as ChunkFromAPI
   * @returns 
   */
  public getChunkDirectory = (): string => {
    if(this.chunkDirectory) {
      return this.chunkDirectory;
    }

    if(process.env.CHUNK_DIRECTORY) {
      console.log('Using CHUNK_DIRECTORY from environment variable. This indicates testing ' +
        '(i.e: The test harness) - the chunkDirectory would never be obtained in this way ' +
        'through the standard deployment/implementation.');
      this.chunkDirectory = process.env.CHUNK_DIRECTORY;
      return this.chunkDirectory;
    }

    // 1. Create synthetic input key (mimics what ChunkFromAPI.getSyntheticInputKey() does)
    const timestamp = new Date().toISOString();
    const { populationType } = this.params;
    const syntheticInputKey = `${populationType}/${timestamp}.json`;
    
    // 2. Extract chunk directory using shared utility (ensures consistency with Phase 1 and Phase 3)
    this.chunkDirectory = extractChunkDirectory(syntheticInputKey);
    return this.chunkDirectory;
  }

  /**
   * Seed the SQS queue with initial messages for parallel chunking.
   * 
   * @param params Configuration for seeding the queue
   * @returns Promise resolving to number of successfully sent messages
   */
  public async seedQueue(): Promise<number> {
    const {
      stackId = process.env.STACK_ID,
      region = process.env.REGION,
      baseUrl,
      fetchPath,
      populationType,
      bulkReset = false,
      trustPreviousStorage = false,
      limit,
      messagesToSeed,
      queueUrl,
    } = this.params;

    // Validation
    if (limit <= 0) {
      throw new Error('limit must be greater than 0 for parallel chunking');
    }
    if (messagesToSeed <= 0) {
      throw new Error('messagesToSeed must be greater than 0');
    }

    const chunkDirectory = this.getChunkDirectory();

    console.log(`🌱 Seeding queue with ${messagesToSeed} messages:`);
    console.log(`   - Base URL: ${baseUrl}`);
    console.log(`   - Fetch Path: ${fetchPath}`);
    console.log(`   - Population Type: ${populationType}`);
    console.log(`   - Limit: ${limit} records per task`);
    console.log(`   - Chunk Directory: ${chunkDirectory}`);
    console.log(`   - Bulk Reset: ${bulkReset}`);
    console.log(`   - Trust Previous Storage: ${trustPreviousStorage}`);
    console.log(`   - Queue URL: ${queueUrl}`);
    console.log(`   - Using Atomic Counter: ${!!this.atomicCounter}`);
    console.log(`   - Stack ID: ${stackId}`);
    console.log(`   - Region: ${region}`);
    console.log('');

    let successCount = 0;
    const errors: Array<{ messageIndex: number; offset: number; error: any }> = [];
    let previousOffset = 0;

    for (let i = 0; i < messagesToSeed; i++) {
      // First message starts at 0, subsequent messages use atomic counter
      const offset = i === 0 ? 0 : await this.getNextOffset(previousOffset, limit);
      previousOffset = offset;

      const apiChunkerEvent: ApiChunkerEvent = {
        baseUrl,
        fetchPath,
        populationType,
        bulkReset,
        trustPreviousStorage,
        limit,
        offset,
        chunkDirectory: this.chunkDirectory,
        processingMetadata: {
          processedAt: new Date().toISOString(),
          processorVersion: '1.0.0'
        }
      };

      try {
        await handleApiEvent(apiChunkerEvent, queueUrl);
        console.log(`   ✓ Sent message ${i + 1}/${messagesToSeed}: offset=${offset}`);
        successCount++;
      } 
      catch (error) {
        console.error(`   ✗ Failed to send message ${i + 1}/${messagesToSeed} (offset=${offset}):`, error);
        errors.push({ messageIndex: i + 1, offset, error });
        
        // Decide whether to continue or abort
        // For now, continue trying to send remaining messages
      }
    }

    console.log('');
    console.log(`🌱 Queue seeding complete: ${successCount}/${messagesToSeed} messages sent`);
    
    if (errors.length > 0) {
      console.error(`⚠️  Encountered ${errors.length} error(s) during seeding:`);
      errors.forEach(({ messageIndex, offset, error }) => {
        console.error(`   - Message ${messageIndex} (offset=${offset}): ${error.message || error}`);
      });
    }

    return successCount;
  }
}


/**
 * Main function to execute seeding when run directly (e.g., via ts-node or as a script)
 * 
 * NOTE: If you DO NOT want the seeding to trigger scaling up of the ECS service:
 * Go to the autoscaling section of the chunker service configuration in the AWS 
 * management console, and modify the "Number of tasks to run for Chunker" so it reads 
 * "0 - 0", where the maximum capacity is set to 0. This will prevent the service from 
 * launching any tasks even if the desiredCount is bumped up to a positive number.
 * 
 */
async function main() {
  let {
    STACK_ID: stackId,
    REGION: region,
    BASE_URL: baseUrl,
    FETCH_PATH: fetchPath,
    POPULATION_TYPE: populationType,
    DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT: limitStr,
    MESSAGES_TO_SEED: messagesToSeedStr,
    SQS_QUEUE_URL: queueUrl,
    BULK_RESET: bulkReset,
    TRUST_PREVIOUS_STORAGE: trustPreviousStorage,
  } = process.env;

  // Validate required parameters
  if (!stackId) {
    throw new Error('STACK_ID environment variable is required');
  }
  if (!region) {
    throw new Error('REGION environment variable is required');
  }
  if (!baseUrl) {
    throw new Error('BASE_URL is required');
  }
  if (!fetchPath) {
    throw new Error('FETCH_PATH is required');
  }
  if (!populationType) {
    throw new Error('POPULATION_TYPE is required');
  }
  if (!limitStr) {
    throw new Error('DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT is required');
  }
  if (!messagesToSeedStr) {
    throw new Error('MESSAGES_TO_SEED is required');
  }
  if (!queueUrl) {
    throw new Error('QUEUE_URL is required');
  }

  // Parse numeric parameters
  const limit = parseInt(limitStr, 10);
  const messagesToSeed = parseInt(messagesToSeedStr, 10);

  // Validate populationType
  const { PersonDelta, PersonFull } = SyncPopulation;
  const validPopulationTypes = [PersonFull, PersonDelta];
  const normalizedPopulationType = populationType?.toLowerCase() === PersonDelta ? PersonDelta : PersonFull;

  if (!validPopulationTypes.includes(normalizedPopulationType)) {
    throw new Error(`Invalid POPULATION_TYPE: ${populationType}. Must be one of: ${validPopulationTypes.join(', ')}`);
  }

  // Validate numeric parameters
  if (isNaN(limit) || limit <= 0) {
    throw new Error(`Invalid LIMIT: ${limitStr}. Must be a positive integer.`);
  }
  if (isNaN(messagesToSeed) || messagesToSeed <= 0) {
    throw new Error(`Invalid MESSAGES_TO_SEED: ${messagesToSeedStr}. Must be a positive integer.`);
  }

  // Run the seeder
  (async () => {
    try {
      const seeder = new QueueSeeder({
        region: region!,
        stackId: stackId!,
        baseUrl,
        fetchPath,
        populationType: normalizedPopulationType,
        bulkReset: bulkReset?.toLowerCase() === 'true',
        trustPreviousStorage: trustPreviousStorage?.toLowerCase() === 'true',
        limit,
        messagesToSeed,
        queueUrl        
      });
      const successCount = await seeder.seedQueue();

      console.log(`\n✅ Queue seeding completed successfully: ${successCount}/${messagesToSeed} messages sent\n`);
      process.exit(0);
    } catch (error) {
      console.error('\n❌ Queue seeding failed:', error);
      process.exit(1);
    }
  })();
}

/**
 * Test Harness
 * 
 * Run directly with environment variables to seed the queue:
 * ```bash
 * QUEUE_SEEDER_BASE_URL=https://api.bu.edu \
 * QUEUE_SEEDER_FETCH_PATH=/people/v1 \
 * QUEUE_SEEDER_POPULATION_TYPE=person-full \
 * QUEUE_SEEDER_DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT=100 \
 * QUEUE_SEEDER_MESSAGES_TO_SEED=15 \
 * QUEUE_SEEDER_SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/chunker-queue \
 * QUEUE_SEEDER_BULK_RESET=false \
 * QUEUE_SEEDER_TRUST_PREVIOUS_STORAGE=false \
 * QUEUE_SEEDER_STACK_ID=huron-person-fargate-processor \
 * REGION=us-east-1 \
 * npx ts-node src/chunking/fetch/QueueSeeder.ts
 * ```
 * 
 * Or use F5 in VS Code with "Debug current file" configuration.
 */
if (require.main === module) {
  const testEnvironment = TestEnvironment('QUEUE_SEEDER');

  // Required environment variables
  [
    'BASE_URL',
    'FETCH_PATH',
    'POPULATION_TYPE',
    'DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT',
    'MESSAGES_TO_SEED',
    'SQS_QUEUE_URL',
    'STACK_ID',
    'REGION',
    'CHUNK_DIRECTORY'
  ].forEach(testEnvironment.getVar);

  // Optional environment variables
  [
    'BULK_RESET',
    'TRUST_PREVIOUS_STORAGE',
    'DRY_RUN'
  ].forEach(testEnvironment.getVarOrEmptyString);

  main();
}