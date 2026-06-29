import { ChunkingServiceRunner } from './AbstractRunner';
import { ChunkingOnlyRunnerDecorator, MessagingOnlyRunnerDecorator, RestoreToFullOperationRunnerDecorator } from './Decorators';
import { QueueSeedingRunner } from './RunnerForQueueSeeding';
import { SingleMessageRunner } from './RunnerForSingleMessage';
import { SinglePersonRunner } from './RunnerForSinglePerson';
import { extractEnvironment, setTestEnvironment } from './RunnerTypes';

/**
 * FOR MANUAL INVOCATION ONLY:
 * 
 * It is expected that an AWS EventBridge schedule will trigger the chunking process on a cron schedule,
 * which will invoke the ChunkerSubscriber Lambda to send messages to the chunker SQS queue. Call this
 * function to manually trigger the chunking service to start a task off schedule, for example to kick 
 * off an initial chunking run or to test the service.
 * There are 3 types of runner, controlled by environment variables:
 * 
 * 1. Single Person Runner: If the environment variable HURON_PERSON_SOURCE_ID is set, the runner 
 *    will fetch and process a single person record for testing.
 * 
 * 2. Queue Seeding Runner: If the environment variable MESSAGES_TO_PREPOPULATE is set to a number 
 *    greater than 0, the runner will pre-populate the chunker queue with that many messages for 
 *    high parallel processing on start.
 * 
 * 3. Single Message Runner: If neither of the above conditions are met, the runner will send a 
 *    single message to the chunker queue for gradually increasing parallel processing on start.
 * 
 * There is a "source simulator" mode that can be enabled via the SOURCE_SIMULATOR_FUNCTION_URL 
 * environment variable. This mode will invoke the use of decorators that wrap the runner with 
 * "pre-flight" steps to disable the chunker or processor services, allowing for testing of the 
 * message creation and chunk file creation features without actually processing the data.
 * This is because the data is fake and should never be taken up by the processor service, which 
 * would send it to the target system.
 */
async function startChunkingService() {
  // Peek at environment to determine which runner to use
  const { 
    buid, messagesToPrepopulate, messagingOnly, chunkingOnly, sourceSimulator, populationScope,
  } = extractEnvironment();
  process.env.POPULATION_SCOPE = populationScope; // Set for downstream use in chunking service

  // Validate messagesToPrepopulate is a valid number
  if (messagesToPrepopulate && isNaN(Number(messagesToPrepopulate))) {
    console.error(`Invalid MESSAGES_TO_PREPOPULATE environment variable: ${messagesToPrepopulate}. Must be a number.`);
    return;
  }

  const seedNumber = parseInt(messagesToPrepopulate);

  // Factory pattern: Select appropriate runner based on environment
  let runner: ChunkingServiceRunner;

  if (buid) {
    // Single person testing mode
    runner = new SinglePersonRunner();
  } else if (seedNumber > 0) {
    // Queue seeding mode for "pre-loaded" parallel processing
    runner = new QueueSeedingRunner();
  } else {
    // Default: Single message mode for "slow ramp-up" parallel processing
    runner = new SingleMessageRunner();
  }

  // Decorator pattern: Wrap the runner with messaging-only or chunking-only behavior if specified
  if (messagingOnly) {
    runner = new MessagingOnlyRunnerDecorator(runner);
  } 
  else if (chunkingOnly || sourceSimulator) {
    if( ! chunkingOnly) {
      // Must prevent source simulator-generated messages from being processed by the processor 
      // service because this is fake data that should NEVER go to the target system, so force chunking-only mode
      console.log('Source simulator enabled, but chunking-only mode is not explicitly set. Forcing chunking-only mode.');
    }
    runner = new ChunkingOnlyRunnerDecorator(runner);
  }
  else if ( ! messagingOnly && !chunkingOnly && !sourceSimulator) {
    // Restore all services to normal operation in case they were previously disabled by a messaging-only or chunking-only run
    console.log('Neither messaging-only nor chunking-only mode specified. Restoring all services to normal operation.');
    runner = new RestoreToFullOperationRunnerDecorator(runner);
  }

  // Execute the runner's template method
  await runner.start();
}

// Run if executed directly
if (require.main === module) {

  setTestEnvironment()

  startChunkingService();
}