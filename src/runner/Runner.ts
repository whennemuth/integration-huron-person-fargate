import { ChunkingServiceRunner } from './AbstractRunner';
import { ChunkingOnlyRunnerDecorator } from './decorators/ChunkingOnlyRunnerDecorator';
import { MessagingOnlyRunnerDecorator } from './decorators/MessagingOnlyRunnerDecorator';
import { MockTargetRunnerDecorator } from './decorators/MockTargetRunnerDecorator';
import { RestoreToFullOperationRunnerDecorator } from './decorators/RestoreToFullOperationRunnerDecorator';
import { SourceSimulatorRunnerDecorator } from './decorators/SourceSimulatorRunnerDecorator';
import { QueueSeedingRunner } from './RunnerForQueueSeeding';
import { SingleMessageRunner } from './RunnerForSingleMessage';
import { SinglePersonRunner } from './RunnerForSinglePerson';
import { extractEnvironment, RunnerEnv, setTestEnvironment } from './RunnerTypes';

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
  const env: RunnerEnv = extractEnvironment();
  
  // Validate messagesToPrepopulate is a valid number
  if (env.messagesToPrepopulate && isNaN(Number(env.messagesToPrepopulate))) {
    console.error(`Invalid MESSAGES_TO_PREPOPULATE environment variable: ${env.messagesToPrepopulate}. Must be a number.`);
    return;
  }

  // Auto-correct: Force mock target to true when source simulator is enabled
  if (env.sourceSimulator && !env.mockTarget) {
    console.log('⚠️  Source simulator enabled without mock target - automatically enabling MOCK_TARGET.');
    env.mockTarget = true;
  }

  // Auto-correct: Force mock population to 1 when using single-person mode with source simulator
  if (env.buid && env.sourceSimulator) {
    if (env.sourceSimulatorMockTotalPopulation && env.sourceSimulatorMockTotalPopulation !== 1) {
      console.log(`⚠️  SINGLE_PERSON_BUID is set - automatically overriding MOCK_TOTAL_POPULATION from ${env.sourceSimulatorMockTotalPopulation} to 1`);
    }
    env.sourceSimulatorMockTotalPopulation = 1;
  }

  const seedNumber = parseInt(env.messagesToPrepopulate);

  let runner: ChunkingServiceRunner;

  // Factory patterns: Select appropriate runner based on environment
  if (env.buid) {
    // Single person testing mode
    runner = new SinglePersonRunner(env);
  } else if (seedNumber > 0) {
    // Queue seeding mode for "pre-loaded" parallel processing
    runner = new QueueSeedingRunner(env);
  } else {
    // Default: Single message mode for "slow ramp-up" parallel processing
    runner = new SingleMessageRunner(env);
  }

  console.log(`\n🏃 Runner initialized with environment: ${JSON.stringify(runner.env, null, 2)}\n`);

  // Decorator patterns: Wrap the runnner with additional functionality.
  if (env.messagingOnly) {
    runner = new MessagingOnlyRunnerDecorator(runner);
  }
  if(env.chunkingOnly) {
    runner = new ChunkingOnlyRunnerDecorator(runner);
  }
  if(env.sourceSimulator) {
    runner = new SourceSimulatorRunnerDecorator(runner);
  }
  // SAFETY ENFORCEMENT: Mock target is ALWAYS used when source simulator is active.
  // This prevents simulated data from reaching the real target API.
  if(env.mockTarget || env.sourceSimulator) {
    runner = new MockTargetRunnerDecorator(runner);
  }
  if ( !env.messagingOnly && !env.chunkingOnly && !env.sourceSimulator) {
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