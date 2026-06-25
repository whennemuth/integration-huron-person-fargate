import { ConfigManager, DataSourceConfig } from "integration-huron-person";
import { getLocalConfig } from "./Utils";
import { SyncPopulation } from "../docker/chunkTypes";
import { DesiredCount } from "./DesiredCount";
import { QueueSeeder } from "./chunking/fetch/QueueSeeder";
import { ApiChunkerEvent } from "./chunking/ChunkerSubscriber";
import { handleApiEvent } from "./chunking/fetch/ChunkerApiSubscriber";
import { TestEnvironment } from "integration-core";

/**
 * FOR TESTING:
 * 
 * It is expected that an AWS EventBridge schedule will trigger the chunking process on a cron schedule,
 * which will invoke the ChunkerSubscriber Lambda to send messages to the chunker SQS queue. call this
 * function to manually trigger the chunking service to start a task off schedule, for example to kick 
 * off an initial chunking run or to test the service.
 * 
 * NOTE: If you do not want new chunk files appearing in the bucket to set off the processor service,
 * you can set the environment variable DRY_RUN=true for the chunker subscriber lambda function to 
 * prevent the chunker from sending messages to the processor queue. The is most easily done through 
 * the AWS management console, or you can use the AWS CLI:
 * 
 * aws lambda update-function-configuration \
 *  --region us-east-2 \
 *  --cli-input-json "$(aws lambda get-function-configuration \
 *     --function-name chunker-subscriber-dev \
 *     --region us-east-2 | jq '.Environment.Variables += {"DRY_RUN": "true"} | {FunctionName: .FunctionName, Environment: .Environment}')"
 */
async function startChunkingService() {
  /** Read additional configuration from environment */
  const {
    // For configuring the chunking tasks.
    SECRET_ARN: secretArn,
    HURON_PERSON_CONFIG_PATH: configPath,
    CHUNKER_QUEUE_URL: queueUrl,
    POPULATION_TYPE: populationType,
    DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT: peopleLimit,
    SINGLE_PERSON_BUID: buid,
    REGION: region,
    STACK_ID: stackId,
    LANDSCAPE: landscape,
    BULK_RESET: bulkReset,
    TRUST_PREVIOUS_STORAGE: trustPreviousStorage,
    // For seeding the queue
    MESSAGES_TO_PREPOPULATE: messagesToPrepopulate = '0',
    DESIRED_COUNT,
    ECS_CLUSTER_NAME: clusterName,
    ECS_SERVICE_NAME: serviceName
  } = process.env;

  /** Load configuration. */
  const configManager = ConfigManager.getInstance();
  const localConfigPath = configPath || getLocalConfig();
  const config = await configManager
    .reset()
    .fromEnvironment()                            // ← Environment is first - takes precedence over all.
    .fromFileSystem(localConfigPath)              // ← Local dev only
    .fromJsonString('HURON_PERSON_CONFIG_JSON')   // ← TaskDef secret injection
    .fromSecretManager(secretArn)                 // ← Fallback to Secrets Manager
    .getConfigAsync(buid ? 'person' : 'people');

  let desiredCount = DESIRED_COUNT ? parseInt(DESIRED_COUNT) : 0;
  if(desiredCount > 0) {
    if( ! clusterName ) {
      console.error('ECS_CLUSTER_NAME environment variable is required to set desired count.');
      return;
    }
    if( ! serviceName ) {
      console.error('ECS_SERVICE_NAME environment variable is required to set desired count.');
      return;
    }
  }

  /**
   * Single person test?
   * 
   * If a buid is provided, pretend the single person endpoint is the full dataset. This allows us to 
   * test the full end to end flow in such a way as to confine errors to one person and in a 
   * conveniently short time due to the miniscule payload.
   */
  let baseUrl: string | undefined, fetchPath: string | undefined;
  if(buid) {
    const { 
      endpointConfig: { baseUrl: personBaseUrl } = {}, fetchPath: personFetchPath 
    } = config.dataSource.person || {};
    baseUrl = personBaseUrl;
    fetchPath = `${personFetchPath}?buid=${buid}`;
  }
  else {
    const { 
      endpointConfig: { baseUrl: peopleBaseUrl } = {}, fetchPath: peopleFetchPath 
    } = config.dataSource?.people as DataSourceConfig || {};
    baseUrl = peopleBaseUrl;
    fetchPath = peopleFetchPath;
  }

  /** Validate configuration */
  if(!baseUrl) {
    console.error('Missing baseUrl in configuration!');
    return;
  }
  if(!fetchPath) {
    console.error('Missing fetchPath in configuration!');
    return;
  }
  if(!queueUrl) {
    console.error('Missing CHUNKER_QUEUE_URL environment variable!');
    return;
  }
  if(messagesToPrepopulate && isNaN(Number(messagesToPrepopulate))) {
    console.error(`Invalid MESSAGES_TO_PREPOPULATE environment variable: ${messagesToPrepopulate}. Must be a number.`);
    return;
  }

  const seedNumber = parseInt(messagesToPrepopulate);
  const { PersonDelta, PersonFull } = SyncPopulation;
  const normalizedPopulationType = populationType?.toLowerCase() === PersonDelta ? PersonDelta : PersonFull;
  
  if(seedNumber > 0) {

    if(buid) {
      console.warn(`MESSAGES_TO_PREPOPULATE is set to ${seedNumber} > 0, but SINGLE_PERSON_BUID is also ` +
        `set (${buid}). Seeding the queue is not appropriate when processing just one person. Cancelling operation`);
      return;
    }

    if( ! region) {
      console.error('REGION environment variable is required to set desired count.');
      return;
    }
    if( ! stackId) {
      console.error('STACK_ID environment variable is required to set desired count.');
      return;
    }
    if( ! landscape) {
      console.error('LANDSCAPE environment variable is required to set desired count.');
      return;
    }

    // Raise the desired count to a level "worthy" of the prepopulation to kick off the processing of the 
    // seeded messages. This is done after seeding to allow the service to process the initial messages 
    // as quickly as possible rather than waiting for the scale up to occur first.
    if(desiredCount > 0) {
      console.log(`\n🚀 Scaling up ECS service ${serviceName} in cluster ${clusterName} to desired count of ${desiredCount}...\n`);
      const desiredCountManager = new DesiredCount({ clusterName, serviceName, region });
      const max = await desiredCountManager.getMax();
      if(max !== undefined && desiredCount > max) {
        console.warn(`Desired count of ${desiredCount} exceeds the maximum allowed by the auto-scaling configuration (${max}). ` +
          `Cancelling operation`);
        return;
      }
      await desiredCountManager.setTo(desiredCount);
    }

    // Seed the queue with initial messages to enable "hitting the ground running"
    console.log(`\n🌱 Seeding queue with ${seedNumber} messages...\n`);    
    const queueSeeder = new QueueSeeder({
      region,
      stackId,
      landscape,
      baseUrl,
      fetchPath,
      populationType: normalizedPopulationType,
      bulkReset: bulkReset?.toLowerCase() === 'true',
      trustPreviousStorage: trustPreviousStorage?.toLowerCase() === 'true',
      limit: peopleLimit ? parseInt(peopleLimit) : 0,
      messagesToSeed: seedNumber,
      queueUrl: queueUrl!,
      dryRun: false
    });
    await queueSeeder.resetAtomicCounter();
    await queueSeeder.seedQueue();
    
    console.log(`\n✓ Queue seeding complete. Ready to scale up desiredCount to ${seedNumber}.\n`);
  } else {
    // Send a single initial message to trigger the chunking process (normal one-in/one-out pattern)
    const apiChunkerEvent = {
      baseUrl,
      fetchPath,
      populationType: normalizedPopulationType,
      bulkReset: bulkReset?.toLowerCase() === 'true',
      trustPreviousStorage: trustPreviousStorage?.toLowerCase() === 'true',
      limit: peopleLimit ? parseInt(peopleLimit) : 0,
      offset: 0,
      processingMetadata: {
        processedAt: new Date().toISOString(),
        processorVersion: '1.0.0'
      }
    } satisfies ApiChunkerEvent;

    // Send the event to the queue to trigger the chunking process.
    await handleApiEvent(apiChunkerEvent, queueUrl);
  }
}

// Run if executed directly
if (require.main === module) {
  const testEnvironment = TestEnvironment('RUNNER');

  [
    'HURON_PERSON_CONFIG_PATH',
    'SECRET_ARN',
    'CHUNKER_QUEUE_URL',
    'POPULATION_TYPE',
    'DATASOURCE_ENDPOINTCONFIG_PEOPLE_LIMIT',
    'SINGLE_PERSON_BUID',
    'REGION',
    'MESSAGES_TO_PREPOPULATE',
    'DESIRED_COUNT',
    'ECS_CLUSTER_NAME',
    'ECS_SERVICE_NAME',
    'STACK_ID',
  ].forEach(testEnvironment.getVar);

  [
    'BULK_RESET',
    'TRUST_PREVIOUS_STORAGE'
  ].forEach(testEnvironment.getVarOrEmptyString);

  startChunkingService();
}