import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { ApiChunkerEvent } from '../ChunkerSubscriber';

const { 
  CHUNKER_QUEUE_URL,
  REGION: region,
  DRY_RUN = 'false' 
} = process.env;

const sqsClient = new SQSClient({ region });

/**
 * API-based Chunker Subscriber
 * 
 * Handles EventBridge schedule events that trigger periodic API-based chunker Fargate tasks via SQS.
 * 
 * This function is called by an EventBridge schedule (configured in ChunkerService) to periodically
 * fetch person records from the BU CDM API endpoint. It sends a message to the chunker's SQS queue
 * with API endpoint parameters. The QueueProcessingFargateService monitors the queue depth and
 * auto-scales to process messages.
 * 
 * Sequence:
 * 1. EventBridge schedule triggers the main ChunkerSubscriber Lambda on a cron schedule
 * 2. ChunkerSubscriber delegates to this handler
 * 3. Handler sends message to SQS queue with task parameters (BASE_URL, FETCH_PATH, POPULATION_TYPE)
 * 4. QueueProcessingFargateService detects queue depth increase
 * 5. Service auto-scales up (increases desired count)
 * 6. ECS launches Fargate task(s) that read from queue
 * 7. Task fetches person data from API endpoint and creates NDJSON chunks
 * 8. Queue depth decreases, service scales back down
 * 
 * Input Event Structure:
 * {
 *   baseUrl: string,              // API base URL (e.g., 'https://api.bu.edu')
 *   fetchPath: string,            // API fetch path (e.g., '/people/v1')
 *   populationType: string,       // 'person-full' or 'person-delta'
 *   processingMetadata?: {
 *     processedAt?: string,
 *     processorVersion?: string
 *   }
 * }
 * 
 * The populationType field determines what data is fetched:
 * - 'person-full': Fetch all person records (initial sync or full refresh)
 * - 'person-delta': Fetch only changed person records (incremental sync)
 * 
 * Note: This handler is invoked by EventBridge schedule with a synthetic event containing
 * API endpoint configuration. The schedule is only created if the IContext configuration
 * specifies fetchSchedule with enabled=true and a valid cronExpression.
 * 
 * @param event - API-based chunker event
 * @returns Response object with status code and message
 */
export async function handleApiEvent(event: ApiChunkerEvent): Promise<any> {
  // Extract API parameters from event
  const { baseUrl, fetchPath, populationType, processingMetadata: { processedAt, processorVersion } = {} } = event;

  // Extract environment variables
  const dryRun = DRY_RUN.toLowerCase() === 'true';

  // Validate environment variables
  if (!CHUNKER_QUEUE_URL) {
    console.error('Missing required environment variable: CHUNKER_QUEUE_URL');
    return { statusCode: 500, body: 'Server configuration error, missing CHUNKER_QUEUE_URL environment variable' };
  }

  // Validate required event fields
  if (!baseUrl || !fetchPath || !populationType) {
    const errMsg = 'Invalid API event: Missing required fields (baseUrl, fetchPath, populationType)';
    console.error(errMsg);
    return { statusCode: 400, body: errMsg };
  }

  // Validate populationType
  const validPopulationTypes = ['person-full', 'person-delta'];
  if (!validPopulationTypes.includes(populationType)) {
    const errMsg = `Invalid populationType: ${populationType}. Must be one of: ${validPopulationTypes.join(', ')}`;
    console.error(errMsg);
    return { statusCode: 400, body: errMsg };
  }

  // End early if in dry run mode
  if (dryRun) {
    const msg = `[DRY RUN] Would start Fargate task for API fetch: ${baseUrl}${fetchPath} (${populationType})`;
    console.log(msg);
    return { statusCode: 200, body: msg };
  }

  console.log(`Processing API fetch: ${baseUrl}${fetchPath} (${populationType})`);

  // Send message to SQS queue to trigger chunker task
  // The QueueProcessingFargateService will detect the message and auto-scale to process it
  // Message format matches what ChunkFromAPI.setTaskParametersFromQueueMessageBody() expects
  const message = {
    BASE_URL: baseUrl,
    FETCH_PATH: fetchPath,
    POPULATION_TYPE: populationType
  };

  try {
    console.log('Sending message to chunker queue:', JSON.stringify(message, null, 2));
    await sqsClient.send(new SendMessageCommand({
      QueueUrl: CHUNKER_QUEUE_URL,
      MessageBody: JSON.stringify(message),
    }));
    console.log('✅ Message sent to queue successfully');
    console.log('   QueueProcessingFargateService will auto-scale to process this message');
    return { statusCode: 200, body: 'Message sent to chunker queue' };
  } catch (error) {
    console.error('Failed to send message to queue:', error);
    throw error;
  }
}
