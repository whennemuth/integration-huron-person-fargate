import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { S3ChunkerEvent } from '../ChunkerSubscriber';

const { 
  CHUNKER_QUEUE_URL,
  REGION: region,
  DRY_RUN = 'false' 
} = process.env;

const sqsClient = new SQSClient({ region });

/**
 * S3-based Chunker Subscriber
 * 
 * Handles S3 file upload events that trigger chunker Fargate tasks via SQS.
 * 
 * This function is called when a large JSON file containing person records is uploaded
 * to the input S3 bucket. Instead of directly launching ECS tasks (which doesn't work
 * with QueueProcessingFargateService), it sends a message to the chunker's SQS queue.
 * The QueueProcessingFargateService monitors the queue depth and auto-scales to process messages.
 * 
 * Sequence:
 * 1. S3 event triggers a foreign Lambda
 * 2. Foreign Lambda calls the main ChunkerSubscriber with custom payload
 * 3. ChunkerSubscriber delegates to this handler
 * 4. Handler sends message to SQS queue with task parameters (INPUT_BUCKET, INPUT_KEY)
 * 5. QueueProcessingFargateService detects queue depth increase
 * 6. Service auto-scales up (increases desired count)
 * 7. ECS launches Fargate task(s) that read from queue
 * 8. Task processes the S3 file and creates NDJSON chunks
 * 9. Queue depth decreases, service scales back down
 * 
 * Input Event Structure:
 * {
 *   s3Path?: string,              // Full S3 path (s3://bucket/key)
 *   bucket?: string,              // S3 bucket name
 *   key?: string,                 // S3 object key
 *   processingMetadata?: {
 *     processedAt?: string,
 *     processorVersion?: string
 *   }
 * }
 * 
 * Note: Event is NOT a standard S3 event. This handler is invoked by a custom subscriber
 * Lambda that receives the S3 event and calls the ChunkerSubscriber with a simplified payload.
 * 
 * @param event - S3-based chunker event
 * @returns Response object with status code and message
 */
export async function handleS3Event(event: S3ChunkerEvent): Promise<any> {
  // Extract S3 bucket and key from event
  let { s3Path, bucket, key, processingMetadata: { processedAt, processorVersion } = {} } = event || {};  

  // Extract environment variables
  const dryRun = DRY_RUN.toLowerCase() === 'true';

  // Validate environment variables
  if (!CHUNKER_QUEUE_URL) {
    console.error('Missing required environment variable: CHUNKER_QUEUE_URL');
    return { statusCode: 500, body: 'Server configuration error, missing CHUNKER_QUEUE_URL environment variable' };
  }

  // Decode and normalize key
  key = decodeURIComponent(key?.replace(/\+/g, ' ') || '');

  // Extract bucket and key from s3Path if not directly provided
  if (!bucket) {
    bucket = getBucketFromS3Path(s3Path);
  }

  if (!key) {
    key = getKeyFromS3Path(s3Path);
  }
  
  if (!bucket || !key) {
    const errMsg = 'Invalid S3 event record: Cannot determine bucket or key from event.';
    console.error(errMsg);
    return { statusCode: 400, body: errMsg };
  }

  // Skip chunk files (only process original JSON files)
  if (key.includes('/chunks/')) {
    console.log('Skipping chunk file:', key);
    return { statusCode: 200, body: 'Skipped chunk file' };
  }

  // End early if in dry run mode
  if (dryRun) {
    const msg = `[DRY RUN] Would start Fargate task for file: s3://${bucket}/${key}`;
    console.log(msg);
    return { statusCode: 200, body: msg };
  }

  console.log(`Processing S3 file: s3://${bucket}/${key}`);

  // Send message to SQS queue to trigger chunker task
  // The QueueProcessingFargateService will detect the message and auto-scale to process it
  // Message format uses camelCase (standardized across all message bodies)
  const message = {
    inputBucket: bucket,
    inputKey: key,
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

/**
 * Extracts the bucket name from an S3 path.
 * @param s3Path - The S3 path in the format s3://bucket/key
 * @returns The bucket name or undefined if the path is invalid
 */
function getBucketFromS3Path(s3Path: string | undefined): string | undefined {
  if (!s3Path) {
    console.error('No S3 path provided');
    return undefined;
  }
  const match = s3Path.match(/^s3:\/\/([^\/]+)\/.+$/);
  if (!match) {
    console.error(`Invalid S3 path: ${s3Path}`);
    return undefined;
  }
  return match[1];
}

/**
 * Extracts the key from an S3 path.
 * @param s3Path - The S3 path in the format s3://bucket/key
 * @returns The key or undefined if the path is invalid
 */
function getKeyFromS3Path(s3Path: string | undefined): string | undefined {
  if (!s3Path) {
    console.error('No S3 path provided');
    return undefined;
  }
  const match = s3Path.match(/^s3:\/\/[^\/]+\/(.+)$/);
  if (!match) {
    console.error(`Invalid S3 path: ${s3Path}`);
    return undefined;
  }
  return match[1];
}
