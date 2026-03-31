import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';

const { 
  CHUNKER_QUEUE_URL,
  REGION:region,
  DRY_RUN='false' 
} = process.env;

const sqsClient = new SQSClient({ region });

/**
 * Lambda function to trigger chunker Fargate tasks via SQS.
 * 
 * This function is called by a foreign lambda responding to S3 events when a new JSON file is 
 * uploaded to the input bucket. Instead of directly launching ECS tasks (which doesn't work with 
 * QueueProcessingFargateService), it sends a message to the chunker's SQS queue. The 
 * QueueProcessingFargateService monitors the queue depth and auto-scales to process messages.
 * 
 * Architecture:
 * 1. S3 event triggers the foreign Lambda
 * 2. Foreign Lambda calls this chunker subscribing Lambda with a custom payload
 * 3. Lambda sends message to SQS queue with task parameters
 * 4. QueueProcessingFargateService detects queue depth increase
 * 5. Service auto-scales up (increases desired count)
 * 6. ECS launches Fargate task(s)
 * 7. Task reads message from queue and processes the file
 * 8. Queue depth decreases, service scales back down
 * 
 * Environment Variables:
 * - CHUNKER_QUEUE_URL: URL of the SQS queue for chunker tasks
 * - REGION: AWS region (e.g., 'us-east-2')
 * - DRY_RUN: If set to 'true', the function will not send messages (default: 'false')
 * 
 * Input:
 * - S3 event containing bucket and key of the uploaded JSON file
 * 
 * @param event 
 * NOT a standard S3 event as per:
 * https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html - 
 * this Lambda is invoked by a custom subscriber Lambda that receives the S3 event and then calls 
 * this chunker subscribing Lambda with a custom payload containing s3Path, bucket, key, and processingMetadata.
 * 
 * @returns 
 */
export async function handler(event:any): Promise<any> {
  console.log('Received event:', JSON.stringify(event, null, 2));

  // Extract S3 bucket and key from event.
  let { s3Path, bucket, key, processingMetadata: { processedAt, processorVersion } = {} } = event || {};  

  // Extract environment variables
  const dryRun = DRY_RUN.toLowerCase() === 'true';

  // Validate environment variables
  if( ! CHUNKER_QUEUE_URL) {
    console.error('Missing required environment variable: CHUNKER_QUEUE_URL');
    return { statusCode: 500, body: 'Server configuration error, missing CHUNKER_QUEUE_URL environment variable' };
  }

  key = decodeURIComponent(key?.replace(/\\+/g, ' ') || '');

  if( ! bucket) {
    bucket = getBucketFromS3Path(s3Path);
  }

  if( ! key) {
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

  // End off early if in dry run mode
  if (dryRun) {
    const msg = `[DRY RUN] Would start Fargate task for file: s3://${bucket}/${key}`;
    console.log(msg);
    return { statusCode: 200, body: msg };
  }

  console.log(`Processing file: s3://${bucket}/${key}`);

  // Send message to SQS queue to trigger chunker task
  // The QueueProcessingFargateService will detect the message and auto-scale to process it
  const message = {
    INPUT_BUCKET: bucket,
    INPUT_KEY: key,
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
};

/**
 * Extracts the bucket name from an S3 path.
 * @param s3Path The S3 path in the format s3://bucket/key
 * @returns The bucket name or undefined if the path is invalid
 */
const getBucketFromS3Path = (s3Path: string | undefined): string | undefined => {
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
};

/**
 * Extracts the key from an S3 path.
 * @param s3Path The S3 path in the format s3://bucket/key
 * @returns The key or undefined if the path is invalid
 */
const getKeyFromS3Path = (s3Path: string | undefined): string | undefined => {
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
};