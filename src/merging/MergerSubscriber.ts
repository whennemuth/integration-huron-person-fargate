import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';

export interface ChunkMetadata {
  chunkCount: number;
  totalRecords: number;
  itemsPerChunk: number;
  sourceFile: string;
  chunkDirectory: string;
  deltaStoragePath: string;
  bulkReset?: boolean;
  createdAt: string;
  chunkKeys: string[];
}

const {
  MERGER_QUEUE_URL,
  REGION: region,
  CHUNKS_BUCKET_NAME,
} = process.env;

const sqsClient = new SQSClient({ region });
const s3Client = new S3Client({ region });

/**
 * Lambda handler triggered by S3 events when delta chunk files are created.
 * Checks if all expected delta chunks have been created. If so, sends message to SQS
 * to trigger the merger Fargate task via QueueProcessingFargateService auto-scaling.
 * 
 * This function is invoked each time a processor task writes a delta chunk file to S3.
 * It reads the metadata file to determine how many chunks are expected, counts the actual
 * delta chunks created, and sends a message to the merger queue when all are complete.
 * 
 * Architecture:
 * 1. Delta chunk file created → S3 event triggers this Lambda
 * 2. Lambda checks if all delta chunks are ready
 * 3. If ready: sends message to merger SQS queue
 * 4. QueueProcessingFargateService detects queue depth increase
 * 5. Service auto-scales up and launches merger Fargate task
 * 6. Merger task reads message and processes delta files
 * 7. Queue empties, service scales back down
 * 
 * Environment Variables:
 * - MERGER_QUEUE_URL: URL of the SQS queue for merger tasks
 * - REGION: AWS region (e.g., 'us-east-2')
 * - CHUNKS_BUCKET_NAME: Name of the S3 bucket where chunk and delta files are stored
 * 
 * Input:
 * - S3 event when processor completes (e.g., deltas/person-full/2026-03-03T19:58:41.277Z/chunk-0042_processing_complete.json)
 * 
 * Output:
 * - If not all chunks are ready: logs status and returns 200 with message "Still processing"
 * - If all chunks are ready: triggers merger Fargate task, logs result, and returns 200 with message "Merger triggered"
 * - If error occurs: logs error and returns 500 with message "Failed to trigger merger"
 */
export async function handler(event: any): Promise<any> {
  console.log('=== Merger Trigger (S3 Event) ===');
  console.log('Event:', JSON.stringify(event, null, 2));

  // Extract marker file info from S3 event
  const records = event.Records || [];
  if (records.length === 0) {
    console.log('No S3 records in event');
    return { statusCode: 200, body: 'No records to process' };
  }

  const record = records[0]; // Process first record
  const bucket = record.s3?.bucket?.name;
  const markerFileKey = decodeURIComponent(record.s3?.object?.key?.replace(/\+/g, ' ') || '');

  if (!bucket || !markerFileKey) {
    console.error('Invalid S3 event record:', record);
    return { statusCode: 400, body: 'Invalid S3 event' };
  }

  console.log(`Marker file created: s3://${bucket}/${markerFileKey}`);

  // Extract delta storage path from the marker file key
  // Example: "deltas/person-full/2026-03-03T19:58:41.277Z/chunk-0042_processing_complete.json" 
  //       -> "deltas/person-full/2026-03-03T19:58:41.277Z"
  const deltaStoragePath = markerFileKey.substring(0, markerFileKey.lastIndexOf('/'));
  
  // Derive chunk directory from delta storage path
  // Example: "deltas/person-full/2026-03-03T19:58:41.277Z" 
  //       -> "chunks/person-full/2026-03-03T19:58:41.277Z"
  const chunkDirectory = deltaStoragePath.replace(/^deltas\//, 'chunks/');

  console.log(`Delta storage path: ${deltaStoragePath}`);
  console.log(`Chunk directory: ${chunkDirectory}`);

  // Step 1: Get expected chunk count from metadata
  const metadata = await getChunkMetadata(chunkDirectory, bucket, region);

  if (!metadata) {
    console.log('No metadata file found - cannot determine completion');
    return { statusCode: 200, body: 'No metadata found' };
  }

  const expectedChunks = metadata.chunkCount;

  // Step 2: Count actual marker files in S3 (one per completed chunk)
  const actualChunks = await countCompletedChunks(deltaStoragePath);

  // Step 3: Check if all chunks are ready
  if (actualChunks < expectedChunks) {
    console.log(
      `⏳ Waiting for more delta chunks: ${actualChunks}/${expectedChunks} ready`
    );
    return { statusCode: 200, body: 'Still processing' };
  }

  if (actualChunks > expectedChunks) {
    console.warn(
      `⚠️  More delta chunks than expected: ${actualChunks} > ${expectedChunks} (possible stale data)`
    );
  }

  // Step 4: All chunks ready - trigger merger
  console.log(`✅ All delta chunks ready: ${actualChunks}/${expectedChunks}`);

  const triggered = await triggerMerger(chunkDirectory);

  if (triggered) {
    return { statusCode: 200, body: 'Merger triggered' };
  } else {
    return { statusCode: 500, body: 'Failed to trigger merger' };
  }
}

/**
 * Reads metadata file to determine expected chunk count
 * @param chunkDirectory - The chunk directory path (e.g., "chunks/person-full/2026-03-03T19:58:41.277Z")
 * @param bucketName - S3 bucket name (optional, uses CHUNKS_BUCKET_NAME env var if not provided)
 * @param region - AWS region (optional, uses REGION env var if not provided)
 */
export async function getChunkMetadata(
  chunkDirectory: string,
  bucketName?: string,
  region?: string
): Promise<ChunkMetadata | null> {
  const bucket = bucketName || CHUNKS_BUCKET_NAME;
  if (!bucket) {
    console.error('Missing required bucket name (parameter or CHUNKS_BUCKET_NAME env var)');
    return null;
  }

  const metadataKey = `${chunkDirectory}/_metadata.json`;
  const client = new S3Client({ region: region || process.env.REGION });

  try {
    const response = await client.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: metadataKey,
      })
    );

    const body = await response.Body?.transformToString();
    if (!body) {
      console.log(`No metadata file found at s3://${bucket}/${metadataKey}`);
      return null;
    }

    const metadata = JSON.parse(body) as ChunkMetadata;
    console.log(`Metadata found: ${metadata.chunkCount} chunks expected`);
    console.log(`Delta storage path from metadata: ${metadata.deltaStoragePath}`);
    return metadata;
  } catch (error: any) {
    if (error.name === 'NoSuchKey') {
      console.log(`No metadata file found (no active chunking in progress)`);
      return null;
    }
    console.error(`Error reading metadata: ${error.message}`);
    return null;
  }
}

/**
 * Counts completed chunks by counting marker files in S3.
 * Each marker file indicates a chunk has finished processing.
 * This approach prevents the race condition where delta files are counted
 * but then deleted before the merger can run.
 * 
 * @param deltaStoragePath - The delta storage path (e.g., "deltas/person-full/2026-03-03T19:58:41.277Z")
 */
async function countCompletedChunks(deltaStoragePath: string): Promise<number> {
  if (!CHUNKS_BUCKET_NAME) {
    return 0;
  }

  const prefix = `${deltaStoragePath}/`;

  try {
    const response = await s3Client.send(
      new ListObjectsV2Command({
        Bucket: CHUNKS_BUCKET_NAME,
        Prefix: prefix,
      })
    );

    const markerFiles = (response.Contents || [])
      .map((obj) => obj.Key!)
      .filter((key) => /chunk-\d+_processing_complete\.json$/.test(key));

    console.log(`Found ${markerFiles.length} completed chunks in s3://${CHUNKS_BUCKET_NAME}/${prefix}`);
    return markerFiles.length;
  } catch (error: any) {
    console.error(`Error listing marker files: ${error.message}`);
    return 0;
  }
}

/**
 * Triggers the merger Fargate task by sending a message to SQS.
 * The QueueProcessingFargateService will detect the message and auto-scale to process it.
 * @param chunkDirectory - The chunk directory path to pass to merger
 */
async function triggerMerger(chunkDirectory: string): Promise<boolean> {
  if (!MERGER_QUEUE_URL) {
    console.error('Missing required environment variable: MERGER_QUEUE_URL');
    return false;
  }
  if (!CHUNKS_BUCKET_NAME) {
    console.error('Missing required environment variable: CHUNKS_BUCKET_NAME');
    return false;
  }

  console.log('🚀 Triggering merger via SQS queue...');

  // Prepare message with task parameters
  // The merger task will read these from the SQS message
  // Message format uses camelCase (standardized across all message bodies)
  const message = {
    chunksBucket: CHUNKS_BUCKET_NAME,
    chunkDirectory,
  };

  try {
    console.log('Sending message to merger queue:', JSON.stringify(message, null, 2));
    await sqsClient.send(new SendMessageCommand({
      QueueUrl: MERGER_QUEUE_URL,
      MessageBody: JSON.stringify(message),
    }));
    console.log('✅ Message sent to merger queue successfully');
    console.log('   QueueProcessingFargateService will auto-scale to process this message');
    return true;
  } catch (error: any) {
    console.error(`❌ Failed to send message to merger queue: ${error.message}`);
    return false;
  }
}

