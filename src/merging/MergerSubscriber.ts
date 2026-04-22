import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';

interface ChunkMetadata {
  chunkCount: number;
  totalRecords: number;
  itemsPerChunk: number;
  sourceFile: string;
  chunkDirectory: string;
  deltaStoragePath: string;
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
 * - S3 event when delta chunk file is created (e.g., deltas/person-full/2026-03-03T19:58:41.277Z/chunk-0042.ndjson)
 * 
 * Output:
 * - If not all chunks are ready: logs status and returns 200 with message "Still processing"
 * - If all chunks are ready: triggers merger Fargate task, logs result, and returns 200 with message "Merger triggered"
 * - If error occurs: logs error and returns 500 with message "Failed to trigger merger"
 */
export async function handler(event: any): Promise<any> {
  console.log('=== Merger Trigger (S3 Event) ===');
  console.log('Event:', JSON.stringify(event, null, 2));

  // Extract delta file info from S3 event
  const records = event.Records || [];
  if (records.length === 0) {
    console.log('No S3 records in event');
    return { statusCode: 200, body: 'No records to process' };
  }

  const record = records[0]; // Process first record
  const bucket = record.s3?.bucket?.name;
  const deltaFileKey = decodeURIComponent(record.s3?.object?.key?.replace(/\+/g, ' ') || '');

  if (!bucket || !deltaFileKey) {
    console.error('Invalid S3 event record:', record);
    return { statusCode: 400, body: 'Invalid S3 event' };
  }

  console.log(`Delta file created: s3://${bucket}/${deltaFileKey}`);

  // Extract delta storage path from the delta file key
  // Example: "deltas/person-full/2026-03-03T19:58:41.277Z/chunk-0042.ndjson" 
  //       -> "deltas/person-full/2026-03-03T19:58:41.277Z"
  const deltaStoragePath = deltaFileKey.substring(0, deltaFileKey.lastIndexOf('/'));
  
  // Derive chunk directory from delta storage path
  // Example: "deltas/person-full/2026-03-03T19:58:41.277Z" 
  //       -> "chunks/person-full/2026-03-03T19:58:41.277Z"
  const chunkDirectory = deltaStoragePath.replace(/^deltas\//, 'chunks/');

  console.log(`Delta storage path: ${deltaStoragePath}`);
  console.log(`Chunk directory: ${chunkDirectory}`);

  // Step 1: Get expected chunk count from metadata
  const metadata = await getChunkMetadata(chunkDirectory);

  if (!metadata) {
    console.log('No metadata file found - cannot determine completion');
    return { statusCode: 200, body: 'No metadata found' };
  }

  const expectedChunks = metadata.chunkCount;

  // Step 2: Count actual delta chunk files in S3
  const actualChunks = await countDeltaChunkFiles(deltaStoragePath);

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
 */
async function getChunkMetadata(chunkDirectory: string): Promise<ChunkMetadata | null> {
  if (!CHUNKS_BUCKET_NAME) {
    console.error('Missing required environment variable: CHUNKS_BUCKET_NAME');
    return null;
  }

  const metadataKey = `${chunkDirectory}/_metadata.json`;

  try {
    const response = await s3Client.send(
      new GetObjectCommand({
        Bucket: CHUNKS_BUCKET_NAME,
        Key: metadataKey,
      })
    );

    const body = await response.Body?.transformToString();
    if (!body) {
      console.log(`No metadata file found at s3://${CHUNKS_BUCKET_NAME}/${metadataKey}`);
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
 * Counts delta chunk files in S3
 * @param deltaStoragePath - The delta storage path (e.g., "deltas/person-full/2026-03-03T19:58:41.277Z")
 */
async function countDeltaChunkFiles(deltaStoragePath: string): Promise<number> {
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

    const deltaChunkFiles = (response.Contents || [])
      .map((obj) => obj.Key!)
      .filter((key) => /chunk-\d+\.ndjson$/.test(key));

    console.log(`Found ${deltaChunkFiles.length} delta chunk files in s3://${CHUNKS_BUCKET_NAME}/${prefix}`);
    return deltaChunkFiles.length;
  } catch (error: any) {
    console.error(`Error listing delta chunk files: ${error.message}`);
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

