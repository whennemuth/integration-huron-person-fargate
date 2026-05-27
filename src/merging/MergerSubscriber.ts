import { ListObjectsV2Command, S3Client } from '@aws-sdk/client-s3';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { ChunkMetadata, MetadataManager } from '../chunking/Metadata';

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

  const {
    CHUNKS_BUCKET_NAME, REGION, MERGER_QUEUE_URL, DRY_RUN = 'false'
  } = process.env; // Destructure env vars here for easier reference and potential validation/logging

  console.log(`Environment Variables - ${JSON.stringify({ 
    CHUNKS_BUCKET_NAME, REGION, MERGER_QUEUE_URL, DRY_RUN 
  }, null, 2)}`);

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

  // Step 1: Validate contiguous marker ordinals to determine completion
  // This replaces the metadata.chunkCount gate with marker-based validation.
  // Merger is triggered only when markers form uninterrupted sequence 0..N.
  const { isComplete, actualChunks, maxOrdinal, hasGaps } = await validateContiguousMarkerOrdinals(
    deltaStoragePath,
    markerFileKey
  );

  if (!isComplete) {
    if (hasGaps) {
      console.log(
        `⏳ Marker gap detected: ${actualChunks} markers found but ordinals not contiguous (max: ${maxOrdinal})`
      );
    } else {
      console.log(
        `⏳ Waiting for markers: only ${actualChunks} markers found starting from 0`
      );
    }
    return { statusCode: 200, body: 'Still processing' };
  }

  // Step 2: All markers are contiguous - get metadata for context and audit trail
  const metadata = await getChunkMetadata(chunkDirectory, bucket, region);
  
  if (!metadata) {
    console.log('Note: metadata file not found, but marker ordinals are contiguous - proceeding with merger');
  } else if (metadata.chunkCount && actualChunks !== metadata.chunkCount) {
    console.warn(
      `⚠️  Metadata mismatch: metadata says ${metadata.chunkCount} chunks but markers show ${actualChunks} (using marker count)`
    );
  }

  const dryRun = DRY_RUN.toLowerCase().trim() === 'true';
  if(dryRun) {
    console.log('DRY_RUN mode enabled - skipping merger trigger');
    return { statusCode: 200, body: 'DRY_RUN - Merger trigger skipped' };
  }

  // Step 3: All markers contiguous - trigger merger
  console.log(`✅ All markers contiguous (${actualChunks} total): 0..${maxOrdinal}`);

  const triggered = await triggerMerger(
    chunkDirectory, 
    metadata?.createdAt || new Date().toISOString()
  );

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

  const metadata = await MetadataManager.read({
    bucketName: bucket,
    chunkDirectory,
    region: region || process.env.REGION
  });

  if (Object.keys(metadata).length === 0) {
    return null;
  }

  // Log additional info specific to merger context
  if (metadata.chunkCount) {
    console.log(`Metadata found: ${metadata.chunkCount} chunks expected`);
  }
  if (metadata.deltaStoragePath) {
    console.log(`Delta storage path from metadata: ${metadata.deltaStoragePath}`);
  }

  return metadata as ChunkMetadata;
}

/**
 * Validates that marker files form a contiguous ordinal sequence starting from 0.
 * This replaces metadata.chunkCount as the authoritative completion signal.
 * 
 * Returns true only when markers present: 0, 1, 2, ..., N with no gaps.
 * Failed-status markers still count as terminal (merger proceeds regardless of failure).
 * Handles S3 pagination for 1000+ markers.
 * 
 * @param deltaStoragePath - The delta storage path (e.g., "deltas/person-full/2026-03-03T19:58:41.277Z")
 * @param currentMarkerKey - The current marker file that triggered this check (to avoid stale-run issues)
 */
async function validateContiguousMarkerOrdinals(
  deltaStoragePath: string,
  currentMarkerKey: string
): Promise<{ isComplete: boolean; actualChunks: number; maxOrdinal: number; hasGaps: boolean }> {
  if (!CHUNKS_BUCKET_NAME) {
    return { isComplete: false, actualChunks: 0, maxOrdinal: -1, hasGaps: true };
  }

  const prefix = `${deltaStoragePath}/`;
  const markerOrdinals: Set<number> = new Set();
  let continuationToken: string | undefined;

  try {
    // List all marker files with pagination support
    do {
      const response = await s3Client.send(
        new ListObjectsV2Command({
          Bucket: CHUNKS_BUCKET_NAME,
          Prefix: prefix,
          ContinuationToken: continuationToken,
        })
      );

      // Extract ordinals from marker file names
      if (response.Contents) {
        for (const obj of response.Contents) {
          const key = obj.Key!;
          // Match: chunk-NNNN_processing_complete.json
          const match = key.match(/chunk-(\d+)_processing_complete\.json$/);
          if (match) {
            const ordinal = parseInt(match[1], 10);
            markerOrdinals.add(ordinal);
          }
        }
      }

      // Handle pagination
      if (response.IsTruncated) {
        continuationToken = response.NextContinuationToken;
      } else {
        continuationToken = undefined;
      }
    } while (continuationToken);

    // Check for contiguous sequence starting at 0
    const actualChunks = markerOrdinals.size;
    
    if (actualChunks === 0) {
      console.log(`No marker files found in s3://${CHUNKS_BUCKET_NAME}/${prefix}`);
      return { isComplete: false, actualChunks: 0, maxOrdinal: -1, hasGaps: true };
    }

    // Find max ordinal
    const maxOrdinal = Math.max(...Array.from(markerOrdinals));

    // Validate contiguity: all numbers from 0 to maxOrdinal must be present
    const hasGaps = actualChunks !== (maxOrdinal + 1);
    
    if (hasGaps) {
      // Log which ordinals are missing for diagnostics
      const missing: number[] = [];
      for (let i = 0; i <= maxOrdinal; i++) {
        if (!markerOrdinals.has(i)) {
          missing.push(i);
        }
      }
      console.log(`Markers found: ${Array.from(markerOrdinals).sort((a, b) => a - b).join(', ')}`);
      console.log(`Missing ordinals: ${missing.join(', ')}`);
      return { isComplete: false, actualChunks, maxOrdinal, hasGaps: true };
    }

    // Contiguous sequence 0..maxOrdinal confirmed
    console.log(`✓ Contiguous marker ordinals validated: 0..${maxOrdinal} (${actualChunks} total)`);
    return { isComplete: true, actualChunks, maxOrdinal, hasGaps: false };

  } catch (error: any) {
    console.error(`Error validating marker ordinals: ${error.message}`);
    return { isComplete: false, actualChunks: 0, maxOrdinal: -1, hasGaps: true };
  }
}

/**
 * Counts completed chunks by counting marker files in S3.
 * DEPRECATED: Use validateContiguousMarkerOrdinals() instead.
 * This function is kept for backwards compatibility but should not be used for merger gating.
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
 * @param createdAt - ISO timestamp when chunking started (from metadata file)
 */
async function triggerMerger(chunkDirectory: string, createdAt: string): Promise<boolean> {
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
    createdAt, // Include timestamp for full sync duration tracking
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

