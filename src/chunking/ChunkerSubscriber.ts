/**
 * Chunker Subscriber Entry Point
 * 
 * This Lambda function acts as a dispatcher for chunker Fargate tasks.
 * It receives events from two possible sources:
 * 
 * 1. **S3-based events**: Triggered when a large JSON file is uploaded to the input S3 bucket
 *    Event shape: { s3Path, bucket, key, processingMetadata }
 * 
 * 2. **API-based events**: Triggered by EventBridge schedule for periodic API fetching
 *    Event shape: { baseUrl, fetchPath, populationType, processingMetadata }
 * 
 * Based on the event shape, this function delegates to the appropriate handler:
 * - S3 events → ChunkerS3Subscriber
 * - API events → ChunkerApiSubscriber
 * 
 * Both handlers send messages to the chunker SQS queue, which triggers the
 * QueueProcessingFargateService to auto-scale and process the data.
 * 
 * Architecture:
 * 1. Event (S3 upload or EventBridge schedule) triggers this Lambda
 * 2. Lambda analyzes event structure and delegates to appropriate handler
 * 3. Handler sends message to SQS queue with task parameters
 * 4. QueueProcessingFargateService detects queue depth increase
 * 5. Service auto-scales up (increases desired count)
 * 6. ECS launches Fargate task(s)
 * 7. Task reads message from queue and processes the data
 * 8. Queue depth decreases, service scales back down
 * 
 * Environment Variables:
 * - CHUNKER_QUEUE_URL: URL of the SQS queue for chunker tasks
 * - REGION: AWS region (e.g., 'us-east-2')
 * - DRY_RUN: If set to 'true', the function will not send messages (default: 'false')
 */

import { handleS3Event } from './filedrop/ChunkerS3Subscriber';
import { handleApiEvent } from './fetch/ChunkerApiSubscriber';
import { SyncPopulation } from '../../docker/chunkTypes';

/**
 * S3-based event structure
 */
export interface S3ChunkerEvent {
  s3Path?: string;
  bucket?: string;
  key?: string;
  processingMetadata?: {
    processedAt?: string;
    processorVersion?: string;
  };
}

/**
 * API-based event structure
 */
export interface ApiChunkerEvent {
  baseUrl: string;
  fetchPath: string;
  populationType: SyncPopulation;
  bulkReset?: boolean;
  processingMetadata?: {
    processedAt?: string;
    processorVersion?: string;
  };
}

/**
 * The Lambda handler function that dispatches based on event type.
 * 
 * @param event - Either an S3ChunkerEvent or ApiChunkerEvent
 * @returns Response with status code and message
 */
export async function handler(event: S3ChunkerEvent | ApiChunkerEvent): Promise<any> {
  console.log('Received event:', JSON.stringify(event, null, 2));

  // Determine event type by checking for API-specific fields
  if (isApiEvent(event)) {
    console.log('Event type: API-based chunking');
    return await handleApiEvent(event as ApiChunkerEvent);
  } else if (isS3Event(event)) {
    console.log('Event type: S3-based chunking');
    return await handleS3Event(event as S3ChunkerEvent);
  } else {
    const errMsg = 'Invalid event structure: Must contain either (s3Path/bucket/key) or (baseUrl/fetchPath/populationType)';
    console.error(errMsg);
    return { statusCode: 400, body: errMsg };
  }
}

/**
 * Type guard to determine if event is an API-based event
 */
function isApiEvent(event: any): event is ApiChunkerEvent {
  return !!(event.baseUrl && event.fetchPath && event.populationType);
}

/**
 * Type guard to determine if event is an S3-based event
 */
function isS3Event(event: any): event is S3ChunkerEvent {
  return !!(event.s3Path || (event.bucket && event.key));
}
