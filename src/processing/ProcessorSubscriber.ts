import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';

const {
  PROCESSOR_QUEUE_URL,
  REGION: region,
  PAUSE_MESSAGING = 'false',
  DRY_RUN = 'false',
} = process.env;

const sqsClient = new SQSClient({ region });

/**
 * Forwards chunk S3 event notifications to the processor SQS queue.
 * The outgoing message body intentionally remains identical to direct S3->SQS delivery.
 */
export async function handler(event: any): Promise<any> {
  const pauseMessaging = PAUSE_MESSAGING.toLowerCase() === 'true';
  const dryRun = DRY_RUN.toLowerCase() === 'true';

  if (pauseMessaging) {
    console.log('⊘ PAUSE_MESSAGING=true; skipping processor queue message creation.');
    return { statusCode: 200, body: 'Paused: message creation skipped' };
  }

  if (dryRun) {
    console.log('⊘ DRY_RUN=true; would forward S3 event to processor queue.');
    return { statusCode: 200, body: 'Dry run: message not sent' };
  }

  if (!PROCESSOR_QUEUE_URL) {
    console.error('Missing required environment variable: PROCESSOR_QUEUE_URL');
    return { statusCode: 500, body: 'Missing PROCESSOR_QUEUE_URL' };
  }

  try {
    // Keep message body identical to prior direct S3->SQS event payload shape.
    const msgParams = {
      QueueUrl: PROCESSOR_QUEUE_URL,
      MessageBody: JSON.stringify(event),
    };
    console.log('Forwarding S3 event to processor queue:', JSON.stringify(msgParams));
    await sqsClient.send(new SendMessageCommand(msgParams));

    const recordCount = Array.isArray(event?.Records) ? event.Records.length : 0;
    console.log(`✓ Forwarded S3 event to processor queue. Records=${recordCount}`);
    return { statusCode: 200, body: 'Message forwarded' };
  } catch (error) {
    console.error('✗ Failed to send message to processor queue:', error);
    throw error;
  }
}