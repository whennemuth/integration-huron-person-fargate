
import { Message } from '@aws-sdk/client-sqs';

/** The source of person data either is all people, or only those that have changed */
export enum SyncPopulation {
  PersonFull = 'person-full',
  PersonDelta = 'person-delta'
}

/**
 * Shared by both ChunkFromAPI.ts and ChunkFromS3.ts (docker/chunker.ts's two chunking
 * sources) - kept out of docker/chunker.ts itself since that module imports both of those
 * classes, which would otherwise need to import these types back from docker/chunker.ts.
 */
export type IChunkFromSource = {
  runChunking: (params: ChunkFromParams) => Promise<void>
  noMessagesFromQueue?: boolean
  getMessage: () => Message | undefined
  getChunkDirectory: () => string
  getBulkResetFlag?: () => boolean  // Optional getter for bulkReset flag from task parameters
  getTrustPreviousStorageFlag?: () => boolean  // Optional getter for trustPreviousStorage flag from task parameters
  getSyncPopulation?: () => SyncPopulation  // Optional getter for syncPopulation from task parameters
  getUseMockTarget?: () => boolean  // Optional getter for useMockTarget flag from task parameters
  getMockTargetValidateOnly?: () => boolean  // Optional getter for mockTargetValidateOnly flag from task parameters
  getPersonRecordProcessorCustomizations?: () => string | undefined  // Optional getter for personRecordProcessorCustomizations from task parameters
}

export type ChunkFromParams = {
  chunksBucket: string,
  region: string | undefined,
  itemsPerChunk: number,
  personIdField: string,
  bulkReset?: boolean, // To override the bulkReset flag set in the TaskParameters of the chunker instance.
  trustPreviousStorage?: boolean, // Controls whether previous delta storage is trusted for create-vs-patch decisions.
  dryRun: string
}
