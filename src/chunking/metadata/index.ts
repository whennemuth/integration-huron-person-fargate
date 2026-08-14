/**
 * Metadata management module for chunk pipeline.
 * 
 * Exports abstract base class, concrete implementations, and factory for
 * managing metadata across different storage backends (S3, DynamoDB).
 * 
 * ## Usage:
 * 
 * ```typescript
 * import { MetadataFactory, WriteMetadataParams } from './metadata';
 * 
 * const metadata = MetadataFactory.create({ config });
 * await metadata.write({ chunkDirectory, itemsPerChunk, source, ...flags });
 * ```
 * 
 * ## Exports:
 * - Types: Flags, ChunkMetadata, WriteMetadataParams, etc.
 * - Abstract: AbstractMetadata
 * - Implementations: MetadataForS3, MetadataForDynamoDb
 * - Factory: MetadataFactory
 */

// Export all types and interfaces
export type {
  Flags,
  ChunkMetadata,
  WriteMetadataParams,
  ReadMetadataParams,
  WriteFlagsParams,
  ReadFlagsParams,
  MarkRunFailedParams,
  ReadTerminalErrorParams,
  TerminalError
} from './AbstractMetadata';

// Export abstract base class
export { AbstractMetadata } from './AbstractMetadata';

// Export concrete implementations
export { MetadataForS3 } from './MetadataForS3';
export { MetadataForDynamoDb } from './MetadataForDynamoDb';

// Export factory
export { MetadataFactory } from './MetadataFactory';

// Export processor factory helper
export { getMetadataManager } from './ProcessorMetadataFactory';
