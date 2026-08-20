/**
 * Metadata management module for chunk pipeline.
 * 
 * Exports interface, concrete implementations, and factory for
 * managing metadata across different storage backends (S3, DynamoDB).
 * 
 * ## Usage:
 * 
 * ```typescript
 * import { MetadataFactory, WriteMetadataParams, ChunkFileManager } from './metadata';
 * 
 * // Metadata storage operations
 * const metadata = MetadataFactory.create({ config });
 * await metadata.write({ chunkDirectory, itemsPerChunk, source, ...flags });
 * 
 * // Chunk file operations (always S3-based)
 * const chunkManager = new ChunkFileManager();
 * const chunkKeys = await chunkManager.listChunkFiles(bucket, chunkDirectory, region);
 * ```
 * 
 * ## Exports:
 * - Types: Flags, ChunkMetadata, WriteMetadataParams, etc.
 * - Interface: IMetadataStorage
 * - Implementations: MetadataForS3, MetadataForDynamoDb
 * - Factory: MetadataFactory
 * - ChunkFileManager: For S3 chunk file operations
 * - Utilities: MetadataUtilsImpl, validateMetadata
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
} from './IMetadataStorage';
export type { MetadataUtils } from './MetadataUtils';

// Export interface
export { IMetadataStorage } from './IMetadataStorage';

// Export utilities
export { StandardMetadataUtils, validateMetadata } from './MetadataUtils';

// Export concrete implementations
export { MetadataForS3 } from './MetadataForS3';
export { MetadataForDynamoDb } from './MetadataForDynamoDb';

// Export factory
export { MetadataFactory, MetadataFactoryForBootstrap } from './MetadataFactory';

// Export chunk file manager
export { ChunkFileManager } from './ChunkFileManager';
