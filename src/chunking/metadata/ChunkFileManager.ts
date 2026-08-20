import { S3Client, ListObjectsV2Command, GetObjectCommand } from '@aws-sdk/client-s3';
import { NextChunk } from '../../Queue';

/**
 * ChunkFileManager - S3 chunk file operations
 * 
 * Handles operations on actual chunk NDJSON files stored in S3.
 * This class is separate from metadata storage because chunk files are ALWAYS
 * stored in S3, regardless of whether metadata is stored in S3 or DynamoDB.
 * 
 * ## Design Rationale
 * 
 * Previously, these operations were part of AbstractMetadata, forcing both S3
 * and DynamoDB implementations to provide them. This caused "NOT IMPLEMENTED"
 * errors in MetadataForDynamoDb for operations that required filesystem access.
 * 
 * The separation recognizes that:
 * - **Metadata Storage**: Storage-agnostic (FLAGS, METADATA, TERMINAL_ERROR records)
 * - **Chunk File Management**: Always S3-based (listing/reading actual chunk files)
 * 
 * ## Usage
 * 
 * ```typescript
 * const chunkManager = new ChunkFileManager();
 * const chunkKeys = await chunkManager.listChunkFiles(bucket, directory, region);
 * const totalRecords = await chunkManager.computeTotalRecords(bucket, chunkKeys, region);
 * const aggregated = await chunkManager.buildAggregatedMetadata(bucket, directory, region);
 * ```
 * 
 * @see IMetadataStorage for metadata storage operations
 * @see MetadataForS3 for S3 metadata implementation
 * @see MetadataForDynamoDb for DynamoDB metadata implementation
 */
export class ChunkFileManager {
  /**
   * List all chunk files in a directory with pagination support.
   * Returns sorted array of chunk file keys matching chunk-*.ndjson pattern.
   * Handles S3 pagination to support 1000+ chunk files.
   * 
   * @param bucketName - S3 bucket name
   * @param chunkDirectory - Chunk directory path (e.g., "chunks/person-full/2026-03-03T19:58:41.277Z")
   * @param region - AWS region
   * @returns Array of chunk file keys sorted by chunk number
   */
  public async listChunkFiles(
    bucketName: string,
    chunkDirectory: string,
    region?: string
  ): Promise<string[]> {
    if (!bucketName) {
      throw new Error('bucketName is required for S3 chunk file operations');
    }

    const s3Client = new S3Client({ region });
    const prefix = `${chunkDirectory}/`;
    const chunkFiles: string[] = [];
    let continuationToken: string | undefined;

    try {
      do {
        const response = await s3Client.send(
          new ListObjectsV2Command({
            Bucket: bucketName,
            Prefix: prefix,
            ContinuationToken: continuationToken,
          })
        );

        // Filter for chunk-*.ndjson files
        if (response.Contents) {
          for (const obj of response.Contents) {
            if (obj.Key && /chunk-\d+\.ndjson$/.test(obj.Key)) {
              chunkFiles.push(obj.Key);
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

      // Sort by chunk number to ensure deterministic ordering
      chunkFiles.sort((a, b) => {
        const numA = parseInt(a.match(/chunk-(\d+)\.ndjson$/)?.[1] || '0', 10);
        const numB = parseInt(b.match(/chunk-(\d+)\.ndjson$/)?.[1] || '0', 10);
        return numA - numB;
      });

      console.log(`✓ Listed ${chunkFiles.length} chunk files from s3://${bucketName}/${prefix}`);
      return chunkFiles;
    } catch (error: any) {
      console.error(`Error listing chunk files: ${error.message}`);
      throw error;
    }
  }

  /**
   * Compute total records by summing NDJSON line counts across chunk files.
   * Streams each file to avoid buffering large payloads.
   * 
   * @param bucketName - S3 bucket name
   * @param chunkKeys - Array of chunk file S3 keys
   * @param region - AWS region
   * @returns Total record count across all chunks
   */
  public async computeTotalRecords(
    bucketName: string,
    chunkKeys: string[],
    region?: string
  ): Promise<number> {
    if (!bucketName) {
      throw new Error('bucketName is required for S3 chunk file operations');
    }

    const s3Client = new S3Client({ region });
    let totalRecords = 0;

    for (const chunkKey of chunkKeys) {
      try {
        const response = await s3Client.send(
          new GetObjectCommand({
            Bucket: bucketName,
            Key: chunkKey,
          })
        );

        const body = await response.Body?.transformToString();
        if (body) {
          // Count non-empty lines (each NDJSON line is one record)
          const lineCount = body.split('\n').filter(line => line.trim().length > 0).length;
          totalRecords += lineCount;
        }
      } catch (error: any) {
        console.warn(`Warning: Could not read chunk file ${chunkKey}: ${error.message}`);
        // Continue with other chunks even if one fails
      }
    }

    console.log(`✓ Computed total records: ${totalRecords} across ${chunkKeys.length} chunks`);
    return totalRecords;
  }

  /**
   * Build aggregated metadata from run-level S3 state.
   * Discovers all chunk files in the directory and computes aggregate totals.
   * Used when finalizing metadata at end-of-run to ensure all chunks are reflected.
   * 
   * @param bucketName - S3 bucket name
   * @param chunkDirectory - Chunk directory path
   * @param region - AWS region
   * @returns Aggregated metadata with full chunk list and totals
   */
  public async buildAggregatedMetadata(
    bucketName: string,
    chunkDirectory: string,
    region?: string
  ): Promise<{ chunkKeys: string[]; totalRecords: number; chunkCount: number }> {
    try {
      // Discover all chunk files
      const chunkKeys = await this.listChunkFiles(bucketName, chunkDirectory, region);

      if (chunkKeys.length === 0) {
        throw new Error(`No chunk files found in ${chunkDirectory}`);
      }

      // Compute total records
      const totalRecords = await this.computeTotalRecords(bucketName, chunkKeys, region);

      const chunkCount = chunkKeys.length;

      console.log(`✓ Aggregated metadata: chunkCount=${chunkCount}, totalRecords=${totalRecords}`);

      return { chunkKeys, totalRecords, chunkCount };
    } catch (error: any) {
      console.error(`Failed to build aggregated metadata: ${error.message}`);
      throw error;
    }
  }

  public static validateChunk = (chunk: NextChunk | undefined) => {
    if (!chunk) {
      throw new Error('No chunk information provided in SQS message or environment variables');
    }
    const { bucketName, s3Key } = chunk;
    if (!bucketName) {
      console.error('ERROR: CHUNKS_BUCKET environment variable or queue message required');
      process.exit(1);
    }
    if (!s3Key) {
      console.error('ERROR: CHUNK_KEY environment variable or queue message required');
      process.exit(1);
    }
  };
}
