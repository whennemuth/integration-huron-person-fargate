/**
 * Chunk Path Utilities
 * 
 * Shared utilities for deriving chunk directory paths from input S3 keys.
 * Used by both the chunker (Phase 1) and merger (Phase 3) to ensure
 * consistent path generation.
 */

/**
 * ISO 8601 timestamp pattern with optional hyphen separator
 * Matches: YYYY-MM-DDTHH:mm:ss.SSSZ or YYYY-MM-DDTHH:mm:ss.SSSZ-
 */
export const ISO_TIMESTAMP_PATTERN = /(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)-?/;

/**
 * Extracts the chunk directory path from an input key.
 * If the filename has an ISO timestamp prefix, includes it in the path.
 * Otherwise, uses the full key path without the file extension.
 * 
 * Examples:
 * - 'person-full/2026-03-03T19:58:41.277Z-people.json' -> 'chunks/person-full/2026-03-03T19:58:41.277Z'
 * - 'person-full/2026-03-03T19:58:41.277Z.json' -> 'chunks/person-full/2026-03-03T19:58:41.277Z'
 * - 'person-full/some-file.json' -> 'chunks/person-full/some-file'
 * - 'data.json' -> 'chunks/data'
 * 
 * @param inputKey - The S3 key of the input file
 * @returns The chunk directory path with 'chunks/' prefix
 */
export function extractChunkBasePath(inputKey: string): string {
  const lastSlashIndex = inputKey.lastIndexOf('/');
  const directory = lastSlashIndex >= 0 ? inputKey.substring(0, lastSlashIndex) : '';
  const filename = lastSlashIndex >= 0 ? inputKey.substring(lastSlashIndex + 1) : inputKey;
  
  // Check if filename starts with ISO timestamp
  const timestampMatch = filename.match(ISO_TIMESTAMP_PATTERN);
  
  if (timestampMatch) {
    // Has ISO timestamp prefix - use directory + timestamp
    const timestamp = timestampMatch[1];
    return directory ? `chunks/${directory}/${timestamp}` : `chunks/${timestamp}`;
  } else {
    // No ISO timestamp - use directory + filename without extension
    const filenameWithoutExt = filename.replace(/\.[^.]+$/, '');
    return directory ? `chunks/${directory}/${filenameWithoutExt}` : `chunks/${filenameWithoutExt}`;
  }
}
