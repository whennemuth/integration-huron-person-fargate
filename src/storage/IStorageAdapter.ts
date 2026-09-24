import { Readable } from 'stream';

/**
 * Interface for storage adapters that can read and write files.
 * 
 * This abstraction allows BigJsonFile to work with different storage backends
 * (S3, local filesystem, Azure Blob, etc.) without changing its core logic.
 */
export interface IStorageAdapter {
  /**
   * Get a readable stream for a file from storage.
   * 
   * @param key - The storage key/path of the file to read
   * @returns Promise resolving to a readable stream
   * @throws Error if file doesn't exist or can't be read
   */
  getReadStream(key: string): Promise<Readable>;

  /**
   * Write content to storage.
   * 
   * @param key - The storage key/path where content should be written
   * @param content - The content to write (string or buffer)
   * @param contentType - Optional content type/mime type
   * @returns Promise that resolves when write is complete
   * @throws Error if write fails
   */
  writeFile(key: string, content: string | Buffer, contentType?: string): Promise<void>;
}
