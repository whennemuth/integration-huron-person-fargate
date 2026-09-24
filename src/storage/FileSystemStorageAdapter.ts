import * as fs from 'fs';
import * as path from 'path';
import { Readable } from 'stream';
import { promisify } from 'util';
import { IStorageAdapter } from './IStorageAdapter';

const mkdir = promisify(fs.mkdir);
const writeFile = promisify(fs.writeFile);
const access = promisify(fs.access);

/**
 * Configuration for filesystem storage adapter
 */
export interface FileSystemStorageAdapterConfig {
  /** Base directory path where files will be stored */
  basePath: string;
}

/**
 * Storage adapter implementation for local filesystem.
 * 
 * Maps storage keys (S3-style paths like "data/people/chunk-0000.ndjson")
 * to filesystem paths relative to a base directory. Automatically creates
 * subdirectories as needed.
 * 
 * @example
 * ```typescript
 * const adapter = new FileSystemStorageAdapter({ basePath: './data' });
 * // Key "data/people/chunk-0000.ndjson" -> "./data/data/people/chunk-0000.ndjson"
 * const stream = await adapter.getReadStream('people.json');
 * ```
 */
export class FileSystemStorageAdapter implements IStorageAdapter {
  private readonly basePath: string;

  constructor(config: FileSystemStorageAdapterConfig) {
    this.basePath = path.resolve(config.basePath);
  }

  /**
   * Get a readable stream from a local file.
   * 
   * @param key - Relative path to the file (e.g., "data/people.json")
   * @returns Readable stream
   * @throws Error if file doesn't exist or can't be read
   */
  async getReadStream(key: string): Promise<Readable> {
    const filePath = this.resolveFilePath(key);
    
    // Check if file exists
    try {
      await access(filePath, fs.constants.R_OK);
    } catch (error) {
      throw new Error(`File not found or not readable: ${filePath}`);
    }

    // Return binary stream (no encoding) - stream-json expects Buffer objects
    return fs.createReadStream(filePath);
  }

  /**
   * Write content to a local file.
   * 
   * Creates parent directories automatically if they don't exist.
   * 
   * @param key - Relative path where file should be written
   * @param content - Content to write
   * @param contentType - Ignored for filesystem (kept for interface compatibility)
   */
  async writeFile(key: string, content: string | Buffer, contentType?: string): Promise<void> {
    const filePath = this.resolveFilePath(key);
    const directory = path.dirname(filePath);

    // Create directory if it doesn't exist
    try {
      await mkdir(directory, { recursive: true });
    } catch (error: any) {
      // Ignore error if directory already exists
      if (error.code !== 'EEXIST') {
        throw new Error(`Failed to create directory ${directory}: ${error.message}`);
      }
    }

    // Write the file
    try {
      await writeFile(filePath, content, 'utf8');
    } catch (error: any) {
      throw new Error(`Failed to write file ${filePath}: ${error.message}`);
    }
  }

  /**
   * Resolve a storage key to an absolute filesystem path.
   * 
   * @param key - Storage key (relative path)
   * @returns Absolute filesystem path
   */
  private resolveFilePath(key: string): string {
    // Normalize the key to handle both forward and backslashes
    const normalizedKey = key.replace(/\\/g, '/');
    return path.join(this.basePath, normalizedKey);
  }
}
