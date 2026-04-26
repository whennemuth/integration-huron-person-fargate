
import { Readable } from 'stream';

export type IPersonArrayWrapper = {
  detectPersonArrayPath(key: string): Promise<string | undefined>;
};

/**
 * Interface for storage operations needed by PersonArrayWrapper
 */
export interface IStorageReader {
  getReadStream(key: string): Promise<Readable>;
}

/**
 * Detects the JSON path to arrays containing person records.
 * 
 * This class analyzes the structure of JSON files to determine where
 * person records are located, enabling memory-efficient streaming.
 * 
 * Detection Strategy:
 * - Reads first 10KB of file as raw text (no full parsing)
 * - Searches for personid field location
 * - Analyzes JSON structure pattern before personid
 * - Returns path string for use with stream-json's pick() filter
 * 
 * @example
 * ```typescript
 * const wrapper = new PersonArrayWrapper(storage, 'personid');
 * const path = await wrapper.detectPersonArrayPath('data.json');
 * // path = '0.response' for [{response: [persons]}]
 * // path = undefined for top-level arrays [person, person, ...]
 * ```
 */
export class PersonArrayWrapper implements IPersonArrayWrapper {
  private readonly storage: IStorageReader;
  private readonly personIdField: string;
  private readonly maxDetectionBytes: number;

  /**
   * @param storage - Storage adapter for reading files
   * @param personIdField - Field name that identifies person objects (default: 'personid')
   * @param maxDetectionBytes - Maximum bytes to read for detection (default: 10KB)
   */
  constructor(
    storage: IStorageReader, 
    personIdField: string = 'personid',
    maxDetectionBytes: number = 10 * 1024
  ) {
    this.storage = storage;
    this.personIdField = personIdField;
    this.maxDetectionBytes = maxDetectionBytes;
  }

  /**
   * Detects the JSON path to the array containing person records.
   * 
   * Reads the beginning of the file and analyzes patterns:
   * - Top-level array: `[{"personid": ...}]` → returns undefined
   * - Nested array: `[{"response": [{"personid": ...}]}]` → returns '0.response'
   * 
   * @param key - Storage key of the file to analyze
   * @returns Path string (e.g., '0.response') or undefined if top-level/indeterminate
   */
  async detectPersonArrayPath(key: string | undefined): Promise<string | undefined> {
    console.log(`Detecting person array path for: ${key}`);
    return new Promise(async (resolve) => {
      try {
        if( ! key) {
          console.log('detectPersonArrayPath: Cancelled - no key provided');
          resolve(undefined);
          return;
        }
        const stream = await this.storage.getReadStream(key);
        let rawData = '';

        stream.on('data', (chunk: Buffer) => {
          rawData += chunk.toString('utf8');
          
          if (rawData.length >= this.maxDetectionBytes) {
            stream.destroy();
            this.analyzeStructure(rawData, resolve);
          }
        });

        stream.on('end', () => {
          this.analyzeStructure(rawData, resolve);
        });

        stream.on('error', (error: Error) => {
          console.error(`Error reading stream during detection for ${key}:`, error.message);
          resolve(undefined);
        });

      } catch (error) {
        console.error(`Error initiating detection stream for ${key}:`, error);
        resolve(undefined);
      }
    });
  }

  /**
   * Analyzes the raw JSON text to determine structure pattern.
   * 
   * @param rawData - Raw JSON text from beginning of file
   * @param resolve - Promise resolver to call with result
   */
  private analyzeStructure(
    rawData: string, 
    resolve: (value: string | undefined) => void
  ): void {
    try {
      // Look for the personid field in the raw JSON
      const personidMatch = rawData.match(new RegExp(`"${this.personIdField}"\\s*:`));
      if (!personidMatch) {
        // No personid found in sample, can't determine structure
        resolve(undefined);
        return;
      }

      const beforePersonid = rawData.substring(0, personidMatch.index);

      // Check if personid is in a top-level array element
      // Pattern: [{"personid": ... or [{ "personid": ... (whitespace variations)
      const topLevelPattern = /^\s*\[\s*\{\s*$/;
      if (topLevelPattern.test(beforePersonid.trim())) {
        // Top-level array of persons, no nesting - use flexible mode
        resolve(undefined);
        return;
      }

      // Look for nested pattern: property name right before the array
      // Match: "propertyName":[{ ... where personid appears after
      const nestedMatch = beforePersonid.match(/"([^"]+)"\s*:\s*\[\s*\{[^}]*$/);
      if (nestedMatch) {
        const propertyName = nestedMatch[1];
        // Assume it's in the first element of top-level array (most common case)
        resolve(`0.${propertyName}`);
        return;
      }

      // Couldn't determine structure clearly - use flexible mode
      resolve(undefined);
    } catch (error) {
      resolve(undefined);
    }
  }
}

/**
 * Example usage / test harness - can be run with `ts-node src/PersonArrayWrapper.ts` 
 * to test the detection operation with different storage backends.
 */
if (require.main === module) {
  // Import storage adapters (only needed for test harness)
  const { FileSystemStorageAdapter } = require('../storage/FileSystemStorageAdapter');
  const { S3StorageAdapter } = require('../storage/S3StorageAdapter');

  (async () => {
    console.log('=== PersonArrayWrapper Test Harness ===\n');

    const { MODE, PERSON_ID_FIELD='personid', MAX_DETECTION_BYTES='10240' } = process.env;
    const personIdField = PERSON_ID_FIELD;
    const maxDetectionBytes = parseInt(MAX_DETECTION_BYTES, 10);

    switch (MODE) {
      case 'filesystem':
        console.log('Running in FILESYSTEM test mode');
        const { FILE_BASE_PATH:basePath='./test-data', FILE_NAME:fileName } = process.env;
        if( ! fileName) {
          console.error('FILE_NAME environment variable is required for FILESYSTEM test mode');
          return;
        }

        // Instantiate wrapper and its parameters
        const fsStorage = new FileSystemStorageAdapter({ basePath });
        const fsWrapper = new PersonArrayWrapper(fsStorage, personIdField, maxDetectionBytes);
        
        // Run the detection
        try {
          const path = await fsWrapper.detectPersonArrayPath(fileName);
          if (path) {
            console.log(`✓ Detected person array path: "${path}"`);
          } else {
            console.log(`✓ Detected top-level array (path: undefined)`);
          }
        } catch (error: any) {
          console.error(`✗ Error: ${error.message}`);
        }
        break;
      case 's3':
        console.log('Running in S3 test mode');
        const { INPUT_BUCKET:bucketName, INPUT_KEY:s3Key, REGION:region } = process.env;
        if( ! bucketName) {
          console.error('INPUT_BUCKET environment variable is required for S3 test mode');
          return;
        }
        if( ! s3Key) {
          console.error('INPUT_KEY environment variable is required for S3 test mode');
          return;
        }
        
        // Instantiate wrapper and its parameters
        const s3Storage = new S3StorageAdapter({ bucketName, region });
        const s3Wrapper = new PersonArrayWrapper(s3Storage, personIdField, maxDetectionBytes);
        
        // Run the detection
        try {
          const path = await s3Wrapper.detectPersonArrayPath(s3Key);
          if (path) {
            console.log(`✓ Detected person array path: "${path}"`);
          } else {
            console.log(`✓ Detected top-level array (path: undefined)`);
          }
        } catch (error: any) {
          console.error(`✗ Error: ${error.message}`);
        }
        break;
      default:
        console.log('No valid MODE specified ("filesystem" or "s3"), skipping test harness execution');
    }

    console.log('\n---\n');
  })();
}




