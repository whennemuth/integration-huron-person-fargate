import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { FieldSet, TestEnvironment, Timer } from 'integration-core';
import { ChunkFileManager } from '../chunking/metadata/ChunkFileManager';

export type ChunkPopulationReaderParams = {
  bucketName: string;
  chunkDirectory: string;
  region?: string;
  /** Raw field name identifying a person in chunk records (default: 'personid') */
  personIdField?: string;
};

/**
 * Reads every raw chunk-*.ndjson file written for a sync run and returns the full "current
 * source population" as FieldSets keyed by sourceIdentifier.
 *
 * This is the DynamoDB-mode equivalent of what S3-mode gets "for free" from its consolidated
 * delta file: a complete, unfiltered list of everyone the source fetch actually saw this run.
 * Chunk files are written by the chunker phase for every storage mode, so reading them directly
 * sidesteps PersonCurrentStateTable's syncRunId, which only advances for persons whose hash
 * actually changed (UNCHANGED records are never written) and therefore cannot answer "who was
 * seen this run" on its own.
 */
export class ChunkPopulationReader {
  private readonly chunkFileManager: ChunkFileManager;

  constructor(private params: ChunkPopulationReaderParams) {
    this.chunkFileManager = new ChunkFileManager();
  }

  public async getCurrentPopulation(logChunks:boolean=false): Promise<FieldSet[]> {
    const { bucketName, chunkDirectory, region, personIdField = 'personid' } = this.params;

    const timer = new Timer();
    timer.start();
    const chunkKeys = await this.chunkFileManager.listChunkFiles(bucketName, chunkDirectory, region);
    console.log(`Reading ${chunkKeys.length} chunk file(s) for current population from s3://${bucketName}/${chunkDirectory}/`);

    const s3Client = new S3Client({ region });
    const population: FieldSet[] = [];
    let skippedCount = 0;

    for (const chunkKey of chunkKeys) {
      const response = await s3Client.send(new GetObjectCommand({ Bucket: bucketName, Key: chunkKey }));
      const content = await response.Body?.transformToString();
      const lines = content?.split('\n').filter(line => line.trim().length > 0) || [];
      if (logChunks) {
        console.log(`  Found ${lines.length} line(s) in ${chunkKey}`);
      }

      for (const line of lines) {
        let record: any;
        try {
          record = JSON.parse(line);
        } catch (parseError: any) {
          console.warn(`  ⚠ Failed to parse line in ${chunkKey}: ${parseError.message}`);
          skippedCount++;
          continue;
        }

        const personId = record?.[personIdField];
        if (personId === undefined || personId === null) {
          console.warn(`  ⚠ Record missing '${personIdField}' field in ${chunkKey} - skipping`);
          skippedCount++;
          continue;
        }

        population.push({ fieldValues: [{ sourceIdentifier: String(personId) }] });
      }
    }

    console.log(`  Parsed ${population.length} record(s) from current population${skippedCount > 0 ? ` (${skippedCount} skipped)` : ''}`);
    timer.stop();
    timer.logElapsed(`Time taken to read current population`);
    return population;
  }
}

if (require.main === module) {
  (async () => {
    const testEnvironment = TestEnvironment('CHUNK_POPULATION_READER');
    [
      'CHUNKS_BUCKET',
      'CHUNK_DIRECTORY',
      'REGION',
      'PERSON_ID_FIELD'
    ].forEach(testEnvironment.getVar);

    const {
      CHUNKS_BUCKET: bucketName,
      CHUNK_DIRECTORY: chunkDirectory,
      REGION: region,
      PERSON_ID_FIELD: personIdField
    } = process.env;

    if (!bucketName) {
      console.error('Missing bucket name in configuration!');
      process.exit(1);
    }
    if (!chunkDirectory) {
      console.error('Missing chunk directory in configuration!');
      process.exit(1);
    }

    const reader = new ChunkPopulationReader({ bucketName, chunkDirectory, region, personIdField });
    const population = await reader.getCurrentPopulation();

    console.log(`\nTotal current population: ${population.length}`);
    console.log('Sample:', JSON.stringify(population.slice(0, 5), null, 2));
  })();
}
