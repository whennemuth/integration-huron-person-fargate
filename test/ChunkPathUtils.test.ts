import { extractChunkBasePath, ISO_TIMESTAMP_PATTERN } from '../src/chunking/filedrop/ChunkPathUtils';

describe('chunkPathUtils', () => {
  describe('ISO_TIMESTAMP_PATTERN', () => {
    it('should match ISO 8601 timestamp', () => {
      const match = '2026-03-03T19:58:41.277Z'.match(ISO_TIMESTAMP_PATTERN);
      expect(match).not.toBeNull();
      expect(match![1]).toBe('2026-03-03T19:58:41.277Z');
    });

    it('should match ISO 8601 timestamp with trailing hyphen', () => {
      const match = '2026-03-03T19:58:41.277Z-'.match(ISO_TIMESTAMP_PATTERN);
      expect(match).not.toBeNull();
      expect(match![1]).toBe('2026-03-03T19:58:41.277Z');
    });

    it('should not match invalid timestamp', () => {
      const match = '2026-03-03'.match(ISO_TIMESTAMP_PATTERN);
      expect(match).toBeNull();
    });
  });

  describe('extractChunkBasePath', () => {
    it('should extract path with ISO timestamp prefix in filename', () => {
      const result = extractChunkBasePath('person-full/2026-03-03T19:58:41.277Z-people.json');
      expect(result).toBe('chunks/person-full/2026-03-03T19:58:41.277Z');
    });

    it('should extract path with ISO timestamp as filename', () => {
      const result = extractChunkBasePath('person-full/2026-03-03T19:58:41.277Z.json');
      expect(result).toBe('chunks/person-full/2026-03-03T19:58:41.277Z');
    });

    it('should extract path without ISO timestamp using filename', () => {
      const result = extractChunkBasePath('person-full/some-file.json');
      expect(result).toBe('chunks/person-full/some-file');
    });

    it('should handle root-level file with timestamp', () => {
      const result = extractChunkBasePath('2026-03-03T19:58:41.277Z-data.json');
      expect(result).toBe('chunks/2026-03-03T19:58:41.277Z');
    });

    it('should handle root-level file without timestamp', () => {
      const result = extractChunkBasePath('data.json');
      expect(result).toBe('chunks/data');
    });

    it('should handle nested directory paths', () => {
      const result = extractChunkBasePath('level1/level2/level3/2026-03-03T19:58:41.277Z-file.json');
      expect(result).toBe('chunks/level1/level2/level3/2026-03-03T19:58:41.277Z');
    });

    it('should handle filename without extension', () => {
      const result = extractChunkBasePath('person-full/datafile');
      expect(result).toBe('chunks/person-full/datafile');
    });

    it('should handle file with multiple dots in name', () => {
      const result = extractChunkBasePath('person-full/my.data.file.json');
      expect(result).toBe('chunks/person-full/my.data.file');
    });

    it('should preserve ISO timestamp with trailing hyphen', () => {
      const result = extractChunkBasePath('person-full/2026-03-03T19:58:41.277Z--people.json');
      expect(result).toBe('chunks/person-full/2026-03-03T19:58:41.277Z');
    });
  });
});
