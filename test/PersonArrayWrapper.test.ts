import { PersonArrayWrapper, IStorageReader } from '../src/chunking/PersonArrayWrapper';
import { Readable } from 'stream';

/**
 * Mock storage reader for testing
 */
class MockStorageReader implements IStorageReader {
  constructor(private content: string, private chunkSize?: number) {}

  async getReadStream(_key: string): Promise<Readable> {
    if (this.chunkSize) {
      // Emit data in chunks
      let remainingContent = this.content;
      const chunkSize = this.chunkSize;
      
      return new Readable({
        read() {
          if (remainingContent.length > 0) {
            const chunk = remainingContent.substring(0, chunkSize);
            remainingContent = remainingContent.substring(chunkSize);
            this.push(chunk);
          } else {
            this.push(null);
          }
        }
      });
    }
    return Readable.from([this.content]);
  }
}

describe('PersonArrayWrapper', () => {
  describe('detectPersonArrayPath', () => {
    describe('top-level array patterns', () => {
      it('should return undefined for top-level array with personid first', async () => {
        const json = '[{"personid":"U001","name":"Alice"}]';
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBeUndefined();
      });

      it('should return undefined for top-level array with personid last', async () => {
        const json = '[{"name":"Alice","email":"alice@test.com","personid":"U001"}]';
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBeUndefined();
      });

      it('should return undefined for top-level array with whitespace', async () => {
        const json = ` 
        [
          { 
            "personid" : "U001" 
          }
        ]`;
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBeUndefined();
      });
    });

    describe('nested array patterns', () => {
      it('should detect nested array at 0.response', async () => {
        const json = '[{"response_code":200,"response":[{"personid":"U001"}]}]';
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBe('0.response');
      });

      it('should detect nested array with different property name', async () => {
        const json = '[{"status":"ok","data":[{"personid":"U001"}]}]';
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBe('0.data');
      });

      it('should detect nested array with whitespace', async () => {
        const json = `[
          {
            "response_code": 200,
            "response": [
              { "personid": "U001" }
            ]
          }
        ]`;
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBe('0.response');
      });

      it('should detect nested array when personid appears later in object', async () => {
        const json = '[{"response_code":200,"response":[{"name":"Alice","age":30,"personid":"U001"}]}]';
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBe('0.response');
      });
    });

    describe('custom personid field', () => {
      it('should detect with custom field name', async () => {
        const json = '[{"response":[{"userId":"U001"}]}]';
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'userId');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBe('0.response');
      });

      it('should return undefined when custom field is at top-level', async () => {
        const json = '[{"userId":"U001","name":"Alice"}]';
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'userId');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBeUndefined();
      });
    });

    describe('edge cases', () => {
      it('should return undefined when personid not found in sample', async () => {
        const json = '[{"name":"Alice","email":"alice@test.com"}]';
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBeUndefined();
      });

      it('should return undefined for empty file', async () => {
        const json = '';
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBeUndefined();
      });

      it('should return undefined for empty array', async () => {
        const json = '[]';
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBeUndefined();
      });

      it('should handle stream errors gracefully', async () => {
        const storage: IStorageReader = {
          async getReadStream() {
            const stream = new Readable({
              read() {
                this.emit('error', new Error('Read error'));
              }
            });
            return stream;
          }
        };
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBeUndefined();
      });

      it('should handle malformed JSON gracefully', async () => {
        const json = '[{"response_code":200,"response":[{"personid"';
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        // Malformed JSON might not match the nested pattern, returns undefined
        expect(result).toBeUndefined();
      });
    });

    describe('detection byte limits', () => {
      it('should respect custom maxDetectionBytes', async () => {
        // Create a large JSON where personid appears after 100 bytes
        const largePreamble = '[{"description":"' + 'x'.repeat(200) + '","response":[{"personid":"U001"}]}]';
        const storage = new MockStorageReader(largePreamble, 10); // Emit in 10-byte chunks
        const wrapper = new PersonArrayWrapper(storage, 'personid', 50); // Only read 50 bytes

        const result = await wrapper.detectPersonArrayPath('test.json');

        // Should return undefined because personid not in first 50 bytes
        expect(result).toBeUndefined();
      });

      it('should work with larger byte limits', async () => {
        const json = '[{"response":[{"personid":"U001"}]}]' + 'x'.repeat(20000);
        const storage = new MockStorageReader(json, 100); // Emit in 100-byte chunks
        const wrapper = new PersonArrayWrapper(storage, 'personid', 100);

        const result = await wrapper.detectPersonArrayPath('test.json');

        // Should detect even though file is larger - stops after 100 bytes which includes the pattern
        expect(result).toBe('0.response');
      });
    });

    describe('complex real-world structures', () => {
      it('should handle BU person API response structure', async () => {
        const json = `[
          {
            "response_code": 200,
            "response": [
              {
                "personid": "U43906143",
                "personBasic": {
                  "names": [
                    {"nameType": "PRF", "firstName": "Kayla"}
                  ]
                },
                "enrollmentInfo": []
              }
            ]
          }
        ]`;
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBe('0.response');
      });

      it('should handle nested objects before the array', async () => {
        const json = `[
          {
            "metadata": {
              "timestamp": "2026-03-18",
              "version": "1.0"
            },
            "persons": [
              {"personid": "U001"}
            ]
          }
        ]`;
        const storage = new MockStorageReader(json);
        const wrapper = new PersonArrayWrapper(storage, 'personid');

        const result = await wrapper.detectPersonArrayPath('test.json');

        expect(result).toBe('0.persons');
      });
    });
  });
});
