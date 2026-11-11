/**
 * Tests the scheduled run's definition (src/chunking/ScheduledRun.ts): a full sync with stored hashes trusted, the
 * two previous-state flags set as one choice, and the EventBridge schedule's input that ChunkerService builds from it
 * (lib/services/chunker/ChunkerService.ts). The dashboard's Bulk sync defaults take the same definition
 * (test/DashboardBulkSync.test.ts).
 */
import { isRunStartEvent } from '../src/chunking/RunCounters';
import { ScheduledRun } from '../src/chunking/ScheduledRun';

describe('ScheduledRun', () => {
  it('is a full sync without a bulk reset, stored hashes trusted', () => {
    expect(ScheduledRun.POPULATION).toBe('person-full');
    expect(ScheduledRun.previousStateFlags(ScheduledRun.BULK_RESET)).toEqual({ bulkReset: false, trustPreviousStorage: true });
  });

  it('sets the two previous-state flags as one choice', () => {
    expect(ScheduledRun.previousStateFlags(true)).toEqual({ bulkReset: true, trustPreviousStorage: false });
  });

  describe('scheduleInput', () => {
    const people = {
      baseUrl: 'https://prod-buprod-fm.snaplogic.io',
      fetchPath: '/api/1/rest/feed/run/task/BUProd/Admin-Integration-Services/GenericGets/huronIRBgetPersonByPopulation'
    };

    it('is the payload the schedule sends: only populationType and trustPreviousStorage differ from before', () => {
      // As the EventBridge schedule's input is stored: JSON, so an undefined field is left out
      const payload = JSON.parse(JSON.stringify(ScheduledRun.scheduleInput({ ...people, iterationLimit: 0, personRecordProcessorAddons: undefined })));
      expect(payload).toEqual({
        ...people,
        populationType: 'person-full',
        iterationLimit: 0,
        offset: 0,
        bulkReset: false,
        trustPreviousStorage: true,
        processingMetadata: { processedAt: 'Placeholder. Override at runtime with ISO timestamp', processorVersion: '1.0.0' }
      });
    });

    it("takes the landscape's chunksPerTask as iterationLimit, and addons when there are some", () => {
      const input = ScheduledRun.scheduleInput({ ...people, iterationLimit: 3, personRecordProcessorAddons: 'ORG_COMPARISON_LOGGING' });
      expect(input).toMatchObject({ iterationLimit: 3, personRecordProcessorAddons: 'ORG_COMPARISON_LOGGING' });
      expect(ScheduledRun.scheduleInput({ ...people, iterationLimit: 3 })).not.toHaveProperty('personRecordProcessorAddons');
    });

    it("starts a run, so the chunker subscriber resets the chunker's counters for it", () => {
      expect(isRunStartEvent(ScheduledRun.scheduleInput({ ...people, iterationLimit: 0 }))).toBe(true);
    });
  });
});
