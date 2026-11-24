/**
 * Tests the scheduled run's definition (src/schedule/ScheduledRun.ts): a full sync with stored hashes trusted, the
 * two previous-state flags set as one choice, and the EventBridge schedule's input that ChunkerService builds from it
 * (lib/services/chunker/ChunkerService.ts). The dashboard's Bulk sync defaults take the same definition
 * (test/DashboardBulkSync.test.ts).
 */
import { isRunStartEvent } from '../src/chunking/RunCounters';
import { ScheduledRun } from '../src/schedule/ScheduledRun';

describe('ScheduledRun', () => {
  it('is a full sync without a bulk reset: stored hashes, then the target for people with none', () => {
    expect(ScheduledRun.POPULATION).toBe('person-full');
    expect(ScheduledRun.PREVIOUS_STATE_SOURCE).toBe('storage-then-target');
  });

  describe('scheduleInput', () => {
    const people = {
      baseUrl: 'https://prod-buprod-fm.snaplogic.io',
      fetchPath: '/api/1/rest/feed/run/task/BUProd/Admin-Integration-Services/GenericGets/huronIRBgetPersonByPopulation'
    };

    it('is the payload the schedule sends: populationType and previousStateSource, startedBy added', () => {
      // As the EventBridge schedule's input is stored: JSON, so an undefined field is left out
      const payload = JSON.parse(JSON.stringify(ScheduledRun.scheduleInput({ ...people, iterationLimit: 0, personRecordProcessorAddons: undefined })));
      expect(payload).toEqual({
        ...people,
        populationType: 'person-full',
        iterationLimit: 0,
        offset: 0,
        previousStateSource: 'storage-then-target',
        startedBy: { by: 'schedule' },
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

    it('reads the real source', () => {
      expect(ScheduledRun.usesRealSource(ScheduledRun.scheduleInput({ ...people, iterationLimit: 0 }))).toBe(true);
    });
  });

  describe('names', () => {
    it("names the landscape's schedule, its role and their ARNs", () => {
      expect(ScheduledRun.scheduleName('Mock')).toBe('chunker-api-schedule-mock');
      expect(ScheduledRun.roleName('Staging')).toBe('chunker-schedule-staging');
      expect(ScheduledRun.roleArn('123456789012', 'mock')).toBe('arn:aws:iam::123456789012:role/chunker-schedule-mock');
      expect(ScheduledRun.functionArn('us-east-2', '123456789012', 'chunker-subscriber-mock'))
        .toBe('arn:aws:lambda:us-east-2:123456789012:function:chunker-subscriber-mock');
    });
  });

  describe('usesRealSource', () => {
    it('is false only for an event carrying the source simulator\'s terms endpoint', () => {
      expect(ScheduledRun.usesRealSource({ baseUrl: 'https://source.example.edu' })).toBe(true);
      expect(ScheduledRun.usesRealSource(undefined)).toBe(true);
      expect(ScheduledRun.usesRealSource({ termsEndpoint: { baseUrl: 'https://sim.lambda-url.us-east-2.on.aws', fetchPath: '/terms' } })).toBe(false);
    });
  });
});
