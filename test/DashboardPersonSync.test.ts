import { CrudOperation, Status } from 'integration-core';
import { Config } from 'integration-huron-person';
import { PersonSync } from '../dashboard/src/services/PersonSync';

const fetchRaw = jest.fn();
const sync = jest.fn();
const getMappedPerson = jest.fn();
let pushResult: any;
const constructed: any[] = [];

jest.mock('integration-huron-person', () => {
  const actual = jest.requireActual('integration-huron-person');
  return {
    ...actual,
    BuCdmPersonDataSource: jest.fn().mockImplementation(() => ({ fetchRaw: (...args: any[]) => fetchRaw(...args) })),
    getDataMapper: jest.fn().mockResolvedValue({ name: 'data mapper' }),
    SinglePersonSync: jest.fn().mockImplementation((params: any) => {
      constructed.push(params);
      return {
        sync: (...args: any[]) => sync(...args),
        getMappedPerson: (...args: any[]) => getMappedPerson(...args),
        clearMappingMessages: jest.fn(),
        getMappingError: () => undefined,
        getPushResult: () => pushResult
      };
    })
  };
});

/**
 * The dashboard's person sync (Lookup and sync's Sync now): always pushed, and the person's hash recorded after a
 * successful push in DynamoDB storage, as a manual SyncPerson with SYNC_UPDATE_HASH=true does
 */
describe('PersonSync', () => {
  const config = (storageType: string) => ({
    dataSource: { person: { fieldsOfInterest: ['personid'] } },
    storage: { type: storageType, config: {} }
  }) as unknown as Config;

  beforeEach(() => {
    jest.clearAllMocks();
    constructed.length = 0;
    fetchRaw.mockResolvedValue([{ personid: 'U12345678' }]);
    pushResult = { status: Status.SUCCESS, message: 'pushed' };
  });

  describe('sync (Sync now)', () => {
    it('records the hash in DynamoDB storage, without trusting any stored hash', async () => {
      const result = await new PersonSync(config('dynamodb')).sync('U12345678', CrudOperation.UPDATE, 'hrn:hrs:persons:1');

      expect(result.status).toBe('Success');
      expect(constructed).toEqual([expect.objectContaining({
        buid: 'U12345678',
        hrn: 'hrn:hrs:persons:1',
        hashStorage: { enabled: true, previousStateSource: 'target' }
      })]);
    });

    it('always pushes: the sync is forced, so no in-sync check can skip it', async () => {
      await new PersonSync(config('dynamodb')).sync('U12345678', CrudOperation.UPDATE);

      expect(sync).toHaveBeenCalledWith({
        crudOperation: CrudOperation.UPDATE, rawData: [{ personid: 'U12345678' }], forceUpdate: true
      });
    });

    it('records no hash outside DynamoDB storage (a baseline file would be rewritten whole)', async () => {
      await new PersonSync(config('s3')).sync('U12345678', CrudOperation.CREATE);

      expect(constructed[0].hashStorage).toBeUndefined();
      expect(sync).toHaveBeenCalledWith(expect.objectContaining({ forceUpdate: true }));
    });

    it('reports a failed push', async () => {
      pushResult = { status: Status.FAILURE, message: 'Huron said no' };

      const result = await new PersonSync(config('dynamodb')).sync('U12345678', CrudOperation.UPDATE);

      expect(result.status).not.toBe('Success');
    });
  });

  describe('preview (Sync Preview)', () => {
    it('neither reads nor records hash storage', async () => {
      getMappedPerson.mockResolvedValue({ fieldSets: [{ fieldValues: [{ id: 'U12345678' }] }] });

      await new PersonSync(config('dynamodb')).preview('U12345678', CrudOperation.UPDATE);

      expect(constructed[0].hashStorage).toBeUndefined();
      expect(sync).not.toHaveBeenCalled();
    });
  });
});
