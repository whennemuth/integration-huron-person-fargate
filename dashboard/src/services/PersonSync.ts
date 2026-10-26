import { CrudOperation, Status } from 'integration-core';
import { BuCdmPersonDataSource, Config, getDataMapper, HuronPersonDataTarget, PersonPushRequest, SinglePersonSync } from 'integration-huron-person';
import { PersonSyncResult, PersonSyncService } from './ServiceTypes';
import { areTheSame } from '../Utils';

/**
 * Person synchronization implementation using SinglePersonSync
 */
export class PersonSync implements PersonSyncService {
  private static cachedDataMapper: any = null;

  constructor(private config: Config) {  }

  private getSyncInstance = async (personId: string, hrn?: string): Promise<SinglePersonSync> => {
    const { config, config: { preLoadedMaps: { orgMap = false, stateMap = true, countryMap = true } = {} } = {} } = this;

    if (config.dataSource.person) {
      // Remove fieldsOfInterest from config before passing to service, as it will interfere with 
      // getting a full person from the lookup, where we don't want any fields to be filtered out.
      delete config.dataSource.person.fieldsOfInterest;
    }
    
    // Use cached data mapper if available, otherwise create and cache it
    if (!PersonSync.cachedDataMapper) {
      PersonSync.cachedDataMapper = await getDataMapper(config, { orgMap, stateMap, countryMap });
    }
    const dataMapper = PersonSync.cachedDataMapper;
    
    return new SinglePersonSync({ config, buid:personId, dataMapper, hrn });
  }

  /**
   * Get the mapped person data for a given person ID without actually pushing to the target system. 
   * This can be used for previewing the sync result before performing the actual sync.
   * @param personId 
   * @param crudOperation 
   * @returns 
   */
  async preview(personId: string, crudOperation?: CrudOperation): Promise<PersonPushRequest> {
    const syncOp = await this.getSyncInstance(personId);
    const mapped = await syncOp.getMappedPerson({ crudOperation });
    const pushRequest = HuronPersonDataTarget.convertFieldSetToRequest(
      mapped.fieldSets?.[0], 
      crudOperation || CrudOperation.CREATE // Default to CREATE for preview, 99% same as UPDATE.
    );
    return pushRequest;
  }

  /**
   * Get the mapped person data where the rawData is provided directly (no lookup needed). 
   * This can be used for testing to see the output of the data mapper on 
   * an arbitrary set of rawData, formatted as the actual request that would be sent to Huron.
   * @param personId 
   * @param rawData 
   * @param crudOperation 
   */
  async previewExperiment(personId:string, rawData: any[], crudOperation?: CrudOperation): Promise<PersonPushRequest> {
    const syncOp = await this.getSyncInstance(personId);
    syncOp.clearMappingMessages();
    const mapped = await syncOp.getMappedPerson({ rawData, crudOperation });
    const mappingError = syncOp.getMappingError();
    if(mappingError) {
      throw new Error(`Error during mapping: ${mappingError}`);
    }
    const pushRequest = HuronPersonDataTarget.convertFieldSetToRequest(
      mapped.fieldSets?.[0], 
      crudOperation || CrudOperation.CREATE // Default to CREATE for preview, 99% same as UPDATE.
    );
    return pushRequest;
  }
  
  async sync(personId: string, crudOperation?: CrudOperation, hrn?: string): Promise<PersonSyncResult> {
    try {
      /** Create data source instance */
      const dataSource = new BuCdmPersonDataSource({
        config:this.config, buid: personId
      });

      /** Fetch from the data source. */
      const sourceData = await dataSource.fetchRaw();

      /** Validate the response population size (do not proceed further if it is NOT exactly 1) */
      if( sourceData.length === 0 ) {
        throw new Error(`No source records found for personId: ${personId}`);
      }
      if( sourceData.length > 1 ) {
        throw new Error(`Multiple source records found for personId: ${personId}`);
      }

      // Get a sync instance and run it.
      const syncOp = await this.getSyncInstance(personId, hrn);
      syncOp.clearMappingMessages();
      await syncOp.sync({ crudOperation, rawData: sourceData });

      // Check the response first for mapping errors.
      const mappingError = syncOp.getMappingError();
      if(mappingError) {
        throw new Error(`Error during mapping: ${mappingError}`);
      }

      // Check the push result for any errors as well.
      const pushResult = syncOp.getPushResult();
      if(!areTheSame(pushResult.status, Status.SUCCESS)) {
        throw new Error(pushResult.message || 'Unknown error during push operation');
      }

      return {
        personId,
        operation: crudOperation,
        status: 'Success',
        timestamp: new Date().toISOString(),
        recordsProcessed: 1,
        details: `Person (${personId}) synchronization completed successfully`
      } as PersonSyncResult;
    } 
    catch (error) {
      console.error(`Person sync failed for Person ID: ${personId}:`, error);
      return {
        personId,
        operation: crudOperation,
        status: 'Failed',
        timestamp: new Date().toISOString(),
        recordsProcessed: 0,
        details: error instanceof Error ? error.message : String(error)
        // details: `${error instanceof Error ? error.message : String(error)}`
      } as PersonSyncResult;
    }
  }
}