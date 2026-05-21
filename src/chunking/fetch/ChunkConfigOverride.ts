import { Config, DataSourceConfig } from "integration-huron-person";
import { TaskParameters } from "./ChunkFromAPI";

/**
 * The chunking process may have consumed an SQS message that specifies a baseUrl and/or
 * fetchPath that are different from the ones configured in the standard config file. If
 * so, this class will override the config values with the ones from the message parameters
 * so that the chunking process will use the correct endpoint and apiKey.
 * 
 * The TaskParameters parameter contains the fields extracted from the SQS message. These
 * values may differ from the config file in testing scenarios where we want to point to 
 * different endpoints.
 */
export class ChunkConfigOverride {
  private overrodeConfig:boolean = false;
  private peopleConfig: DataSourceConfig;
  private personConfig: DataSourceConfig;
  private messages: string[] = [];

  constructor(private config: Config, private taskParameters: TaskParameters) {
    const { dataSource: { people = {}, person = {} } = {} } = this.config;
    this.peopleConfig = people as DataSourceConfig;
    this.personConfig = person as DataSourceConfig;
  } 

  private overrideConfig = (cfg:string|undefined, msg:string|undefined): boolean => {
    if(cfg && cfg !== 'from_config') {
      if(msg && msg !== cfg) {
        this.overrodeConfig = true;
        return true;
      }
    }
    return false;
  }

  public getOverridenConfig = (): Config => {
    try {
      const { 
        overrideConfig,
        taskParameters: { baseUrl, fetchPath } = {},
        peopleConfig: { fetchPath: configFetchPath, endpointConfig: { baseUrl: configBaseUrl, apiKey } = {} } = {} 
      } = this;

      const shouldOverrideBaseUrl = overrideConfig(configBaseUrl, baseUrl);
      const shouldOverrideFetchPath = overrideConfig(configFetchPath, fetchPath);

      const effectiveBaseUrl = shouldOverrideBaseUrl ? baseUrl! : configBaseUrl!;
      const effectiveFetchPath = shouldOverrideFetchPath ? fetchPath! : configFetchPath!;

      const overriddenApiKey = apiKey && this.overrodeConfig
        ? this.getOverriddenApiKey(effectiveBaseUrl, effectiveFetchPath)
        : undefined;

      if(shouldOverrideBaseUrl) {
        const msg = `Overriding config baseUrl (${configBaseUrl}) with value from message parameters (${baseUrl})`;
        console.log(msg);
        this.messages.push(msg);
        (this.config.dataSource.people as DataSourceConfig).endpointConfig.baseUrl = baseUrl!;
      }

      if(shouldOverrideFetchPath) {
        const msg = `Overriding config fetchPath (${configFetchPath}) with value from message parameters (${fetchPath})`;
        console.log(msg);
        this.messages.push(msg);
        (this.config.dataSource.people as DataSourceConfig).fetchPath = fetchPath!;
      }

      if (apiKey && this.overrodeConfig) {
        const maskedPeopleApiKey = apiKey.length > 4 
          ? `${apiKey.slice(0, 2)}${apiKey.split('').map(c => '*').join('').substring(4)}${apiKey.slice(-2)}` 
          : '****';

        if (overriddenApiKey) {
          (this.config.dataSource.people as DataSourceConfig).endpointConfig =
            (this.config.dataSource.people as DataSourceConfig).endpointConfig || {};
          (this.config.dataSource.people as DataSourceConfig).endpointConfig.apiKey = overriddenApiKey;

          const maskedPersonApiKey = overriddenApiKey && overriddenApiKey.length > 4
            ? `${overriddenApiKey.slice(0, 2)}${overriddenApiKey.split('').map(c => '*').join('').substring(4)}${overriddenApiKey.slice(-2)}`
            : '****';
          const msg = `Overriding config apiKey: ${JSON.stringify({ original: maskedPeopleApiKey, overridden: maskedPersonApiKey })}`;
          console.log(msg);
          this.messages.push(msg);
        }
        else {
          const msg = `NOTE: One or more of the standard source API parameters have been overridden by sqs ` +
            `message details, and no matching key for overriding could be found. This ` +
            `assumes that the configured apiKey (${maskedPeopleApiKey}) is still valid ` +
            `for the alternate endpoint`;
          console.log(msg);
          this.messages.push(msg);
        }
      }
    }
    catch(e) {
      console.error('Error overriding config with message parameters:', e);
      this.messages.push(`Error overriding config with message parameters: ${e}`);
    }
    finally {
      return this.config;
    }
  }

  public getMessages = (): string[] => {
    return this.messages; 
  }

  /**
   * Compare urls based on baseUrl + fetchPath parameters and return the person api endpoint
   * apiKey if it is found to be the true matching endpoint.
   * @param baseUrl 
   * @param fetchPath 
   * @returns 
   */
  private getOverriddenApiKey = (baseUrl: string, fetchPath: string): string | undefined => {
    const { fetchPath: personFetchPath, endpointConfig: { baseUrl: personBaseUrl, apiKey: personApiKey } = {} } = this.personConfig;
    const { fetchPath: peopleFetchPath, endpointConfig: { baseUrl: peopleBaseUrl, apiKey: peopleApiKey } = {} } = this.peopleConfig;

    const buildUrl = (base: string | undefined, path: string | undefined): URL | undefined => {
      if (!base) {
        return undefined;
      }

      const url = new URL(base);
      const [pathWithoutQuery] = (path || '').split('?');
      const [pathWithoutHash] = pathWithoutQuery.split('#');
      if (pathWithoutHash) {
        url.pathname = pathWithoutHash;
      }
      url.search = '';
      url.hash = '';
      return url;
    };

    const url = buildUrl(baseUrl, fetchPath);
    const peopleUrl = buildUrl(peopleBaseUrl, peopleFetchPath);
    const personUrl = buildUrl(personBaseUrl, personFetchPath);

    if (!url || !peopleUrl || !personUrl) {
      return undefined;
    }
    
    if (url.href !== peopleUrl.href) {
      if (url.href === personUrl.href) {
        return personApiKey;
      }
    }
    return undefined;
  }
}