import { Config, DataSourceConfig } from "integration-huron-person";
import { TERMS_PATH, SourceSimulatorFunctionURL, getSimulatorCounter, personIndexFromBuid, simulatorFetchPath } from "../../chunking/fetch/SourceSimulator";
import { ChunkingServiceRunner } from "../AbstractRunner";
import { Endpoint, NormalizedPopulationType, TargetConfig } from "../RunnerTypes";

/**
 * Decorator that configures and uses the source simulator Lambda when enabled, providing
 * mock person and terms data for testing without the 30-minute API cooldown constraint.
 *
 * The decorator overrides both the people and terms data source endpoints to point to the
 * source simulator Lambda Function URL, which simulates responses from the real BU CDM APIs.
 * The terms endpoint is returned as the source endpoint's termsEndpoint, which travels with the
 * chunker message into the run's Flags, where the processor tasks pick it up. (Changing this
 * runner's config object would not reach them: they load their own config from the secret.)
 * In single person mode (SINGLE_PERSON_BUID) the source is the simulator's single person endpoint
 * (`/person?buid=...`) instead, which returns the same payload the people endpoint produces for
 * that person.
 *
 * The run's simulator settings (SOURCE_SIMULATOR_MOCK_*) go in the source's fetchPath as query
 * parameters (simulatorFetchPath(), as the dashboard's Bulk sync builds it), which every request of
 * the run keeps. The simulator Lambda's configuration is never changed: a setting left unset is the
 * simulator's own, and nothing carries over to later runs.
 */
export class SourceSimulatorRunnerDecorator extends ChunkingServiceRunner {
  constructor(private readonly wrappedRunner: ChunkingServiceRunner) {
    super(wrappedRunner.env);
  }
  public async validatePrerequisites(): Promise<boolean> {
    let { region, landscape, messagesToPrepopulate, iterationLimit } = this.wrappedRunner.env;

    // Check if the source simulator lambda function URL exists before proceeding.
    const sourceSimulatorFunctionUrl = new SourceSimulatorFunctionURL({ landscape: landscape!, region: region! });
    if( ! await sourceSimulatorFunctionUrl.exists() ) {
      console.error(`Source simulator function URL does not exist for landscape ${landscape} in region ${region}. Please ensure the source simulator is deployed and the function URL is created.`);
      return false;
    }

    const settings = this.simulatorSettings();
    console.log(Object.keys(settings).length > 0
      ? `Source simulator settings for this run, sent as fetchPath query parameters (any other is the simulator's own): ${JSON.stringify(settings)}`
      : `No SOURCE_SIMULATOR_MOCK_* settings: the run uses the source simulator's own.`);

    const getNbrFromStr = (val: string | undefined): number => val && !isNaN(Number(val)) ? parseInt(val) : 0;

    const getNbr = (nbr: number | undefined): number => nbr && !isNaN(nbr) ? nbr : 0;

    const { buid } = this.wrappedRunner.env;

    // Single person mode: the simulator only knows BUIDs it generates itself (U + digits), and only
    // returns a person within its population.
    if (buid) {
      const index = personIndexFromBuid(buid);
      if (index === undefined) {
        console.error(`SINGLE_PERSON_BUID=${buid} is not a BUID the source simulator generates ("U" followed by digits, e.g. U0000042).`);
        return false;
      }
      const { totalPopulation } = await this.getSourceSimulatorPredictions();
      if (totalPopulation > 0 && index >= totalPopulation) {
        console.warn(`SINGLE_PERSON_BUID=${buid} is beyond the source simulator's population (${totalPopulation}), so it will return no person.`);
      }
    }

    // Warn if queue seeding is being carried out that we are not overseeding as determined by
    // comparing the seedNumber math to source simulator predicted population.
    // Skip this check for single-person mode (buid is set) since it doesn't use queue seeding.
    if (!buid && getNbrFromStr(messagesToPrepopulate) > 0 ) {
      const seedNumber = parseInt(messagesToPrepopulate);

      if(seedNumber > 0) {
        const predictions = await this.getSourceSimulatorPredictions();
        const { totalPopulation, itemsPerChunk } = predictions;
        iterationLimit = getNbr(iterationLimit);
        const singleTaskPopulation = iterationLimit * itemsPerChunk;
        if(singleTaskPopulation >= 0) {
          const seedingPopulation = seedNumber * itemsPerChunk * iterationLimit;

          if(seedingPopulation > totalPopulation) {
            const overage = seedingPopulation - totalPopulation;
            if(overage > singleTaskPopulation) {
              console.warn(`Overseeding: Seeding population (${seedingPopulation}) exceeds total population (${totalPopulation}) by more than a single task's worth of items (${singleTaskPopulation}).`);
            }
          }
          else {
            console.log(`Seeding population (${seedingPopulation}) is within the total population (${totalPopulation}). Proceeding with operation.`);
          }
        }
      }
    }


    return this.wrappedRunner.validatePrerequisites();
  }

  public async resolveDataSource(config: Config): Promise<Endpoint> {
    const { region, landscape, sourceSimulator, stackId } = this.wrappedRunner.env;
    const { buid } = this.wrappedRunner.env;
    let {
      endpointConfig: { baseUrl } = {},
      fetchPath
    } = config.dataSource?.people as DataSourceConfig || {};

    if(!sourceSimulator) {
      console.log('Source simulator not enabled. Using configured endpoint.');
      return { baseUrl: baseUrl!, fetchPath: fetchPath! };
    }

    // Use source simulator if enabled
    if (!region || !landscape) {
      throw new Error('REGION and LANDSCAPE are required to use source simulator');
    }

    const sourceSimulatorFunctionUrl = new SourceSimulatorFunctionURL({ landscape, region });
    const functionUrlObj = new URL(await sourceSimulatorFunctionUrl.getUrl());
    baseUrl = `${functionUrlObj.protocol}//${functionUrlObj.host}`;
    // The people endpoint, or the single person endpoint for a BUID, with the run's simulator settings
    fetchPath = simulatorFetchPath({ path: functionUrlObj.pathname, buid, settings: this.simulatorSettings() });

    console.log(`Using source simulator for ${buid ? 'single person' : 'people'} endpoint: ${JSON.stringify({ baseUrl, fetchPath }, null, 2)}`);

    // Simulated source data gets simulated terms too (hybrid runs never get here, so they keep the
    // configured terms endpoint).
    const termsEndpoint = { baseUrl: baseUrl!, fetchPath: TERMS_PATH };
    console.log(`Using source simulator for terms endpoint: ${JSON.stringify(termsEndpoint, null, 2)}`);

    if (buid) {
      // The single person endpoint is stateless: no predictions to log, no depletion counter to reset.
      return { baseUrl: baseUrl!, fetchPath: fetchPath!, termsEndpoint };
    }

    await this.logSourceSimulatorPredictions();

    // Reset the atomic counter for the source simulator to ensure a clean state before starting the operation. This is important to avoid any residual state from previous runs affecting the current operation.
    const atomicCounter = getSimulatorCounter({ stackId, region, landscape });
    console.log(`Resetting source simulator atomic counter for landscape ${landscape} in region ${region}.`);
    if(!atomicCounter) {
      throw new Error('Unable to get atomic counter for source simulator. Ensure STACK_ID, REGION, and LANDSCAPE are set.');
    }
    await atomicCounter.reset();

    return { baseUrl: baseUrl!, fetchPath: fetchPath!, termsEndpoint };
  }

  public async resolveDataTarget(config: Config): Promise<TargetConfig> {
    return this.wrappedRunner.resolveDataTarget(config);
  }

  public async execute(
    sourceEndpoint: Endpoint,
    targetConfig: TargetConfig,
    config: Config,
    populationType: NormalizedPopulationType
  ): Promise<void> {
    return this.wrappedRunner.execute(sourceEndpoint, targetConfig, config, populationType);
  }
}
