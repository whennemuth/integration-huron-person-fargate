import { FUNCTION_BASE_NAME as chunkerFunctionBaseName } from "../../chunking/fetch/ChunkerApiSubscriber";
import { FUNCTION_BASE_NAME as processorFunctionBaseName } from "../../processing/ProcessorSubscriber";
import { ChunkingServiceRunner } from "../AbstractRunner";
import { Config } from "integration-huron-person";
import { Endpoint, NormalizedPopulationType, TargetConfig } from "../RunnerTypes";
import { ServiceToDisable, ServiceToggler } from "../ServiceToggler";

/**
 * Chunking only Decorator.
 * Chunk files will be created, but the service that processes them will be disabled.
 * This is useful for testing the chunk file creation features, allowing the entire end-to-end 
 * syncing process to proceed no further than that.
 */
export class ChunkingOnlyRunnerDecorator extends ChunkingServiceRunner {
  private serviceToggler: ServiceToggler;

  constructor(private readonly wrappedRunner: ChunkingServiceRunner) {
    super(wrappedRunner.env);
    const env = this.wrappedRunner.env;
    this.serviceToggler = new ServiceToggler({
      service: ServiceToDisable.PROCESSOR,
      lambdaFunctionName: `${processorFunctionBaseName}-${env.landscape}`,
      region: env.region!
    });
  }

  public async validatePrerequisites(): Promise<boolean> {
    const { landscape, region, messagingOnly, buid } = this.wrappedRunner.env;

    // Validate the configuration to catch mutually exclusive modes and incompatible settings.
    if(messagingOnly) {
      console.error('Invalid configuration: MESSAGING_ONLY and CHUNKING_ONLY cannot both be true.');
      return false;
    }
    if(buid) {
      console.error('Invalid configuration: MESSAGING_ONLY and SINGLE_PERSON_BUID cannot both be true.');
      return false;
    }

    // First enable chunker message creation if it was previously disabled.
    await new ServiceToggler({
      service: ServiceToDisable.CHUNKER,
      lambdaFunctionName: `${chunkerFunctionBaseName}-${landscape}`,
      region: region!
    }).enableService();

    // Then disable the processor service so that chunk files are created but not processed.
    await this.serviceToggler.disableService();

    // Finally, validate the prerequisites of the wrapped runner.
    return this.wrappedRunner.validatePrerequisites();
  }
  public async resolveDataSource(config: Config): Promise<Endpoint> {
    return this.wrappedRunner.resolveDataSource(config);
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
