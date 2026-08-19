import { FUNCTION_BASE_NAME as chunkerFunctionBaseName } from "../../chunking/fetch/ChunkerApiSubscriber";
import { ChunkingServiceRunner } from "../AbstractRunner";
import { Config } from "integration-huron-person";
import { Endpoint, NormalizedPopulationType, TargetConfig } from "../RunnerTypes";
import { ServiceToDisable, ServiceToggler } from "../ServiceToggler";

/**
 * Messaging only decorator.
 * No chunk files will be created, but messages will be sent to the chunker 
 * queue. This is useful for testing the message creation features, allowing the entire 
 * end-to-end syncing process to proceed no further than that.
 */
export class MessagingOnlyRunnerDecorator extends ChunkingServiceRunner {
  private serviceToggler: ServiceToggler;

  constructor(private readonly wrappedRunner: ChunkingServiceRunner) {
    super();
    const env = this.wrappedRunner.env;
    this.serviceToggler = new ServiceToggler({
      service: ServiceToDisable.CHUNKER,
      lambdaFunctionName: `${chunkerFunctionBaseName}-${env.landscape}`,
      region: env.region!
    });
  }

  public async validatePrerequisites(): Promise<boolean> {
    const { chunkingOnly, sourceSimulator } = this.wrappedRunner.env;

    // Validate the configuration to catch mutually exclusive modes and incompatible settings.
    if(chunkingOnly) {
      console.error('Invalid configuration: MESSAGING_ONLY and CHUNKING_ONLY cannot both be true.');
      return false;
    }
    if(sourceSimulator) {
      console.error('Invalid configuration: MESSAGING_ONLY and SOURCE_SIMULATOR cannot both be true.');
      return false;
    }

    await this.serviceToggler.disableService();
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
