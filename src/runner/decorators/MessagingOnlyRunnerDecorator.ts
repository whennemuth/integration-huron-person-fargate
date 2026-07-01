import { FUNCTION_BASE_NAME as chunkerFunctionBaseName } from "../../chunking/fetch/ChunkerApiSubscriber";
import { ChunkingServiceRunner } from "../AbstractRunner";
import { Endpoint, NormalizedPopulationType } from "../RunnerTypes";
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
  public async resolveDataSource(config: any): Promise<Endpoint> {
    return this.wrappedRunner.resolveDataSource(config);
  }
  public async execute(endpoint: Endpoint, config: any, populationType: NormalizedPopulationType): Promise<void> {
    return this.wrappedRunner.execute(endpoint, config, populationType);
  }
}
