import { FUNCTION_BASE_NAME as chunkerFunctionBaseName } from "../../chunking/fetch/ChunkerApiSubscriber";
import { FUNCTION_BASE_NAME as processorFunctionBaseName } from "../../processing/ProcessorSubscriber";
import { ChunkingServiceRunner } from "../AbstractRunner";
import { Config } from "integration-huron-person";
import { Endpoint, NormalizedPopulationType, TargetConfig } from "../RunnerTypes";
import { ServiceToDisable, ServiceToggler } from "../ServiceToggler";

/**
 * Decorator that ensures the chunker and processor services are enabled before executing the 
 * wrapped runner. This "restores" to full operation after having potentially having run the
 * MessagingOnlyRunnerDecorator or ChunkingOnlyRunnerDecorator.
 */
export class RestoreToFullOperationRunnerDecorator extends ChunkingServiceRunner {
  constructor(private readonly wrappedRunner: ChunkingServiceRunner) {
    super(wrappedRunner.env);
    console.log('Neither messaging-only nor chunking-only mode specified. Restoring all services to normal operation.');
  }

  public async validatePrerequisites(): Promise<boolean> {
    const { region, landscape, messagingOnly, chunkingOnly, sourceSimulator } = this.wrappedRunner.env;

    // Validate the configuration to catch mutually exclusive modes and incompatible settings.
    if(messagingOnly) {
      console.error('Invalid configuration: RestoreToFullOperationRunnerDecorator should not be used when messaging-only mode is enabled.');
      return false;
    }
    if(chunkingOnly) {
      console.error('Invalid configuration: RestoreToFullOperationRunnerDecorator should not be used when chunking-only mode is enabled.');
      return false;
    }
    if(sourceSimulator) {
      console.error('Invalid configuration: RestoreToFullOperationRunnerDecorator should not be used when source simulator mode is enabled.');
      return false;
    }

    // Checks if service is disabled and enables it if it is.
    await new ServiceToggler({
      lambdaFunctionName: `${chunkerFunctionBaseName}-${landscape}`,
      region: region!,
      service: ServiceToDisable.CHUNKER
    }).enableService();

    // Checks if service is disabled and enables it if it is.
    await new ServiceToggler({
      lambdaFunctionName: `${processorFunctionBaseName}-${landscape}`,
      region: region!,
      service: ServiceToDisable.PROCESSOR
    }).enableService();

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