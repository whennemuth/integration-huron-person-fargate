import { FUNCTION_BASE_NAME as processorFunctionBaseName } from "../../lib/services/chunker/ChunkerSubscribingLambda";
import { FUNCTION_BASE_NAME as chunkingFunctionBaseName } from "../../lib/services/processor/ProcessorSubscribingLambda";
import { ChunkingServiceRunner } from "./AbstractRunner";
import { Endpoint, NormalizedPopulationType } from "./RunnerTypes";
import { ServiceToDisable, ServiceToggler } from "./ServiceToggler";

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
      lambdaFunctionName: `${chunkingFunctionBaseName}-${env.landscape}`,
      region: env.region!
    });
  }
  public async validatePrerequisites(): Promise<boolean> {
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


/**
 * Chunking only Decorator.
 * Chunk files will be created, but the service that processes them will be disabled.
 * This is useful for testing the chunk file creation features, allowing the entire end-to-end 
 * syncing process to proceed no further than that.
 */
export class ChunkingOnlyRunnerDecorator extends ChunkingServiceRunner {
  private serviceToggler: ServiceToggler;

  constructor(private readonly wrappedRunner: ChunkingServiceRunner) {
    super();
    const env = this.wrappedRunner.env;
    this.serviceToggler = new ServiceToggler({
      service: ServiceToDisable.PROCESSOR,
      lambdaFunctionName: `${processorFunctionBaseName}-${env.landscape}`,
      region: env.region!
    });
  }
  public async validatePrerequisites(): Promise<boolean> {
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

export class RestoreToFullOperationRunnerDecorator extends ChunkingServiceRunner {
  constructor(private readonly wrappedRunner: ChunkingServiceRunner) {
    super();
  }
  public async validatePrerequisites(): Promise<boolean> {
    const env = this.wrappedRunner.env;

    // Checks if service is disabled and enables it if it is.
    await new ServiceToggler({
      lambdaFunctionName: `${chunkingFunctionBaseName}-${env.landscape}`,
      region: env.region!,
      service: ServiceToDisable.CHUNKER
    }).enableService();

    // Checks if service is disabled and enables it if it is.
    await new ServiceToggler({
      lambdaFunctionName: `${processorFunctionBaseName}-${env.landscape}`,
      region: env.region!,
      service: ServiceToDisable.PROCESSOR
    }).enableService();

    return this.wrappedRunner.validatePrerequisites();
  }
  public resolveDataSource(config: any): Promise<Endpoint> {
    return this.wrappedRunner.resolveDataSource(config);
  }
  public execute(endpoint: Endpoint, config: any, populationType: NormalizedPopulationType): Promise<void> {
    return this.wrappedRunner.execute(endpoint, config, populationType);
  }

}
