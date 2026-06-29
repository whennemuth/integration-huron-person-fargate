import { LambdaFunctionEnvironmentVariable } from "./LambdaFunctionEnvironmentVariable";
import { FUNCTION_BASE_NAME as chunkerFunctionBaseName } from "../../lib/services/chunker/ChunkerSubscribingLambda";
import { FUNCTION_BASE_NAME as processorFunctionBaseName } from "../../lib/services/processor/ProcessorSubscribingLambda";
import { TestEnvironment } from "integration-core";

export enum ServiceToDisable { CHUNKER, PROCESSOR }

export type ServiceTogglerParams = {
  service: ServiceToDisable, 
  lambdaFunctionName: string, 
  region: string 
};

export type ChunkerTogglerParams = ServiceTogglerParams & {
  lambdaFunctionEnvironmentVariable: LambdaFunctionEnvironmentVariable
};

export type ProcessorTogglerParams = ServiceTogglerParams & {
  lambdaFunctionEnvironmentVariable: LambdaFunctionEnvironmentVariable
};

export type IServiceToggler = {
  disableService(): Promise<void>;
  enableService(): Promise<void>;
  isServiceDisabled(): Promise<boolean>;
}

/**
 * ServiceToggler is a utility class that allows you to enable or disable specific services 
 * (CHUNKER or PROCESSOR) by manipulating the DRY_RUN environment variable of the corresponding 
 * AWS Lambda event subscriber functions.
 * 
 * NOTE: The choice of implementation allows for adding or swapping different ways to enable/disable 
 * services in the future without changing the interface of ServiceToggler.
 */
export class ServiceToggler implements IServiceToggler {
  private wrappedToggler: IServiceToggler;

  constructor(private params: ServiceTogglerParams) {
    const lambdaFunctionEnvironmentVariable = new LambdaFunctionEnvironmentVariable({ 
      lambdaFunctionName: params.lambdaFunctionName, 
      region: params.region 
    });

    switch(params.service) {
      case ServiceToDisable.CHUNKER:
        this.wrappedToggler = new ChunkerToggler({ ...params, lambdaFunctionEnvironmentVariable });
        break;
      case ServiceToDisable.PROCESSOR:
        this.wrappedToggler = new ProcessorToggler({ ...params, lambdaFunctionEnvironmentVariable });
        break;
      default:
        throw new Error(`Unsupported service to disable: ${params.service}`);
    }
  }

  public async disableService(): Promise<void> {
    if( ! await this.wrappedToggler.isServiceDisabled()) {
      await this.wrappedToggler.disableService();
    }
  }

  public async enableService(): Promise<void> {
    if(await this.wrappedToggler.isServiceDisabled()) {
      await this.wrappedToggler.enableService();
    }
  }

  public async isServiceDisabled(): Promise<boolean> {
    return await this.wrappedToggler.isServiceDisabled();
  }
}

/**
 * Messaging only:
 * If you do not want new chunk files appearing in the bucket to set off the processor service,
 * you can set the environment variable DRY_RUN=true for the chunker subscriber lambda function to 
 * prevent the chunker from sending messages to the processor queue. This is most easily done through 
 * the AWS management console, or you can use the AWS CLI:
 * 
 * aws lambda update-function-configuration \
 *  --region us-east-2 \
 *  --cli-input-json "$(aws lambda get-function-configuration \
 *     --function-name chunker-subscriber-dev \
 *     --region us-east-2 | jq '.Environment.Variables += {"DRY_RUN": "true"} | {FunctionName: .FunctionName, Environment: .Environment}')"
 * 
 * Or, run this decorator.
 */
class ChunkerToggler implements IServiceToggler {
  constructor(private params: ChunkerTogglerParams) {}

  public async disableService(): Promise<void> {
    const { params: { lambdaFunctionEnvironmentVariable: lambdaFunction } } = this;
    await lambdaFunction.setEnvironmentVariable('DRY_RUN', 'true');
  }

  public async enableService(): Promise<void> {
    const { params: { lambdaFunctionEnvironmentVariable: lambdaFunction } } = this;
    await lambdaFunction.setEnvironmentVariable('DRY_RUN', 'false');
  }

  public async isServiceDisabled(): Promise<boolean> {
    const { params: { lambdaFunctionEnvironmentVariable: lambdaFunction } } = this;
    const dryRunValue = await lambdaFunction.getEnvironmentVariable('DRY_RUN');
    return `${dryRunValue}`.toLowerCase() === 'true';
  }
}

/**
 * Chunking only:
 * If you want chunking to proceed and have files appearing in the bucket, but you do not want 
 * the processor service to pick them up for processing, you can set the environment variable
 * DRY_RUN=true for the processor subscriber lambda function to prevent it from sending messages
 * to the processor queue. This is most easily done through the AWS management console, or you can
 * use the AWS CLI:
 * 
 * aws lambda update-function-configuration \
 *  --region us-east-2 \
 *  --cli-input-json "$(aws lambda get-function-configuration \
 *     --function-name processor-subscriber-dev \
 *     --region us-east-2 | jq '.Environment.Variables += {"DRY_RUN": "true"} | {FunctionName: .FunctionName, Environment: .Environment}')"
 * 
 * Or, run this decorator.
 */
class ProcessorToggler implements IServiceToggler {
  constructor(private params: ProcessorTogglerParams) {}

  public async disableService(): Promise<void> {
    const { params: { lambdaFunctionEnvironmentVariable: lambdaFunction } } = this;
    await lambdaFunction.setEnvironmentVariable('DRY_RUN', 'true');
  }

  public async enableService(): Promise<void> {
    const { params: { lambdaFunctionEnvironmentVariable: lambdaFunction } } = this;
    await lambdaFunction.setEnvironmentVariable('DRY_RUN', 'false');
  }

  public async isServiceDisabled(): Promise<boolean> {
    const { params: { lambdaFunctionEnvironmentVariable: lambdaFunction } } = this;
    const dryRunValue = await lambdaFunction.getEnvironmentVariable('DRY_RUN');
    return `${dryRunValue}`.toLowerCase() === 'true';
  }
}

// ============================================================================
// TEST HARNESS
// ============================================================================

/**
 * Test harness for ServiceToggler with multiple task modes.
 * 
 * Environment Variables (prefixed with SERVICE_TOGGLER_):
 * - TASK: Task to perform (CHECK_STATUS | DISABLE | ENABLE)
 * - SERVICE: Service to toggle (CHUNKER | PROCESSOR)
 * - REGION: AWS region (e.g., us-east-2)
 * - LANDSCAPE: Environment landscape (e.g., dev, stg, prd)
 * 
 * Usage Examples:
 * 
 * 1. Check if chunker is disabled:
 *    SERVICE_TOGGLER_TASK=CHECK_STATUS
 *    SERVICE_TOGGLER_SERVICE=CHUNKER
 *    SERVICE_TOGGLER_REGION=us-east-2
 *    SERVICE_TOGGLER_LANDSCAPE=dev
 * 
 * 2. Disable processor service:
 *    SERVICE_TOGGLER_TASK=DISABLE
 *    SERVICE_TOGGLER_SERVICE=PROCESSOR
 *    SERVICE_TOGGLER_REGION=us-east-2
 *    SERVICE_TOGGLER_LANDSCAPE=dev
 * 
 * 3. Enable chunker service:
 *    SERVICE_TOGGLER_TASK=ENABLE
 *    SERVICE_TOGGLER_SERVICE=CHUNKER
 *    SERVICE_TOGGLER_REGION=us-east-2
 *    SERVICE_TOGGLER_LANDSCAPE=dev
 */

enum Task {
  CHECK_STATUS = 'CHECK_STATUS',
  DISABLE = 'DISABLE',
  ENABLE = 'ENABLE'
}

async function main() {
  // Extract required environment variables
  const {
    TASK: taskStr,
    SERVICE: serviceStr,
    REGION: region,
    LANDSCAPE: landscape
  } = process.env;

  // Validate required variables
  if (!taskStr || !serviceStr || !region || !landscape) {
    console.error('Missing required environment variables. Please set:');
    if (!taskStr) console.error('  - SERVICE_TOGGLER_TASK');
    if (!serviceStr) console.error('  - SERVICE_TOGGLER_SERVICE');
    if (!region) console.error('  - SERVICE_TOGGLER_REGION');
    if (!landscape) console.error('  - SERVICE_TOGGLER_LANDSCAPE');
    return;
  }

  // Validate task
  const task = taskStr as Task;
  if (!Object.values(Task).includes(task)) {
    console.error(`Invalid TASK: ${taskStr}. Must be one of: ${Object.values(Task).join(', ')}`);
    return;
  }

  // Validate and map service
  let service: ServiceToDisable;
  let lambdaFunctionName: string;
  
  if (serviceStr === 'CHUNKER') {
    service = ServiceToDisable.CHUNKER;
    lambdaFunctionName = `${chunkerFunctionBaseName}-${landscape}`;
  } else if (serviceStr === 'PROCESSOR') {
    service = ServiceToDisable.PROCESSOR;
    lambdaFunctionName = `${processorFunctionBaseName}-${landscape}`;
  } else {
    console.error(`Invalid SERVICE: ${serviceStr}. Must be CHUNKER or PROCESSOR`);
    return;
  }

  console.log(`\n${'='.repeat(70)}`);
  console.log(`ServiceToggler Test Harness`);
  console.log(`${'='.repeat(70)}`);
  console.log(`Task: ${task}`);
  console.log(`Service: ${serviceStr}`);
  console.log(`Lambda Function: ${lambdaFunctionName}`);
  console.log(`Region: ${region}`);
  console.log(`${'='.repeat(70)}\n`);

  // Create ServiceToggler instance
  const toggler = new ServiceToggler({
    service,
    lambdaFunctionName,
    region
  });

  try {
    switch (task) {
      case Task.CHECK_STATUS:
        await checkStatus(toggler, serviceStr);
        break;
      
      case Task.DISABLE:
        await disableService(toggler, serviceStr);
        break;
      
      case Task.ENABLE:
        await enableService(toggler, serviceStr);
        break;
      
      default:
        console.error(`Unhandled task: ${task}`);
    }
  } catch (error) {
    console.error(`\n❌ Error executing task ${task}:`, error);
    throw error;
  }
}

async function checkStatus(toggler: ServiceToggler, serviceName: string): Promise<void> {
  console.log(`📊 Checking status of ${serviceName} service...\n`);
  
  const isDisabled = await toggler.isServiceDisabled();
  
  console.log(`\nStatus: ${isDisabled ? '🔴 DISABLED' : '🟢 ENABLED'}`);
  console.log(`DRY_RUN environment variable: ${isDisabled ? 'true' : 'false'}\n`);
  
  if (isDisabled) {
    console.log(`The ${serviceName} subscriber Lambda is in DRY_RUN mode.`);
    console.log(`Messages will not be sent to downstream services.\n`);
  } else {
    console.log(`The ${serviceName} subscriber Lambda is operating normally.`);
    console.log(`Messages will be sent to downstream services.\n`);
  }
}

async function disableService(toggler: ServiceToggler, serviceName: string): Promise<void> {
  console.log(`🛑 Disabling ${serviceName} service...\n`);
  
  const wasDisabled = await toggler.isServiceDisabled();
  
  if (wasDisabled) {
    console.log(`ℹ️  ${serviceName} service is already disabled (DRY_RUN=true).\n`);
  } else {
    await toggler.disableService();
    console.log(`✅ Successfully disabled ${serviceName} service.`);
    console.log(`   DRY_RUN environment variable set to: true\n`);
  }
  
  // Verify the change
  const isNowDisabled = await toggler.isServiceDisabled();
  console.log(`Verification: Service is now ${isNowDisabled ? 'DISABLED ✓' : 'ENABLED ✗'}\n`);
}

async function enableService(toggler: ServiceToggler, serviceName: string): Promise<void> {
  console.log(`✅ Enabling ${serviceName} service...\n`);
  
  const wasDisabled = await toggler.isServiceDisabled();
  
  if (!wasDisabled) {
    console.log(`ℹ️  ${serviceName} service is already enabled (DRY_RUN=false or unset).\n`);
  } else {
    await toggler.enableService();
    console.log(`✅ Successfully enabled ${serviceName} service.`);
    console.log(`   DRY_RUN environment variable set to: false\n`);
  }
  
  // Verify the change
  const isNowDisabled = await toggler.isServiceDisabled();
  console.log(`Verification: Service is now ${isNowDisabled ? 'DISABLED ✗' : 'ENABLED ✓'}\n`);
}

// Run test harness if executed directly
if (require.main === module) {
  const testEnvironment = TestEnvironment('SERVICE_TOGGLER');

  [
    'TASK',
    'SERVICE',
    'REGION',
    'LANDSCAPE'
  ].forEach(testEnvironment.getVar);

  main().catch(error => {
    console.error('Fatal error in test harness:', error);
    process.exit(1);
  });
}

