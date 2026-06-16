import { TestEnvironment } from "integration-core";
import { IContext } from "../context/IContext";
import { Service, ECSClient, UpdateServiceCommand } from "@aws-sdk/client-ecs";
import { ApplicationAutoScalingClient, DescribeScalableTargetsCommand, DescribeScalableTargetsCommandInput } from "@aws-sdk/client-application-auto-scaling";

export type DesiredCountParams = {
  clusterName?: string;
  serviceName?: string;
  region?: string;
}

/**
 * Use this class to get or set the desired count of an ECS service. 
 */
export class DesiredCount {
  private maxTasks: number | undefined;

  constructor(private params: DesiredCountParams) {
    const { clusterName, serviceName, region } = this.params;

    // Validate required environment variables
    if (!serviceName) {
      console.warn('⚠️  Service name not set. Unable to scale.');
      return;
    }

    if (!clusterName) {
      console.warn('⚠️  Cluster name not set. Unable to scale.');

      return;
    }

    if (!region) {
      console.warn('⚠️  Region not set. Unable to scale.');
      return;
    }   
  }

  public setTo = async (desiredCount: number): Promise<void> => {
    const { params: { clusterName, serviceName, region }, getMax } = this;

    try {
      // Abort if attempting to set desired count above the max tasks defined in auto-scaling of the service.
      const maxTasks = await getMax();
      if (maxTasks !== undefined && desiredCount > maxTasks) {
        console.warn(`⚠️  Desired count value ${desiredCount} exceeds max capacity of `
          + `${maxTasks} for ${clusterName}.${serviceName}. Aborting scale operation.`);
        return;
      }

      const ecsClient = new ECSClient({ region });

      const command = new UpdateServiceCommand({
        cluster: clusterName,
        service: serviceName,
        desiredCount,
      });

      if (process.env.DRY_RUN?.toLowerCase() === 'true') {
        console.log(`[DRY RUN] Would scale ${clusterName}.${serviceName} to a desired count of ${desiredCount}`);
        return;
      }

      await ecsClient.send(command);
      console.log(`✓ Successfully scaled ${clusterName}.${serviceName} to a desired count of ${desiredCount}`);
    } catch (error: any) {
      console.error(`⚠️  Failed to scale ${clusterName}.${serviceName} to a desired count of ${desiredCount}: ${error.message}`);
    }
  }

  /**
   * Retrieves the maximum number of tasks the desired count can be set to for the specified ECS service.
   * @returns The maximum number of tasks the desired count can be set to.
   */
  public getMax = async (): Promise<number | undefined> => {
    const { params: { clusterName, serviceName, region }, maxTasks } = this;

    if(maxTasks !== undefined) {
      return maxTasks;
    }
    
    const client = new ApplicationAutoScalingClient({ region });

    const params = {
      ServiceNamespace: "ecs",
      ResourceIds: [`service/${clusterName}/${serviceName}`],
      ScalableDimension: "ecs:service:DesiredCount"
    } as DescribeScalableTargetsCommandInput;

    const command = new DescribeScalableTargetsCommand(params);
    const response = await client.send(command);
    
    if (response.ScalableTargets && response.ScalableTargets.length > 0) {
      const scalableTarget = response.ScalableTargets[0];
      this.maxTasks = scalableTarget.MaxCapacity;
      return this.maxTasks;
    } else {
      console.log(`No scalable target found for ${clusterName}.${serviceName}`);
      return undefined;
    }
  }

  /**
   * Set the desired count to the maximum defined in auto-scaling for the service, to trigger 
   * scaling up to handle increased load. Used to cause the service to "hit the ground running" 
   * as an alternative to gradually "ramping up" from 0 desired count.
   * @returns 
   */
  public setToMax = async (): Promise<void> => {
    const { params: { clusterName, serviceName }, getMax, setTo } = this;
    try {
      const maxTasks = await getMax();

      if (maxTasks === undefined) {
        console.warn(`Unable to retrieve max desired count for ${clusterName}.${serviceName}. Aborting setToMax.`);
        return;
      }

      await setTo(maxTasks);
    }
    catch (error: any) {
      console.error(`⚠️  Failed to set desired count to max for ${clusterName}.${serviceName}: ${error.message}`);
    }
  }

  private getService = async (): Promise<Service | undefined> => {
    const { clusterName, serviceName, region } = this.params;
    const { ECSClient, DescribeServicesCommand } = await import('@aws-sdk/client-ecs');
    const ecsClient = new ECSClient({ region });

    const command = new DescribeServicesCommand({
      cluster: clusterName,
      services: [serviceName || ''],
    });

    const response = await ecsClient.send(command);
    const service = response.services?.[0];
    return service;
  }

  public getCurrent = async (): Promise<number | undefined> => {
    const { clusterName, serviceName } = this.params;

    try {
      const service = await this.getService();
      const currentDesiredCount = service?.desiredCount;

      console.log(`Current desired count for ${clusterName}.${serviceName}, desiredCount=${currentDesiredCount}`);
      return currentDesiredCount;
    } catch (error: any) {
      console.error(`⚠️  Failed to get current desired count for ${clusterName}.${serviceName}: ${error.message}`);
      return undefined;
    }
  }
}

async function main() {
  let { 
    TASK: task,
    ECS_CLUSTER_NAME: clusterName,
    ECS_SERVICE_NAME: serviceName,
    DESIRED_COUNT: desiredCountEnv = '0',
    REGION: region 
  } = process.env;

  // Load context.
  const context = require('../context/context.json') as IContext;
  
  clusterName = clusterName || context?.ECS.clusterName;
  region = region || context?.REGION;

  const desiredCount = new DesiredCount({ clusterName, serviceName, region });

  switch (task as 'get' | 'set' | 'get-max' | 'set-max') {
    case 'get':
      await desiredCount.getCurrent();
      break;
    case 'get-max':
      const max = await desiredCount.getMax();
      console.log(`Max desired count for ${clusterName}.${serviceName} is ${max}`);
      break;
    case 'set':
      const desiredCountValue = parseInt(desiredCountEnv, 10);
      if(isNaN(desiredCountValue)) {
        console.error(`Invalid DESIRED_COUNT value: ${desiredCountEnv}`);
        return;
      }
      await desiredCount.setTo(desiredCountValue);
      break;
    case 'set-max':
      await desiredCount.setToMax();
      break;
    default:
      console.error(`Unknown task: ${task}`);
  }
}


// Run if executed directly
if(require.main === module) {
  const testEnvironment = TestEnvironment('DESIRED_COUNT');

  [
    'TASK',
    'ECS_CLUSTER_NAME',
    'ECS_SERVICE_NAME',
    'DESIRED_COUNT',
    'REGION',
    'DRY_RUN'
  ].forEach(testEnvironment.getVar);

  main();
}