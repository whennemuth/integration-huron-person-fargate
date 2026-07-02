import { GetFunctionConfigurationCommand, LambdaClient, UpdateFunctionConfigurationCommand } from "@aws-sdk/client-lambda";

/**
 * Utility class to get and set environment variables for a specific AWS Lambda function.
 * 
 * This class uses the AWS SDK to interact with the Lambda service and manage environment variables.
 * It provides methods to retrieve and update environment variables while preserving existing ones.
 */
export class LambdaFunctionEnvironmentVariable {

  constructor(private params: { lambdaFunctionName: string, region: string }) { }

  public getEnvironmentVariables = async (): Promise<{ [key: string]: string }> => {
    const { lambdaFunctionName, region } = this.params;
    
    const lambdaClient = new LambdaClient({ region });
    
    try {
      const command = new GetFunctionConfigurationCommand({
        FunctionName: lambdaFunctionName
      });
      
      const response = await lambdaClient.send(command);
      
      const { Environment: { Variables = {} } = {} } = response;
      return Variables;
    } catch (error) {
      console.error(`Failed to get environment variables from Lambda function ${lambdaFunctionName}:`, error);
      throw error;
    }

  }

  /**
   * Get an environment variable from a Lambda function's configuration.
   * 
   * @param name - Name of the environment variable to retrieve
   * @returns The value of the environment variable, or undefined if not found
   */
  public getEnvironmentVariable = async (name: string): Promise<string | undefined> => {
    try {
      const envVars = await this.getEnvironmentVariables();
      return envVars[name];
    } catch (error) {
      console.error(`Failed to get environment variable ${name} from Lambda function ${this.params.lambdaFunctionName}:`, error);
      throw error;
    }
  }
  
  /**
   * Set an environment variable on a Lambda function.
   * 
   * This updates the Lambda function's configuration with the new environment variable value.
   * All existing environment variables are preserved.
   * 
   * @param name - Name of the environment variable to set
   * @param val - Value to set for the environment variable
   */
  public setEnvironmentVariable = async (name: string, val: string): Promise<void> => {
    await this.setEnvironmentVariables({ [name]: val });
  }

  /**
   * Set one or more environment variables on a Lambda function in a single update request.
   *
   * This avoids back-to-back UpdateFunctionConfiguration calls when multiple variables
   * need to be changed together.
   *
   * @param entries - Record of environment variable names and values to apply
   */
  public setEnvironmentVariables = async (entries: Record<string, string>): Promise<void> => {
    const { lambdaFunctionName, region } = this.params;

    if (!entries || Object.keys(entries).length === 0) {
      console.log(`No environment variable changes provided for Lambda function ${lambdaFunctionName}`);
      return;
    }

    const lambdaClient = new LambdaClient({ region });

    try {
      // First, get the current configuration to preserve existing environment variables
      const getCommand = new GetFunctionConfigurationCommand({
        FunctionName: lambdaFunctionName
      });

      const currentConfig = await lambdaClient.send(getCommand);
      const currentVariables = currentConfig.Environment?.Variables || {};

      const updatedVariables = {
        ...currentVariables,
        ...entries
      };

      // Apply a single update containing all requested variable changes.
      const updateCommand = new UpdateFunctionConfigurationCommand({
        FunctionName: lambdaFunctionName,
        Environment: {
          Variables: updatedVariables
        }
      });

      await lambdaClient.send(updateCommand);

      const updatedNames = Object.keys(entries).join(', ');
      console.log(`Successfully set environment variables [${updatedNames}] on Lambda function ${lambdaFunctionName}`);
    } catch (error) {
      const updatedNames = Object.keys(entries).join(', ');
      console.error(`Failed to set environment variables [${updatedNames}] on Lambda function ${lambdaFunctionName}:`, error);
      throw error;
    }
  }
}