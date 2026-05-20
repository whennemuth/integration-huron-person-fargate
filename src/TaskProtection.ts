
/**
 * TaskProtection provides ECS task scale-in protection for Fargate tasks.
 *
 * Why needed: In ECS services, scale-in events (such as setting desiredCount=0) can kill running tasks
 * even if they are still processing work, resulting in SIGKILL/exit 137. This class enables protection
 * at the task level, preventing ECS from terminating the task during scale-in until protection is removed.
 *
 * Usage:
 *   const protection = new TaskProtection();
 *   await protection.enable(); // at task startup
 *   ... do work ...
 *   await protection.disable(); // before exit
 *
 * See: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-scale-in-protection.html
 */
export class TaskProtection {
  private readonly ecsAgentUri: string;
  private readonly protectionMinutes: number;

  /**
   * @param protectionMinutes Number of minutes to enable protection (max 2880 = 48h, default 60)
   * @param ecsAgentUri Optional override for ECS agent URI (default from ECS_AGENT_URI env)
   */
  constructor(protectionMinutes: number = 60, ecsAgentUri?: string) {
    this.ecsAgentUri = ecsAgentUri || process.env.ECS_AGENT_URI || 'http://169.254.170.2';
    // 48 hours max protection (2880 minutes) to prevent excessively long protection that could cause issues if tasks get stuck
    this.protectionMinutes = Math.min(Math.max(protectionMinutes, 1), 2880);
  }

  /**
   * Enable task protection to prevent ECS scale-in from terminating this task.
   */
  public enable = async (): Promise<void> => {
    const { IS_ECS_TASK = 'false' } = process.env;
    if (IS_ECS_TASK.toLowerCase() !== 'true') {
      console.log(`[TaskProtection] Not running in ECS task, skipping task protection.`);
      return;
    }

    // Set protection
    console.log(`[TaskProtection] Enabling task protection for ${this.protectionMinutes} minutes...`);
    const url = `${this.ecsAgentUri}/task-protection/v1/state`;
    const payload = { ProtectionEnabled: true, ExpiresInMinutes: this.protectionMinutes };
    const setResponse = await fetch(url, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    await this.logResponse(setResponse, '[TaskProtection] Protection set response');

    // Verify protection was set
    const getResponse = await fetch(url, { method: 'GET' });
    await this.logResponse(getResponse, '[TaskProtection] Protection verified status');
  }

  private logResponse = async (response: Response, msg: string) => {
    // NOTE: Despite the method being named json(), the result is not JSON but is instead the 
    // result of taking JSON as input and parsing it to produce a JavaScript object.
    const body = await response.json();
    const json = JSON.stringify(body);
    console.log(`${msg}: ${json}`);
  }

  /**
   * Disable task protection, allowing ECS to terminate this task if desiredCount=0.
   */
  public disable = async (): Promise<void> => {
    const { IS_ECS_TASK = 'false' } = process.env;
    if (IS_ECS_TASK.toLowerCase() !== 'true') {
      console.log(`[TaskProtection] Not running in ECS task, skipping task protection.`);
      return;
    }
    console.log(`[TaskProtection] Disabling task protection...`);
    const url = `${this.ecsAgentUri}/task-protection/v1/state`;
    const payload = { ProtectionEnabled: false };
    const setResponse = await fetch(url, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    await this.logResponse(setResponse, '[TaskProtection] Protection disabled response');
  }
}