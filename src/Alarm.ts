import {
  ApplicationAutoScalingClient,
  DescribeScalingPoliciesCommand,
  ScalingPolicy
} from '@aws-sdk/client-application-auto-scaling';
import {
  CloudWatchClient,
  DescribeAlarmsCommand,
  MetricAlarm,
  StateValue
} from '@aws-sdk/client-cloudwatch';
import { TestEnvironment } from 'integration-core';

export type ServiceScaleInAlarmParams = {
  clusterName: string;
  serviceName: string;
  region: string;
};

/**
 * Locates and reads scale-in CloudWatch alarm of the specified ECS service.
 */
export class ServiceScaleInAlarm {
  private readonly cloudWatch: CloudWatchClient;
  private readonly appAutoScaling: ApplicationAutoScalingClient;
  private resolvedAlarmName?: string;

  constructor(private readonly params: ServiceScaleInAlarmParams) {
    const { region } = params;
    this.cloudWatch = new CloudWatchClient({ region });
    this.appAutoScaling = new ApplicationAutoScalingClient({ region });
  }

  public async getAlarmName(): Promise<string> {
    if (this.resolvedAlarmName) {
      return this.resolvedAlarmName;
    }

    const { clusterName, serviceName } = this.params;
    const resourceId = `service/${clusterName}/${serviceName}`;

    const response = await this.appAutoScaling.send(
      new DescribeScalingPoliciesCommand({
        ServiceNamespace: 'ecs',
        ResourceId: resourceId,
        ScalableDimension: 'ecs:service:DesiredCount'
      })
    );

    const scaleInPolicy = (response.ScalingPolicies || []).find(
      (policy: ScalingPolicy) =>
        (policy.StepScalingPolicyConfiguration?.StepAdjustments || []).some(
          (adjustment) => (adjustment.ScalingAdjustment || 0) < 0
        )
    );

    const alarmName = scaleInPolicy?.Alarms?.[0]?.AlarmName;
    if (!alarmName) {
      throw new Error(
        `Unable to discover scale-in alarm for ${resourceId}. ` +
        'No step scaling policy with a negative scaling adjustment alarm was found.'
      );
    }

    this.resolvedAlarmName = alarmName;
    return alarmName;
  }

  public async getAlarmState(): Promise<StateValue | undefined> {
    const alarm = await this.getMetricAlarmOrThrow();
    return alarm.StateValue;
  }

  public async getAlarmPeriodSeconds(): Promise<number> {
    const alarm = await this.getMetricAlarmOrThrow();

    if (alarm.Period && alarm.Period > 0) {
      return alarm.Period;
    }

    const metricPeriods = (alarm.Metrics || [])
      .map((metricQuery) => metricQuery.MetricStat?.Period)
      .filter((period): period is number => typeof period === 'number' && period > 0);

    if (metricPeriods.length > 0) {
      return Math.max(...metricPeriods);
    }

    const alarmName = await this.getAlarmName();
    throw new Error(
      `Unable to determine period for alarm ${alarmName}. ` +
      'Alarm has no Period and no Metrics[*].MetricStat.Period values.'
    );
  }

  private async getMetricAlarmOrThrow(): Promise<MetricAlarm> {
    const alarmName = await this.getAlarmName();
    const response = await this.cloudWatch.send(
      new DescribeAlarmsCommand({ AlarmNames: [alarmName] })
    );

    const alarm = response.MetricAlarms?.[0];
    if (!alarm) {
      throw new Error(`Alarm not found: ${alarmName}`);
    }

    return alarm;
  }
}

async function main(): Promise<void> {
  const {
    TASK: task,
    ECS_CLUSTER_NAME: clusterName,
    ECS_SERVICE_NAME: serviceName,
    REGION: region
  } = process.env;

  if (!task) {
    throw new Error('TASK environment variable is required. Valid values: get-name, get-state, get-period');
  }
  if (!clusterName) {
    throw new Error('ECS_CLUSTER_NAME environment variable is required.');
  }
  if (!serviceName) {
    throw new Error('ECS_SERVICE_NAME environment variable is required.');
  }
  if (!region) {
    throw new Error('REGION environment variable is required.');
  }

  const alarm = new ServiceScaleInAlarm({
    clusterName,
    serviceName,
    region
  });

  switch (task as 'get-name' | 'get-state' | 'get-period') {
    case 'get-name': {
      const alarmName = await alarm.getAlarmName();
      console.log(`Scale-in alarm name: ${alarmName}`);
      break;
    }
    case 'get-state': {
      const state = await alarm.getAlarmState();
      console.log(`Scale-in alarm state: ${state || 'UNKNOWN'}`);
      break;
    }
    case 'get-period': {
      const period = await alarm.getAlarmPeriodSeconds();
      console.log(`Scale-in alarm period: ${period} seconds`);
      break;
    }
    default:
      throw new Error(`Unknown TASK value: ${task}. Valid values: get-name, get-state, get-period`);
  }
}

if (require.main === module) {
  const testEnvironment = TestEnvironment('ALARM');

  [
    'TASK',
    'ECS_CLUSTER_NAME',
    'ECS_SERVICE_NAME',
    'REGION'
  ].forEach(testEnvironment.getVar);

  main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`Alarm harness failed: ${message}`);
    process.exitCode = 1;
  });
}
