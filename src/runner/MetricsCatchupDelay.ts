import { TestEnvironment } from 'integration-core';
import { ServiceScaleInAlarm } from '../Alarm';

export type AlarmLookupParams = {
  clusterName?: string;
  serviceName?: string;
  region?: string;
};

export type TemporalParams = {
  countdownStepSeconds?: number;
  additionalDelaySeconds?: number;
  alarmPeriodSeconds?: number;
}
export type MetricsCatchupDelayParams = AlarmLookupParams & TemporalParams;

/**
 * MetricsCatchupDelay exists to handle a timing edge-case between queue seeding and autoscaling alarm
 * evaluation. During incident analysis, the lower threshold alarm could still be in ALARM from a prior
 * empty-queue state when a new run starts. Even after new messages are seeded, CloudWatch alarm state
 * does not update instantly because evaluation happens on the alarm metric period.
 *
 * This class waits for one alarm period plus optional buffer before proceeding. It first attempts to
 * resolve the scale-in alarm and read its actual configured period; if lookup is unavailable, it falls
 * back to ALARM_PERIOD_SECONDS and then a default of 60 seconds.
 *
 * During the delay, it logs a countdown every countdownStepSeconds. If alarm lookup is available, it
 * also checks alarm state during each interval and can end early once the alarm is no longer ALARM.
 * If the full delay elapses while state is still ALARM, it emits a warning so callers can decide
 * whether to proceed or bail out.
 */
export class MetricsCatchupDelay {
  private scaleInAlarm?: ServiceScaleInAlarm;

  constructor(private readonly params?: MetricsCatchupDelayParams) {}

  private getScaleInAlarm = (): ServiceScaleInAlarm | undefined=> {
    if (!this.scaleInAlarm) {
      const { clusterName, serviceName, region } = this.params ?? {};
      if (clusterName && serviceName && region) {
        this.scaleInAlarm = new ServiceScaleInAlarm({ clusterName, serviceName, region });
      }
    }
    return this.scaleInAlarm;
  }

  private async getAlarmPeriodSeconds(): Promise<number> {
    const scaleInAlarm = this.getScaleInAlarm();
    if (scaleInAlarm) {
      try {
        const period = await scaleInAlarm.getAlarmPeriodSeconds();
        console.log(`Alarm period obtained by lookup: ${period} seconds`);
        return period;
      }
      catch (error) {
        console.warn(`Failed to lookup alarm period for ${this.params?.clusterName}/${this.params?.serviceName} in ${this.params?.region}: ${error}`);
      }
    }
    const { alarmPeriodSeconds } = this.params ?? {};
    if( alarmPeriodSeconds ) {
      return alarmPeriodSeconds;
    }
    console.warn('No alarm period could be determined; using default of 60 seconds');
    return 60; // default alarm period if lookup fails and no alarmPeriodSeconds provided
  }

  private async getAlarmState(): Promise<string | undefined> {
    const scaleInAlarm = this.getScaleInAlarm();
    if (scaleInAlarm) {
      try {
        const state = await scaleInAlarm.getAlarmState();
        return state;
      }
      catch (error) {
        return undefined;
      }
    }
    return undefined;
  }

  /**
   * @param signal Optional abort signal - when aborted, the delay ends promptly and returns false.
   * @returns true if the alarm is no longer ALARM (safe to proceed with scaling), false if the
   * full delay elapsed while still in ALARM state, or the delay was aborted (caller should NOT
   * proceed with scaling).
   */
  public async startDelay(signal?: AbortSignal): Promise<boolean> {
    const alarmPeriodSeconds = await this.getAlarmPeriodSeconds();
    const {
      // CloudWatch alarm evaluation and SDK-visible alarm state are not always synchronized
      // immediately. In incident follow-up testing, alarm state queried via SDK remained ALARM
      // for roughly 2-3 minutes after queue depth increased and alarm history showed transition.
      // We intentionally use a conservative 5-minute default propagation buffer to avoid scaling
      // into a stale ALARM view that can immediately trigger an unwanted scale-in reaction.
      additionalDelaySeconds=300,
      countdownStepSeconds=5
    } = this.params ?? {};
    const totalDelaySeconds = alarmPeriodSeconds + additionalDelaySeconds;

    console.log(
      `\n⏳ Starting metrics catch-up delay using fixed base delay (${alarmPeriodSeconds}s)` +
      `${additionalDelaySeconds > 0 ? ` + ${additionalDelaySeconds}s buffer` : ''}.`
    );

    let remaining = totalDelaySeconds;
    let alarmState: string | undefined;
    
    while (remaining > 0) {
      if (signal?.aborted) {
        console.warn(`⚠️ Metrics catch-up delay aborted after ${totalDelaySeconds - remaining} seconds, before alarm state reached OK.`);
        return false;
      }
      alarmState = await this.getAlarmState();
      if (alarmState !== 'OK') {
        console.log(`Alarm state: ${alarmState ? alarmState : 'undefined'} (needs to be OK). ${remaining} of ${totalDelaySeconds} seconds remain to try again, retrying in ${countdownStepSeconds} seconds...`);
      }
      else {
        console.log(`✓ Metrics catch-up delay completed after ${totalDelaySeconds - remaining} seconds (alarm state, ${alarmState}, is no longer ALARM).\n`);
        return true;
      }
      const sleepSeconds = Math.min(countdownStepSeconds, remaining);
      await this.sleep(sleepSeconds * 1000, signal);
      remaining -= sleepSeconds;
    }

    if (signal?.aborted) {
      console.warn(`⚠️ Metrics catch-up delay aborted before alarm state reached OK.`);
      return false;
    }
    if( alarmState === 'ALARM') {
      console.warn(`⚠️ Metrics catch-up delay completed, but alarm state is still ALARM. Scaling may not go as expected.`);
      return false;
    }
    console.log(`✓ Metrics catch-up delay completed.\n`);
    return true;
  }

  private sleep(ms: number, signal?: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      if (signal?.aborted) {
        return resolve();
      }
      const onAbort = () => {
        clearTimeout(timeoutId);
        resolve();
      };
      const timeoutId = setTimeout(() => {
        signal?.removeEventListener('abort', onAbort);
        resolve();
      }, ms);
      signal?.addEventListener('abort', onAbort, { once: true });
    });
  }
}

async function main(): Promise<void> {
  const {
    COUNTDOWN_STEP_SECONDS: countdownStepSeconds,
    ADDITIONAL_DELAY_SECONDS: additionalDelaySeconds,
    ALARM_PERIOD_SECONDS: alarmPeriodSeconds,
    REGION: region,
    ECS_CLUSTER_NAME: clusterName,
    ECS_SERVICE_NAME: serviceName
  } = process.env;

  const delay = new MetricsCatchupDelay({
    countdownStepSeconds: countdownStepSeconds ? parseInt(countdownStepSeconds, 10) : undefined,
    additionalDelaySeconds: additionalDelaySeconds ? parseInt(additionalDelaySeconds, 10) : undefined,
    alarmPeriodSeconds: alarmPeriodSeconds ? parseInt(alarmPeriodSeconds, 10) : undefined,
    region,
    clusterName,
    serviceName
  });

  await delay.startDelay();
}

if (require.main === module) {
  const testEnvironment = TestEnvironment('METRICS_CATCHUP_DELAY');

  [
    'COUNTDOWN_STEP_SECONDS',
    'ADDITIONAL_DELAY_SECONDS',
    'ALARM_PERIOD_SECONDS',
    'REGION',
    'ECS_CLUSTER_NAME',
    'ECS_SERVICE_NAME'
  ].forEach(testEnvironment.getVarOrEmptyString);

  main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`MetricsCatchupDelay failed: ${message}`);
    process.exitCode = 1;
  });
}
