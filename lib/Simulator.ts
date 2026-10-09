import { ITable } from 'aws-cdk-lib/aws-dynamodb';
import { Construct } from 'constructs';
import { IContext } from '../context/IContext';
import { SourceSimulator } from './services/chunker/SourceSimulator';
import { TargetSimulator } from './services/processor/TargetSimulator';
import { DataTargetOverride, HuronPersonSecrets } from './Secrets';

export interface SimulatorProps {
  context: IContext;
  /** The mock target table the target simulator stores persons in */
  mockTargetPersonTable: ITable;
  tags?: { [key: string]: string };
}

/**
 * The simulators of a mock landscape: the target simulator (always) and the source simulator
 * (if enabled in the context). Neither may exist in any other landscape.
 *
 * Two steps because the source simulator reads the integration config secret, which itself embeds
 * the target simulator's endpoint: construct this, build the secret from `dataTargetOverride`, then
 * call `createSourceSimulator()`.
 */
export class Simulator extends Construct {
  public readonly targetSimulator: TargetSimulator;
  public sourceSimulator?: SourceSimulator;
  private readonly context: IContext;
  private readonly tags?: { [key: string]: string };

  constructor(scope: Construct, id: string, props: SimulatorProps) {
    super(scope, id);

    const { context, mockTargetPersonTable, tags } = props;
    const { STACK_ID, REGION, TAGS: { Landscape }, LAMBDA: { targetSimulator: tsCtx } } = context;
    this.context = context;
    this.tags = tags;

    this.targetSimulator = new TargetSimulator(this, 'Target', {
      landscape: Landscape.toLowerCase(),
      stackId: STACK_ID,
      region: REGION,
      table: mockTargetPersonTable,
      timeoutSeconds: tsCtx?.timeoutSeconds,
      memorySizeMb: tsCtx?.memorySizeMb,
      listCacheTtlSeconds: tsCtx?.listCacheTtlSeconds,
      tags,
    });
  }

  /** The data target endpoint the integration config secret must point at */
  public get dataTargetOverride(): DataTargetOverride {
    return {
      baseUrl: this.targetSimulator.baseUrl,
      externalToken: this.targetSimulator.externalToken,
    };
  }

  /** Creates the source simulator if enabled in the context; a no-op otherwise. */
  public createSourceSimulator(huronPersonSecrets: HuronPersonSecrets): SourceSimulator | undefined {
    const { STACK_ID, REGION, ACCOUNT, TAGS: { Landscape }, LAMBDA: { sourceSimulator: ssCtx } } = this.context;

    if (!ssCtx?.enabled) {
      console.log('[Simulator] Source Simulator disabled - Skipping creation');
      return undefined;
    }

    this.sourceSimulator = new SourceSimulator(this, 'Source', {
      huronPersonSecrets,
      landscape: Landscape.toLowerCase(),
      stackId: STACK_ID,
      region: REGION,
      account: ACCOUNT,
      timeoutSeconds: ssCtx.timeoutSeconds,
      memorySizeMb: ssCtx.memorySizeMb,
      mockTotalPopulation: ssCtx.mockTotalPopulation,
      mockErrorRate: ssCtx.mockErrorRate,
      simulatedDelaySeconds: ssCtx.simulatedDelaySeconds,
      secretArn: huronPersonSecrets.secretArn,
      tags: this.tags,
    });

    console.log('[Simulator] Source Simulator enabled - Function URL will be created');
    return this.sourceSimulator;
  }
}
