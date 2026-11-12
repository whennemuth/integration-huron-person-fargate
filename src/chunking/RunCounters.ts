import { AbstractAtomicCounter } from '../dynamodb/AtomicCounter';
import { CHUNKER_COUNTER_NAME, CHUNK_ORDINAL_COUNTER_NAME } from './ChunkerQueue';
import type { ApiChunkerEvent } from './ChunkerSubscriber';

/** The chunker's counters a run starts from zero: the source offset its chained messages take, and chunk ordinals */
export const RUN_COUNTER_NAMES = [CHUNKER_COUNTER_NAME, CHUNK_ORDINAL_COUNTER_NAME];

/** Where a landscape's atomic counter table is (DYNAMODB_TABLE_NAME in src/dynamodb/AtomicCounter.ts names it) */
export type RunCounterLocation = { stackId: string; region: string; landscape: string };

/**
 * Reset the chunker's counters to zero, so a new run's chained chunking messages and chunk ordinals start at 0
 * (ChunkerQueue: "Counter lifecycle is controlled by runner-level reset before each operation"). One reset, shared
 * by every way a run starts: the chunker subscriber Lambda for a run's first event (the EventBridge schedule, the
 * dashboard's Bulk sync), and the manual Runner (AbstractRunner.resetAtomicCounters()).
 */
export async function resetRunCounters(location: RunCounterLocation): Promise<void> {
  for (const name of RUN_COUNTER_NAMES) {
    console.log(`Resetting atomic counter ${name}`);
    await new class extends AbstractAtomicCounter {
      getCounterName(): string {
        return name;
      }
    }(location).reset();
  }
}

/**
 * Whether an API chunker event starts a run, rather than continuing one: offset 0 and no chunk directory. The
 * schedule's and the dashboard's events are; the Runner's seeded messages are not (each carries the run's
 * chunkDirectory, and all but the first a later offset), and the chunker's own chained messages go to its queue,
 * never through the subscriber Lambda.
 */
export function isRunStartEvent(event: ApiChunkerEvent): boolean {
  return (event.offset ?? 0) === 0 && !event.chunkDirectory;
}
