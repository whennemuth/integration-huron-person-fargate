/**
 * What a run records of the processor service's maximum tasks it started with (src/chunking/ProcessorScalingCapacity.ts):
 * the maximum the chunker subscriber set at its start, and whether it was the run's own override rather than the
 * landscape's maxScalingCapacity. It travels like startedBy: the run's first event -> the chunker's queue messages
 * (TaskParameters, each chained and seeded message) -> the run's Flags (written once, by its first chunker task) -> its
 * summary, for Run history. Absent for a run whose maximum wasn't set (DRY_RUN, or a failed restore of the landscape's,
 * which leaves it unknown), for runs from before it was recorded, and for the S3 file drop (its queue message carries
 * only the file). The manual Runner sets the maximum itself (ChunkingServiceRunner.start()) and puts the record on its
 * events through the source Endpoint.
 *
 * In a module of its own, without AWS clients, so the Flags type, the statistics table and the dashboard can use it.
 */
export type ProcessorCapacityRecord = {
  /** The processor service's maximum tasks for the run */
  maxTasks: number;
  /** Whether it was the run's processorScalingCapacityOverride, rather than the landscape's maxScalingCapacity */
  overridden: boolean;
};

export class ProcessorCapacity {
  /** The record of what a run start set: only when it was set */
  static record(result: { applied: boolean; capacity?: number; override?: number }): ProcessorCapacityRecord | undefined {
    return result.applied && result.capacity !== undefined
      ? { maxTasks: result.capacity, overridden: result.override !== undefined }
      : undefined;
  }

  /** A record read back (from a queue message, the Flags or a summary), keeping only the known fields; undefined if it isn't one */
  static from(value: unknown): ProcessorCapacityRecord | undefined {
    if (!value || typeof value !== 'object') {
      return undefined;
    }
    const { maxTasks, overridden } = value as Record<string, unknown>;
    if (typeof maxTasks !== 'number' || !Number.isInteger(maxTasks) || maxTasks < 1) {
      return undefined;
    }
    return { maxTasks, overridden: overridden === true };
  }
}
