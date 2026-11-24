import { PreviousStateSource, previousStateSourceOf } from 'integration-huron-person';

export type { PreviousStateSource } from 'integration-huron-person';

/**
 * A run's previous state source (integration-huron-person's PreviousStateSource): `storage`, the hashes stored at
 * earlier syncs decide what changed; `target`, every person is looked up in the target (a bulk reset). It travels the
 * run's messages as `previousStateSource` (ApiChunkerEvent -> the chunker queue message -> TaskParameters -> each
 * chained message -> Flags -> the processors).
 *
 * The pipeline has no default: every run start says which it wants, and anything that doesn't is refused. It replaced
 * the bulkReset / trustPreviousStorage pair, which is refused too, by name: a message, event or record carrying it is
 * from before previousStateSource (only Run history still reads such records, to show old runs).
 */
export type PreviousStateFields = {
  previousStateSource?: unknown;
  /** Refused: the pair previousStateSource replaced */
  bulkReset?: unknown;
  /** Refused: the pair previousStateSource replaced */
  trustPreviousStorage?: unknown;
};

/** Absent, or as good as: undefined, null or blank (a value that travelled as a string) */
const blank = (value: unknown): boolean => value === undefined || value === null || `${value}`.trim() === '';

/**
 * The previous state source the fields ask for, or undefined when they carry nothing about it.
 *
 * @throws if previousStateSource is not `storage` or `target`, or the fields carry the removed bulkReset /
 *   trustPreviousStorage (integration-huron-person's previousStateSourceOf() names them)
 */
export const previousStateSourceIn = (fields: PreviousStateFields): PreviousStateSource | undefined => {
  const { previousStateSource, bulkReset, trustPreviousStorage } = fields;
  if (blank(previousStateSource) && blank(bulkReset) && blank(trustPreviousStorage)) {
    return undefined;
  }
  return previousStateSourceOf({
    ...(blank(previousStateSource) ? {} : { previousStateSource: previousStateSource as PreviousStateSource }),
    ...(blank(bulkReset) ? {} : { bulkReset }),
    ...(blank(trustPreviousStorage) ? {} : { trustPreviousStorage })
  } as Parameters<typeof previousStateSourceOf>[0]);
};

/**
 * Why the fields can't start or carry a run: they don't say where its previous state comes from, say something
 * invalid, or carry the removed pair. Undefined when they can.
 */
export const previousStateRefusal = (fields: PreviousStateFields): string | undefined => {
  try {
    return previousStateSourceIn(fields) === undefined
      ? `No previousStateSource: a run must say where its previous state comes from ('storage' or 'target')`
      : undefined;
  } catch (error) {
    return error instanceof Error ? error.message : String(error);
  }
};

/**
 * previousStateSourceIn(), refusing fields that carry nothing about it.
 *
 * @param what Names the fields' source in the error, e.g. 'The chunker queue message'
 * @throws as previousStateSourceIn() does, and when the fields carry nothing
 */
export const requirePreviousStateSource = (fields: PreviousStateFields, what: string): PreviousStateSource => {
  const previousStateSource = previousStateSourceIn(fields);
  if (previousStateSource === undefined) {
    throw new Error(`${what} has no previousStateSource: a run must say where its previous state comes from ` +
      `('storage' or 'target')`);
  }
  return previousStateSource;
};
