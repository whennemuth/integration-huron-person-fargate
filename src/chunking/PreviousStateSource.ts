import { PreviousStateSource, previousStateSourceOf } from 'integration-huron-person';

export type { PreviousStateSource } from 'integration-huron-person';

/**
 * A run's previous state source (integration-huron-person's PreviousStateSource): `storage`, the hashes stored at
 * earlier syncs decide what changed; `target`, every person is looked up in the target (a bulk reset). It travels the
 * run's messages as `previousStateSource` (ApiChunkerEvent -> the chunker queue message -> TaskParameters -> each
 * chained message -> Flags -> the processors), replacing the bulkReset / trustPreviousStorage pair.
 *
 * The pipeline has no default: every run start says which it wants, and anything that doesn't is refused.
 *
 * The pair is still read, and translated, from what was sent or written before previousStateSource existed: events
 * from senders not yet moved over, a message queued or a run underway when this was deployed. In the pipeline an
 * unset trustPreviousStorage beside a set bulkReset meant false (the chunker subscriber's default), unlike in
 * integration-huron-person, where it means true; messages always carried both, but the pipeline's meaning is kept.
 */
export type PreviousStateFields = {
  previousStateSource?: unknown;
  /** @deprecated Read only, from what was sent or written before previousStateSource */
  bulkReset?: unknown;
  /** @deprecated Read only, from what was sent or written before previousStateSource */
  trustPreviousStorage?: unknown;
};

/** A message's boolean, which may have travelled as a string; undefined when absent */
const flag = (value: unknown): boolean | undefined => {
  if (value === undefined || value === null || `${value}`.trim() === '') {
    return undefined;
  }
  return typeof value === 'boolean' ? value : `${value}`.trim().toLowerCase() === 'true';
};

/**
 * The previous state source the fields ask for: previousStateSource, or else the deprecated pair translated (`target`
 * if bulkReset is true or trustPreviousStorage is false). Undefined when they carry neither.
 *
 * @throws if previousStateSource is not `storage` or `target`, or the pair is also given and means something else
 */
export const previousStateSourceIn = (fields: PreviousStateFields): PreviousStateSource | undefined => {
  const previousStateSource = `${fields.previousStateSource ?? ''}`.trim() === '' ? undefined : fields.previousStateSource;
  const bulkReset = flag(fields.bulkReset);
  const trustPreviousStorage = flag(fields.trustPreviousStorage);
  if (previousStateSource === undefined && bulkReset === undefined && trustPreviousStorage === undefined) {
    return undefined;
  }
  const pairGiven = bulkReset !== undefined || trustPreviousStorage !== undefined;
  return previousStateSourceOf({
    previousStateSource: previousStateSource as PreviousStateSource | undefined,
    ...(pairGiven ? { bulkReset: bulkReset ?? false, trustPreviousStorage: trustPreviousStorage ?? false } : {})
  });
};

/**
 * Why the fields can't start or carry a run: they don't say where its previous state comes from, or say something
 * invalid or contradictory. Undefined when they can.
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
 * previousStateSourceIn(), refusing fields that carry neither the setting nor the deprecated pair.
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
