/**
 * Marks a stored baseline hash as belonging to a person who has been soft-deleted (deactivated)
 * in the target by the merger's deferred delete handling.
 *
 * Why the marker lives in the hash itself:
 * Baselines (PersonCurrentStateTable in DynamoDB mode, previous-input.ndjson in S3 mode) retain
 * every person ever seen. Without a record of the deactivation, a person absent from the source
 * stays in the baseline and is identified for deletion again on every subsequent full sync.
 * Prefixing the hash serves both purposes that deactivation state needs to serve:
 *   1. Deferred delete handling skips marked baseline entries, so a person is deactivated once.
 *   2. A marked hash can never equal a freshly computed source hash, so a person who reappears in
 *      the source is classified UPDATED (not UNCHANGED) by the processors, and that UPDATE carries
 *      the __active flag that reactivates them in the target. Processors overwrite the marked hash
 *      with the new one on a successful push, which clears the marker.
 */
export const DELETED_HASH_PREFIX = 'DELETED:';

export const isDeletedHash = (hash?: string): boolean => {
  return typeof hash === 'string' && hash.startsWith(DELETED_HASH_PREFIX);
}

/**
 * Idempotent: an already marked hash is returned unchanged.
 */
export const toDeletedHash = (hash: string): string => {
  return isDeletedHash(hash) ? hash : `${DELETED_HASH_PREFIX}${hash}`;
}
