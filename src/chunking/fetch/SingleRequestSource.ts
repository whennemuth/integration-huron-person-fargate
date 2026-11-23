import { BuCdmPeopleDataSource, Config, DataSourceConfig } from 'integration-huron-person';

/**
 * True when the configured people source can't be paged: it answers one request with the same
 * records every time and ignores offsets. That is the case for person-mode config, or a people fetch
 * URL that names one person (a `buid` query parameter, e.g. `...huronIRBPerson?buid=U12345678`, as a
 * single-person Test or the Runner's single-person run sends).
 *
 * BigJsonFetch then fetches through a DepletingSingleRequestDataSource, so the run sees a population of
 * one that behaves like the real, depleting people source.
 *
 * The URL is built as BuCdmPeopleDataSource.getFetchUrl() builds it (fetchPath resolved against the
 * endpoint's baseUrl), from config that already carries the queue message's baseUrl/fetchPath
 * (ChunkConfigOverride).
 */
export const isSingleRequestSource = (config: Config | undefined): boolean => {
  if (config?.executionMode === 'person') {
    return true;
  }

  const people = config?.dataSource?.people as DataSourceConfig | undefined;
  const fetchPath = people?.fetchPath;
  if (!fetchPath) {
    return false;
  }

  try {
    return new URL(fetchPath, people?.endpointConfig?.baseUrl).searchParams.get('buid') !== null;
  } catch {
    // Conservative fallback: if the URL can't be parsed, don't treat the source as single-request.
    return false;
  }
};

/**
 * Claims a run's one fetch of a single-request source: true for exactly one caller in the run
 * (MetadataBroker.claimSingleRequestFetch()), false for every later one.
 */
export type SingleRequestFetchClaim = () => Promise<boolean>;

/** A claim local to one process (harnesses and tests without run metadata): the first call wins. */
export const localSingleRequestFetchClaim = (): SingleRequestFetchClaim => {
  let claimed = false;
  return async () => {
    if (claimed) {
      return false;
    }
    claimed = true;
    return true;
  };
};

/**
 * A single-request source (isSingleRequestSource()) made to behave like the real people source with a
 * population of one: the real source depletes, handing out each record once per run whatever the
 * offset, so a single-person run must do the same for the pipeline to run as a full sync does.
 *
 * The run's first fetch, by whichever chunker task gets there first (claimFetch), returns the person;
 * every later fetch in the run returns an empty batch. So a task's fetch loop runs as in a full sync (a
 * batch, then an empty batch marking the end), chained tasks start as usual, and a chained task finds
 * the source depleted instead of fetching and chunking the same person again.
 *
 * The person endpoint takes no paging parameters, so recordCount/offset are never added to its URL.
 */
export class DepletingSingleRequestDataSource extends BuCdmPeopleDataSource {
  constructor(
    params: ConstructorParameters<typeof BuCdmPeopleDataSource>[0],
    private readonly claimFetch: SingleRequestFetchClaim
  ) {
    super(params);
  }

  /** The person endpoint takes no paging parameters (recordCount, offset) */
  public setQueryParam = (_key: string, _value: any): void => { };

  async fetchRaw(): Promise<any[]> {
    if (await this.claimFetch()) {
      return super.fetchRaw();
    }
    console.log('Single-request source already fetched in this run: returning an empty batch (a population of one, depleted).');
    return [];
  }
}
