
/** The source of person data either is all people, or only those that have changed */
export enum SyncPopulation {
  PersonFull = 'person-full',
  PersonDelta = 'person-delta'
}
