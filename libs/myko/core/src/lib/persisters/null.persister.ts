import { MItem } from '../types'
import { Persister, type PersisterFactory } from './persister'

export class NullPersister<T extends MItem> extends Persister<T> {
  persist(event) {
    // loop thru
    this.output$.next(event)
  }
}

export const nullPersisterFactory: PersisterFactory = (_: string) => {
  return new NullPersister()
}
