import { MItem, type MEvent } from '../types'
import { Persister, type PersisterFactory } from './persister'

export class NullPersister<T extends MItem> extends Persister<T> {
  constructor() {
    super()

    this.load$.complete()
  }

  persist(event: MEvent<T>) {
    this.output$.next({ event, percent: 100 })
  }
}

export const nullPersisterFactory: PersisterFactory = (_: string) => {
  return new NullPersister()
}
