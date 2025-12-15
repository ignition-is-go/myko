import type { MItem, MEvent } from '../types'
import { Persister, type PersisterFactory } from './persister'

export class NullPersister<T extends MItem> extends Persister<T> {
  constructor() {
    super()

    // Defer completion to the next tick to allow all repos to register first
    // This prevents race conditions where onAllInit callbacks try to access
    // repos that haven't been created yet
    queueMicrotask(() => this.load$.complete())
  }

  persist(event: MEvent<T>) {
    this.output$.next({ event, percent: 100 })
  }
}

export const nullPersisterFactory: PersisterFactory = (_: string) => {
  return new NullPersister()
}
