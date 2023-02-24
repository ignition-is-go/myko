import { MEvent, MItem } from '../types'

export abstract class Persister<T extends MItem> {
  abstract persist(event: MEvent<T>): void

  abstract init(): Promise<void>
}
