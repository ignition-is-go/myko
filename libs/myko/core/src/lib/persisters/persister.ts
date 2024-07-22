import { Subject } from 'rxjs'
import type { MEvent, MItem } from '../types'

export abstract class Persister<T extends MItem> {
  constructor() {
    this.output$ = new Subject<MEvent<T>>()
  }

  protected output$: Subject<MEvent<T>>

  get output() {
    return this.output$.asObservable()
  }

  abstract persist(event: MEvent<T>): void
}

export type PersisterFactory = <O extends Record<string, any>, T extends MItem>(
  itemName: string,
  options?: O,
) => Persister<T>
