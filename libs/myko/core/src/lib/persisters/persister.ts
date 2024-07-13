import type { Observable } from 'rxjs'
import type { MEvent, MItem } from '../types'

export abstract class Persister<T extends MItem> {
  public output: Observable<MEvent<T>>

  abstract persist(event: MEvent<T>): void
}

export type PersisterFactory = <O extends Record<string, any>, T extends MItem>(
  itemName: string,
  options?: O,
) => Persister<T>
