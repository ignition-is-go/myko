import { from, type Observable } from 'rxjs'
import { type IRepo } from '../aggregates/repo.abstract'
import type { DeepPartial, ID, MItem } from '../types'
import type { HistoryProvider } from './abstract.history'

export class HistoryRepo<T extends MItem> implements IRepo<T> {
  #windbackTime: string
  #history: HistoryProvider

  constructor(protected entity: string) {}
  watchId(id: ID): Observable<T | null> {
    return from(this.getId(id))
  }
  watchIds(ids: ID[]): Observable<T[]> {
    return from(this.getIds(ids))
  }
  watchFilter(filterFunc: (ent: T) => boolean): Observable<T[]> {
    return from(this.getFilter(filterFunc))
  }
  watch(query: DeepPartial<T>): Observable<T[]> {
    return from(this.get(query))
  }
  getSearch(query: string): Promise<T[]> {
    throw new Error('Method not implemented.')
  }

  getIds(ids: ID[]): Promise<T[]> {
    throw new Error('Method not implemented.')
  }

  getId(id: ID): Promise<T | null> {
    return this.#history.getItemAsOfTime<T>(id, this.entity, this.#windbackTime)
  }

  get(query: DeepPartial<T>): Promise<T[]> {
    return this.#history.getItemsByQueryAsOfTime<T>(
      query,
      this.entity,
      this.#windbackTime,
    )
  }

  getFilter(filterFunc: (ent: T) => boolean): Promise<T[]> {
    return this.#history
      .getAllItemsAsOfTime<T>(this.entity, this.#windbackTime)
      .then((res) => res.filter(filterFunc))
  }

  withContext(time: string, history: HistoryProvider): HistoryRepo<T> {
    this.#windbackTime = time
    this.#history = history
    return this
  }

  getIndex(index: keyof T, value: any): Promise<T[]> {
    throw new Error('Method not implemented.')
  }

  watchSearch(
    query: string,
    opts?: { showAllOnEmpty?: boolean },
    filters?:
      | {
          func?: ((ent: T) => boolean) | undefined
          query?: DeepPartial<T> | undefined
        }
      | undefined,
  ): Observable<T[]> {
    throw new Error('Method not implemented.')
  }
}
