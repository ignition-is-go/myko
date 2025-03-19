import { firstValueFrom, from, type Observable } from 'rxjs'
import { type IRepo } from '../aggregates/repo.abstract'
import type { DeepPartial, ID, MItem } from '../types'
import type { HistoryProvider } from './abstract.history'

export class HistoryRepo<T extends MItem> implements IRepo<T> {
  constructor(
    protected entity: string,
    private history: HistoryProvider,
    private windbackTime: string,
  ) {}

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

  getIds(ids: ID[]): Promise<T[]> {
    return Promise.all(ids.map((id) => this.getId(id))).then(
      (res) => res.filter((ent) => ent !== null) as T[],
    )
  }

  getId(id: ID): Promise<T | null> {
    return this.history.getItemAsOfTime<T>(id, this.entity, this.windbackTime)
  }

  get(query: DeepPartial<T>): Promise<T[]> {
    return this.history.getItemsByQueryAsOfTime<T>(
      query,
      this.entity,
      this.windbackTime,
    )
  }

  getFilter(filterFunc: (ent: T) => boolean): Promise<T[]> {
    return this.history
      .getAllItemsAsOfTime<T>(this.entity, this.windbackTime)
      .then((res) => res.filter(filterFunc))
  }

  getIndex(_index: keyof T, _value: any): Promise<T[]> {
    throw new Error('Method not implemented.')
  }

  watchSearch(
    _query: string,
    _opts?: { showAllOnEmpty?: boolean },
    _filters?:
      | {
          func?: ((ent: T) => boolean) | undefined
          query?: DeepPartial<T> | undefined
        }
      | undefined,
  ): Observable<T[]> {
    throw new Error('Method not implemented.')
  }

  getSearch(_query: string): Promise<T[]> {
    return firstValueFrom(this.watchSearch(_query))
  }
}
