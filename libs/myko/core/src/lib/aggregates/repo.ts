import {
  combineLatest,
  distinctUntilChanged,
  filter,
  finalize,
  map,
  mergeWith,
  Observable,
  of,
  scan,
  startWith,
  Subject,
  switchMap,
  tap,
} from 'rxjs'
import { ID, MEvent, MEventType, MItem } from '../types'
import { unwrapItem } from '../wrappers'
import { Store } from './store'
import { getIds, getFilters, watchIds } from '../registry'
import { MYKO_ITEM_TYPE } from '../constants'

export interface RepoOptions<T extends MItem> {
  // stream of events to provide realtime updates. need not be filtered for the entity type
  stream?: Observable<MEvent<T>>

  // arbitrary onEvent hook (mostly for debugging, or logging)
  onEvent?: (event: MEvent<T>) => void

  // connect this repo without requring the scope parameter
  noScope?: boolean

  // debug options
  enableLogs?: boolean

  // indeces
  indeces?: (keyof T)[]

  peerQuery?: (ids: ID[]) => Observable<T[]>
}

export abstract class Repo<T extends MItem> {
  private readonly store: Store<T>
  private subject: Subject<MEvent<T>>
  private entity: string

  private all$: Subject<Map<ID, T>> = new Subject<Map<ID, T>>()

  constructor(
    ent: new (...args: any[]) => T,
    private readonly options?: RepoOptions<T>,
  ) {
    this.entity = Reflect.getMetadata(MYKO_ITEM_TYPE, ent)

    getIds.set(this.entity, this.getIds.bind(this))
    getFilters.set(this.entity, this.getFilter.bind(this))
    watchIds.set(this.entity, this.watchIds.bind(this))

    this.store = new Store()

    this.subject = new Subject()

    if (this.options?.stream) {
      this.options.stream
        .pipe(
          filter((event) => event.itemType === this.entity),
          tap((event: MEvent<MItem>) => {
            if (!options?.onEvent) {
              return
            }
            options.onEvent(event as MEvent<T>)
          }),
        )
        .subscribe((event) => {
          this.subject.next(event as MEvent<T>)
        })
    }

    if (this.options?.indeces) {
      this.store.createIndeces(this.options.indeces)
    }

    this.subject.subscribe((event: MEvent<T>) => {
      switch (event.changeType) {
        case MEventType.SET:
          this.store.set(event.item.id, unwrapItem(event) as T)
          break
        case MEventType.DEL:
          this.store.delete(event.item.id)
          break
      }
    })
  }

  watchFilter(filterFunc: (ent: T) => boolean): Observable<T[]> {
    return this.all$.pipe(
      startWith(this.store.getFilter(filterFunc)),
      switchMap((all) =>
        this.subject.pipe(
          scan(
            (acc, event) => {
              acc.changed = false
              if (event.changeType === MEventType.DEL) {
                acc.lookup.delete(event.item.id)
                acc.changed = true
              }
              if (
                event.changeType === MEventType.SET &&
                filterFunc(event.item)
              ) {
                acc.lookup.set(event.item.id, unwrapItem(event) as T)
                acc.changed = true
              }
              if (
                event.changeType === MEventType.SET &&
                !filterFunc(event.item)
              ) {
                if (acc.lookup.has(event.item.id)) {
                  acc.lookup.delete(event.item.id)
                  acc.changed = true
                }
              }
              return acc
            },
            { lookup: all, changed: false },
          ),
          filter((e) => e.changed),
          map((e) => e.lookup),
          startWith(this.store.getFilter(filterFunc)),
        ),
      ),
      map(toArray),
    )
  }

  watchId(id: ID): Observable<T | null> {
    const obs = this.subject.pipe(
      filter((e) => e.item.id === id),
      map((event) => {
        switch (event.changeType) {
          case MEventType.SET:
            return unwrapItem(event)
          case MEventType.DEL:
            return null
        }
      }),
      startWith(this.store.get(id) ?? null),
      mergeWith(this.all$.pipe(map((e) => e.get(id) ?? null))),
    )

    return obs as Observable<T | null>
  }

  watchIds(ids: ID[]): Observable<T[]> {
    if (ids.length === 0) {
      return of([])
    }
    return combineLatest(ids.map((id) => this.watchId(id).pipe())).pipe(
      map((x) => x.filter((y) => y !== null)),
    )
  }

  getIds(ids: ID[]) {
    return ids.map((id) => this.getId(id)).filter((x) => x !== null) as T[]
  }

  watch(query: Partial<T>): Observable<T[]> {
    const filterFunc = buildFilter(query)

    const ret = this.watchFilter(filterFunc)

    if (!ret) {
      throw new Error('Error making or getting stream')
    }
    return ret
  }

  getId(id: ID): T | null {
    return this.store.get(id) ?? null
  }

  getIndex(index: keyof T, value: any): T[] {
    try {
      return this.store.getIndex(index, value)
    } catch (e) {
      console.warn(
        `No index crated on ${index.toString()} for ${
          this.entity
        } - falling back`,
      )
      return this.getFilter((el) => el[index] === value)
    }
  }

  get(query: Partial<T>): T[] {
    const filterFunc = buildFilter(query)
    return toArray(this.store.getFilter(filterFunc))
  }

  getFilter(filterFunc: (ent: T) => boolean): T[] {
    return toArray(this.store.getFilter(filterFunc))
  }
}

const toArray = <T>(m: Map<string, T>): T[] => [...m.values()]

const buildFilter =
  <T extends MItem>(query: Partial<T>) =>
  (ent: T) =>
    Reflect.ownKeys(query).every(
      (key) => Reflect.get(query, key) === Reflect.get(ent, key),
    )
