import flexsearch from 'flexsearch'
import { uniq } from 'ramda'
import {
  bufferTime,
  combineLatest,
  concat,
  concatMap,
  distinctUntilChanged,
  filter,
  finalize,
  from,
  map,
  type Observable,
  of,
  scan,
  shareReplay,
  startWith,
  Subject,
  switchMap,
  tap,
} from 'rxjs'
import { beforeInit, fireInit } from '../hooks'
import type { Persister } from '../persisters'
import {
  MEventType,
  type MItem,
  type DeepPartial,
  type ID,
  type MEvent,
} from '../types'
import { unwrapItem } from '../wrappers'

export interface RepoOptions<T extends MItem> {
  // stream of events to provide realtime updates. need not be filtered for the entity type
  // stream?: Observable<MEvent<T>>
  persister: Persister<T>

  // arbitrary onEvent hook (mostly for debugging, or logging)
  onEvent?: (event: MEvent<T>) => void

  // connect this repo without requring the scope parameter
  noScope?: boolean

  // debug options
  enableLogs?: boolean

  // indeces
  indeces?: (keyof T)[]

  searchIndeces?: (keyof T)[]

  peerQuery?: (ids: ID[]) => Observable<T[]>
}

export type RepoFactory = <T extends MItem>(
  itemName: string,
  options: RepoOptions<T>,
) => Repo<T>

/**
 * Represents an abstract repository for managing items of type T.
 * Provides methods for querying and manipulating the underlying store.
 *
 * @template T - The type of items stored in the repository.
 */
export abstract class Repo<T extends MItem> {
  protected subject: Subject<MEvent<T>>
  private search: flexsearch.Document<T>
  private searchObs: Subject<flexsearch.Document<T>>
  constructor(
    readonly entity: string,
    protected readonly options: RepoOptions<T>,
  ) {
    this.subject = new Subject()
    this.searchObs = new Subject()
    this.search = new flexsearch.Document({
      document: {
        id: 'id',
        index: (options.searchIndeces || []) as any as string[],
      },
      tokenize: 'full',
    })

    const loaded = new Subject<number>()
    let stopLoadingLogs = false

    loaded
      .pipe(
        bufferTime(1000),
        map((x) => x.slice(-1)[0]),
        distinctUntilChanged(),
        filter((x) => !!x),
      )

      .subscribe((last) => {
        const bar = '█'.repeat(Math.floor(last * 10))
        const space = ' '.repeat(10 - bar.length)

        import('../logger').then(({ MykoLogger }) => {
          if (stopLoadingLogs) {
            return
          }

          new MykoLogger(entity).info(
            `Loading [${bar}${space}] ${Math.floor(last * 100)}%`,
          )
        })
      })

    beforeInit(this.entity)
    const stream = concat(
      this.options.persister.load.pipe(
        concatMap((load) =>
          from(
            this.save(load.event).then((x) => {
              loaded.next(load.percent)
              return x
            }),
          ),
        ),

        finalize(() => {
          stopLoadingLogs = true
          loaded.complete()
          fireInit(this.entity)
        }),
      ),
      this.options.persister.output.pipe(
        filter((x) => !!x),
        concatMap((x) => from(this.save(x))),
      ),
    )

    stream
      .pipe(
        filter((event) => event.itemType === this.entity),
        tap((event: MEvent<T>) => {
          if (options?.searchIndeces && options.searchIndeces.length > 0) {
            if (event.changeType === MEventType.SET) {
              this.search.add(event.item)
            }

            if (event.changeType === MEventType.DEL) {
              this.search.remove(event.item.id)
            }

            this.searchObs.next(this.search)
          }

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

  safeLog(...args: any[]) {
    if (this.options?.enableLogs) {
      console.log(...args)
    }
  }

  /**
   * Watches for changes to an item with the specified ID.
   * @param id The ID of the item to watch.
   * @returns An Observable that emits the item when it changes, or null if it is deleted.
   */
  watchId(id: ID): Observable<T | null> {
    const init = this.getId(id)

    const obs = this.subject.pipe(
      filter((e) => e.item.id === id),
      map((event) => {
        switch (event.changeType) {
          case MEventType.SET:
            return unwrapItem(event) as T
          case MEventType.DEL:
            return null
        }
      }),
    )

    return concat(init, obs)
  }

  // Performance optimization: Cache watchId observables to prevent subscription explosion
  private watchCache = new Map<ID, Observable<T | null>>()

  /**
   * Watches multiple IDs and returns an Observable of the corresponding values.
   * If the provided array of IDs is empty or null, an empty array is returned.
   *
   * Performance optimized: Uses shareReplay to cache individual ID subscriptions,
   * preventing exponential subscription growth when watching many IDs.
   *
   * @param ids - An array of IDs to watch.
   * @returns An Observable that emits an array of values corresponding to the watched IDs.
   */
  watchIds(ids: ID[]): Observable<T[]> {
    if (!ids) {
      return of([])
    }
    if (ids.length === 0) {
      return of([])
    }

    // Use cached observables with shareReplay to prevent subscription explosion
    const observables = ids.map((id) => {
      if (!this.watchCache.has(id)) {
        this.watchCache.set(
          id,
          this.watchId(id).pipe(
            shareReplay({ bufferSize: 1, refCount: true }),
          ),
        )
      }
      return this.watchCache.get(id)!
    })

    return combineLatest(observables).pipe(map((x) => x.filter((y) => y !== null)))
  }

  /**
   * Returns an Observable that emits an array of filtered items whenever there is a change in the store.
   * The items emitted are filtered based on the provided filter function.
   *
   * @param filterFunc The filter function used to determine which items to include in the emitted array.
   * @returns An Observable that emits an array of filtered items.
   */
  watchFilter(filterFunc: (ent: T) => boolean): Observable<T[]> {
    const init = this.getFilter(filterFunc)

    return from(init).pipe(
      map((init) => new Map(init.map((item) => [item.id, item]))),
      switchMap((init) =>
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
            { lookup: init, changed: true },
          ),
          filter((e) => e.changed),
          map((e) => e.lookup),
          startWith(init),
          map(toArray),
        ),
      ),
    )
  }

  /**
   * Watches for changes in the repository based on the provided query.
   * @param query - The query used to filter the changes.
   * @returns An Observable that emits an array of items that match the query.
   * @throws Error if there is an error making or getting the stream.
   */
  watch(query: DeepPartial<T>): Observable<T[]> {
    const filterFunc = buildFilter(query)

    const ret = this.watchFilter(filterFunc)

    if (!ret) {
      throw new Error('Error making or getting stream')
    }
    return ret
  }

  /**
   * Retrieves an array of items that match the provided search.
   *
   * @param query - The query used to filter the items.
   * @returns An array of items that match the query.
   */
  async getSearch(query: string): Promise<T[]> {
    return Promise.all(
      this.search
        .search(query.toLocaleLowerCase())
        .map((x) => this.getId(x.toString())),
    ).then((x) => x.filter((x) => x !== null))
  }

  watchSearch(
    query: string,
    opts?: { showAllOnEmpty?: boolean },
    filters?: {
      func?: (ent: T) => boolean
      query?: DeepPartial<T>
    },
  ): Observable<T[]> {
    const filterFunc = filters?.func || buildFilter(filters?.query || {})

    if (query === '' && opts?.showAllOnEmpty) {
      return this.watchFilter(filterFunc)
    }

    return this.searchObs.pipe(
      startWith(this.search),
      map((s) =>
        s
          .search(query.toLocaleLowerCase(), Infinity, { suggest: true })
          .flatMap((x) => x.result),
      ),
      map(uniq),
      switchMap((ids) =>
        ids.length === 0
          ? of([])
          // Use watchIds for cached subscriptions - prevents subscription explosion
          : this.watchIds(ids.map((id) => id.toString())),
      ),
      map((x) => x.filter(filterFunc)),
    )
  }

  /**
   * Retrieves an item from the store based on the provided ID.
   *
   * @param id The ID of the item to retrieve.
   * @returns The retrieved item if found, otherwise null.
   */
  abstract getId(id: ID): Promise<T | null>

  /**
   * Retrieves the objects with the specified IDs.
   *
   * @param ids - An array of IDs.
   * @returns An array of objects with the specified IDs.
   */
  async getIds(ids: ID[]): Promise<T[]> {
    return await Promise.all(ids.map((id) => this.getId(id))).then((x) =>
      x.filter((x) => x !== null),
    )
  }

  /**
   * Retrieves an array of entities from the store based on the specified index and value.
   * If the index does not exist, it falls back to filtering the entities based on the index and value.
   *
   * @param index - The key of the index to search for.
   * @param value - The value to match against the index.
   * @returns An array of entities that match the specified index and value.
   */
  abstract getIndex(index: keyof T, value: any): Promise<T[]>

  /**
   * Retrieves an array of items that match the given query.
   *
   * @param query - The partial object used to filter the items.
   * @returns An array of items that match the query.
   */
  abstract get(query: DeepPartial<T>): Promise<T[]>

  /**
   * Retrieves an array of entities that match the provided filter function.
   *
   * @param filterFunc The filter function used to determine if an entity should be included in the result.
   * @returns An array of entities that match the filter function.
   */
  abstract getFilter(filterFunc: (ent: T) => boolean): Promise<T[]>

  abstract save(event: MEvent<T>): Promise<MEvent<T>>

  abstract getItemCount(): Promise<number>
}

export const toArray = <T>(m: Map<string, T>): T[] => [...m.values()]

export const buildFilter =
  <T extends MItem>(query: Partial<T>): ((ent: T) => boolean) =>
  (ent: T): boolean => {
    try {
      return objectFilter(query, ent)
    } catch (e) {
      import('../logger').then(({ MykoLogger }) => {
        const logger = new MykoLogger('AbstractRepo')
        logger.error('Error building filter:', e)
      })
      return false
    }
  }

export const objectFilter = (query: object, ent: object): boolean => {
  return Reflect.ownKeys(query).every((key) => {
    const querySide = Reflect.get(query, key)
    const entSide = Reflect.get(ent, key)

    if (querySide === null) {
      return true
    }

    if (Array.isArray(querySide) && Array.isArray(entSide)) {
      return entSide.every((item, index) =>
        objectFilter(querySide[index], item),
      )
    }

    try {
      if (typeof querySide === 'object' && typeof entSide === 'object') {
        return objectFilter(querySide, entSide)
      }
    } catch (_e) {
      throw Error(
        `Error filtering object: query: ${JSON.stringify(querySide)}: ${typeof querySide}, ent: ${JSON.stringify(entSide)}: ${typeof entSide}`,
      )
    }

    return querySide === entSide
  })
}

export type IRepo<T extends MItem> = Omit<
  Repo<T>,
  'entity' | 'safeLog' | 'subject' | 'search' | 'searchObs' | 'save'
>
