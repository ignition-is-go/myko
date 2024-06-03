import {
  combineLatest,
  filter,
  map,
  Observable,
  of,
  scan,
  startWith,
  Subject,
  tap,
} from 'rxjs'
import { MYKO_ITEM_TYPE } from '../constants'
import { getFilters, getIds, watchIds } from '../registry'
import {
  addMissingHash,
  DeepPartial,
  ID,
  MEvent,
  MEventType,
  MItem,
} from '../types'
import { unwrapItem } from '../wrappers'
import { Store } from './store'

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

/**
 * Represents an abstract repository for managing items of type T.
 * Provides methods for querying and manipulating the underlying store.
 *
 * @template T - The type of items stored in the repository.
 */
export abstract class Repo<T extends MItem> {
  private readonly store: Store<T>
  private subject: Subject<MEvent<T>>
  private entity: string

  constructor(
    ent: new (...args: any[]) => T,
    private readonly options?: RepoOptions<T>,
  ) {
    this.entity = Reflect.getMetadata(MYKO_ITEM_TYPE, ent)

    getIds.set(this.entity, this.getIds.bind(this))
    getFilters.set(this.entity, this.getFilter.bind(this))
    watchIds.set(this.entity, this.watchIds.bind(this))

    this.store = new Store({ enableLogs: options?.enableLogs })

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
          const item = unwrapItem(event) as T

          const hashedItem = addMissingHash(item) as T

          this.store.set(event.item.id, hashedItem)
          break
        case MEventType.DEL:
          this.store.delete(event.item.id)
          break
      }
    })
  }

  safeLog(...args: any[]) {
    if (this.options?.enableLogs) {
      console.log(...args)
    }
  }

  /**
   * Returns an Observable that emits an array of filtered items whenever there is a change in the store.
   * The items emitted are filtered based on the provided filter function.
   *
   * @param filterFunc The filter function used to determine which items to include in the emitted array.
   * @returns An Observable that emits an array of filtered items.
   */
  watchFilter(filterFunc: (ent: T) => boolean): Observable<T[]> {
    const init = this.store.getFilter(filterFunc)
    return this.subject.pipe(
      scan(
        (acc, event) => {
          acc.changed = false
          if (event.changeType === MEventType.DEL) {
            acc.lookup.delete(event.item.id)
            acc.changed = true
          }
          if (event.changeType === MEventType.SET && filterFunc(event.item)) {
            acc.lookup.set(event.item.id, unwrapItem(event) as T)
            acc.changed = true
          }
          if (event.changeType === MEventType.SET && !filterFunc(event.item)) {
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
    )
  }

  /**
   * Watches for changes to an item with the specified ID.
   * @param id The ID of the item to watch.
   * @returns An Observable that emits the item when it changes, or null if it is deleted.
   */
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
      startWith(this.getId(id)),
    )

    return obs as Observable<T | null>
  }

  /**
   * Watches multiple IDs and returns an Observable of the corresponding values.
   * If the provided array of IDs is empty or null, an empty array is returned.
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
    return combineLatest(ids.map((id) => this.watchId(id).pipe())).pipe(
      map((x) => x.filter((y) => y !== null)),
    )
  }

  /**
   * Retrieves the objects with the specified IDs.
   *
   * @param ids - An array of IDs.
   * @returns An array of objects with the specified IDs.
   */
  getIds(ids: ID[]): T[] {
    return ids.map((id) => this.getId(id)).filter((x) => x !== null) as T[]
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
   * Retrieves an item from the store based on the provided ID.
   *
   * @param id The ID of the item to retrieve.
   * @returns The retrieved item if found, otherwise null.
   */
  getId(id: ID): T | null {
    return this.store.get(id) ?? null
  }

  /**
   * Retrieves an array of entities from the store based on the specified index and value.
   * If the index does not exist, it falls back to filtering the entities based on the index and value.
   *
   * @param index - The key of the index to search for.
   * @param value - The value to match against the index.
   * @returns An array of entities that match the specified index and value.
   */
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

  /**
   * Retrieves an array of items that match the given query.
   *
   * @param query - The partial object used to filter the items.
   * @returns An array of items that match the query.
   */
  get(query: Partial<T>): T[] {
    const filterFunc = buildFilter(query)
    const arr = toArray(this.store.getFilter(filterFunc))
    return arr
  }

  /**
   * Retrieves an array of entities that match the provided filter function.
   *
   * @param filterFunc The filter function used to determine if an entity should be included in the result.
   * @returns An array of entities that match the filter function.
   */
  getFilter(filterFunc: (ent: T) => boolean): T[] {
    const arr = toArray(this.store.getFilter(filterFunc))
    return arr
  }
}

// export abstract class ControledRepo<T extends MItem> implements Controlled {
//   private readonly store: Store<T>
//   private subject: Subject<MEvent<T>>
//   private entity: string

//   private all$: Subject<Map<ID, T>> = new Subject<Map<ID, T>>()
//   constructor(
//     ent: new (...args: any[]) => T,
//     readonly listenId: (id: ID) => ID,
//     readonly release: (id: ID) => void,
//     private readonly options?: RepoOptions<T>,
//   ) {
//     this.entity = Reflect.getMetadata(MYKO_ITEM_TYPE, ent)

//     getIds.set(this.entity, this.getIds.bind(this))
//     watchIds.set(this.entity, this.watchIds.bind(this))

//     this.store = new Store({ enableLogs: options?.enableLogs })

//     this.subject = new Subject()

//     if (this.options?.stream) {
//       this.options.stream
//         .pipe(
//           filter((event) => event.itemType === this.entity),
//           tap((event: MEvent<MItem>) => {
//             if (!options?.onEvent) {
//               return
//             }
//             options.onEvent(event as MEvent<T>)
//           }),
//         )
//         .subscribe((event) => {
//           this.subject.next(event as MEvent<T>)
//         })
//     }

//     if (this.options?.indeces) {
//       this.store.createIndeces(this.options.indeces)
//     }

//     this.subject.subscribe((event: MEvent<T>) => {
//       switch (event.changeType) {
//         case MEventType.SET:
//           this.store.set(event.item.id, unwrapItem(event) as T)
//           break
//         case MEventType.DEL:
//           this.store.delete(event.item.id)
//           break
//       }
//     })
//   }

//   watchId(id: ID): Observable<T | null> {
//     const obs = this.subject.pipe(
//       filter((e) => e.item.id === id),
//       map((event) => {
//         switch (event.changeType) {
//           case MEventType.SET:
//             return unwrapItem(event)
//           case MEventType.DEL:
//             return null
//         }
//       }),
//       startWith(this.store.get(id) ?? null),
//       mergeWith(this.all$.pipe(map((e) => e.get(id) ?? null))),
//     )

//     return obs as Observable<T | null>
//   }

//   watchIds(ids: ID[]): Observable<T[]> {
//     if (!ids) {
//       return of([])
//     }
//     if (ids.length === 0) {
//       return of([])
//     }
//     return combineLatest(ids.map((id) => this.watchId(id).pipe())).pipe(
//       map((x) => x.filter((y) => y !== null)),
//     )
//   }

//   getIds(ids: ID[]) {
//     return ids.map((id) => this.getId(id)).filter((x) => x !== null) as T[]
//   }

//   getId(id: ID): T | null {
//     return this.store.get(id) ?? null
//   }
// }

const toArray = <T>(m: Map<string, T>): T[] => [...m.values()]

const buildFilter =
  <T extends MItem>(query: Partial<T>) =>
  (ent: T) =>
    objectFilter(query, ent)

const objectFilter = (query: object, ent: object) => {
  return Reflect.ownKeys(query).every((key) => {
    const querySide = Reflect.get(query, key)
    const entSide = Reflect.get(ent, key)

    if (typeof querySide === 'object' && typeof entSide === 'object') {
      return objectFilter(querySide, entSide)
    }

    return querySide === entSide
  })
}
