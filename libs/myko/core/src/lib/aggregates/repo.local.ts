import {
  MEventType,
  MItem,
  addMissingHash,
  type DeepPartial,
  type ID,
  type MEvent,
} from '../types'
import { unwrapItem } from '../wrappers'
import { Repo, buildFilter, toArray, type RepoOptions } from './repo.abstract'
import { Store } from './store'

/**
 * Represents an abstract repository for managing items of type T.
 * Provides methods for querying and manipulating the underlying store.
 *
 * @template T - The type of items stored in the repository.
 */
export class LocalRepo<T extends MItem> extends Repo<T> {
  private readonly store: Store<T>

  constructor(
    readonly entity: string,
    protected readonly options: RepoOptions<T>,
  ) {
    super(entity, options)

    this.store = new Store({ enableLogs: !!options?.enableLogs })

    if (this.options?.indeces) {
      this.store.createIndeces(this.options.indeces)
    }
  }

  async save(event: MEvent<T>): Promise<MEvent<T>> {
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
    return event
  }

  /**
   * Retrieves an item from the store based on the provided ID.
   *
   * @param id The ID of the item to retrieve.
   * @returns The retrieved item if found, otherwise null.
   */
  async getId(id: ID): Promise<T | null> {
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
  async getIndex(index: keyof T, value: any): Promise<T[]> {
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
  async get(query: DeepPartial<T>): Promise<T[]> {
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
  async getFilter(filterFunc: (ent: T) => boolean): Promise<T[]> {
    const arr = toArray(this.store.getFilter(filterFunc))
    return arr
  }

  async getItemCount(): Promise<number> {
    return this.store.size
  }
}
