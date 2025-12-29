import type { ID, MItem } from '../types'

export class Store<T extends MItem> extends Map<ID, T> {
  indeces = new Map<keyof T, Map<any, Set<ID>>>()

  constructor(private opts: { enableLogs: boolean }) {
    super()
  }

  safeLog(...args: any[]) {
    if (this.opts.enableLogs) {
      console.log(...args)
    }
  }

  /**
   * Sets the value for the specified key in the store.
   *
   * @param key - The key to set the value for.
   * @param value - The value to set.
   * @returns The updated store instance.
   */
  set(key: string, value: T): this {
    super.set(key, value)
    this.addToIndeces(value)
    return this
  }

  /**
   * Deletes an item from the store.
   *
   * @param id - The ID of the item to delete.
   * @returns `true` if the item was successfully deleted, `false` otherwise.
   */
  delete(id: string): boolean {
    const val = super.delete(id)
    if (val) {
      this.removeFromIndeces(id)
    }
    return val
  }

  /**
   * Sets multiple elements in the store.
   *
   * @param els - An array of elements to be set in the store.
   */
  setMany(els: T[]): void {
    els.forEach((el) => {
      this.set(el.id, el)
    })
  }

  /**
   * Deletes multiple elements from the store.
   *
   * @param els - An array of elements to be deleted.
   */
  deleteMany(els: T[]): void {
    els.forEach((el) => {
      this.delete(el.id)
    })
  }

  /**
   * Retrieves all items from the store.
   *
   * @returns A Map containing all items in the store.
   */
  getAll(): Map<ID, T> {
    return new Map(this)
  }

  /**
   * Retrieves a filtered map of entries from the store based on the provided filter function.
   *
   * @param filterFunc The filter function used to determine which entries to include in the filtered map.
   *                   It takes an element of type T as a parameter and should return a boolean value.
   *                   If the function returns true, the element will be included in the filtered map.
   *                   If the function returns false, the element will be excluded from the filtered map.
   * @returns A map containing the filtered entries from the store.
   */
  getFilter(filterFunc: (el: T) => boolean): Map<ID, T> {
    // Avoid intermediate array allocations - iterate directly
    const result = new Map<ID, T>()
    for (const [key, val] of this) {
      if (filterFunc(val)) {
        result.set(key, val)
      }
    }
    return result
  }

  /**
   * Retrieves an array of items from the store based on the specified index and value.
   *
   * @param index - The key of the index to search.
   * @param value - The value to search for in the index.
   * @returns An array of items that match the specified index and value.
   * @throws Error if the specified index does not exist in the store.
   */
  getIndex(index: keyof T, value: any): T[] {
    const propIndex = this.indeces.get(index)

    if (!propIndex) {
      throw new Error()
    }

    return [...(propIndex?.get(value)?.values() ?? [])].map((id) => this.get(id)).filter((x) => !!x) as T[]
  }

  private addToIndeces(el: T) {
    ;[...this.indeces.entries()].forEach(([property, valueMap]) => {
      if (!valueMap.has(el[property])) {
        valueMap.set(el[property], new Set())
      }
      valueMap.get(el[property])?.add(el.id)
    })
  }

  private removeFromIndeces(key: ID) {
    ;[...this.indeces.values()].forEach((propIndex) => [...propIndex.values()].forEach((set) => set.delete(key)))
  }

  /**
   * Creates indexes for the specified properties.
   * @param properties - An array of property names to create indexes for.
   */
  createIndeces(properties: (keyof T)[]) {
    const all = [...this.getAll().values()]

    properties.forEach((property) => {
      if (!this.indeces.has(property)) {
        this.indeces.set(property, new Map())
      }

      const propertyIndex = this.indeces.get(property)

      if (!propertyIndex) {
        throw new Error('Error creating index')
      }
    })
    all.forEach((el) => {
      this.addToIndeces(el)
    })
  }
}
