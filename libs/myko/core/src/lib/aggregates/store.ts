import { ID, MItem } from '../types'

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

  set(key: string, value: T): this {
    super.set(key, value)
    this.addToIndeces(value)
    return this
  }

  delete(id: string): boolean {
    const val = super.delete(id)
    if (val) {
      this.removeFromIndeces(id)
    }
    return val
  }

  setMany(els: T[]): void {
    els.forEach((el) => {
      this.set(el.id, el)
    })
  }

  deleteMany(els: T[]): void {
    els.forEach((el) => {
      this.delete(el.id)
    })
  }

  getAll(): Map<ID, T> {
    return new Map(this)
  }

  getFilter(filterFunc: (el: T) => boolean): Map<ID, T> {
    const map = [...this.entries()]
      .filter(([_, val]) => filterFunc(val))
      .reduce((m, [key, val]) => {
        m.set(key, val)
        return m
      }, new Map())
    return map
  }

  getIndex(index: keyof T, value: any): T[] {
    const propIndex = this.indeces.get(index)

    if (!propIndex) {
      throw new Error()
    }

    return [...(propIndex?.get(value)?.values() ?? [])]
      .map((id) => this.get(id))
      .filter((x) => !!x) as T[]
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
    ;[...this.indeces.values()].forEach((propIndex) =>
      [...propIndex.values()].forEach((set) => set.delete(key)),
    )
  }

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
