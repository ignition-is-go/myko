export class MultiMap<K, V> {
  private reverseMap: Map<V, K> = new Map()
  private map: Map<K, Set<V>> = new Map()

  add(key: K, value: V) {
    if (!this.map.has(key)) {
      this.map.set(key, new Set())
    }

    this.reverseMap.set(value, key)
    const valueSet = this.map.get(key)
    if (valueSet) {
      valueSet.add(value)
    }
  }

  removeValue(value: V): K | null {
    if (!this.reverseMap.has(value)) {
      return null
    }

    const key = this.reverseMap.get(value)
    if (key === undefined) {
      return null
    }
    return this.remove(key, value)
  }

  remove(key: K, value: V): K | null {
    if (!this.map.has(key)) {
      return null
    }

    const valueSet = this.map.get(key)
    if (!valueSet) {
      return null
    }

    valueSet.delete(value)

    if (valueSet.size === 0) {
      this.map.delete(key)
      return key
    }
    return null
  }

  get(key: K): Set<V> {
    const valueSet = this.map.get(key)
    if (!valueSet) {
      return new Set<V>()
    }
    return valueSet
  }

  has(key: K): boolean {
    return this.map.has(key)
  }
}
