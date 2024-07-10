export class MultiMap<K, V> {
  private reverseMap: Map<V, K> = new Map()
  private map: Map<K, Set<V>> = new Map()

  add(key: K, value: V) {
    if (!this.map.has(key)) {
      this.map.set(key, new Set())
    }

    this.reverseMap.set(value, key)
    this.map.get(key).add(value)
  }

  removeValue(value: V): K | null {
    if (!this.reverseMap.has(value)) {
      return
    }

    const key = this.reverseMap.get(value)
    return this.remove(key, value)
  }

  remove(key: K, value: V): K | null {
    if (!this.map.has(key)) {
      return null
    }

    this.map.get(key).delete(value)

    if (this.map.get(key).size === 0) {
      this.map.delete(key)
      return key
    }
    return null
  }

  get(key: K): Set<V> {
    return this.map.get(key)
  }

  has(key: K): boolean {
    return this.map.has(key)
  }
}
