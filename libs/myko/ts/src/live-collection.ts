export type Identified = { id: string }

export type LiveCollectionStatus = 'loading' | 'live' | 'error'

export type CollectionDiff<T> = {
  sequence: bigint
  deletes: readonly string[]
  upserts: readonly T[]
}

export type CollectionChanges<T> = {
  sequence: bigint | null
  reset: boolean
  deletes: readonly string[]
  upserts: readonly T[]
}

export interface MutableKeyedCollection<T> {
  clear(): void
  delete(id: string): boolean
  set(id: string, item: T): unknown
}

const EMPTY_CHANGES: CollectionChanges<never> = {
  sequence: null,
  reset: false,
  deletes: [],
  upserts: [],
}

/** Apply a Myko collection diff to any Map-compatible reactive collection. */
export function applyCollectionDiff<T extends Identified>(
  target: MutableKeyedCollection<T>,
  diff: CollectionDiff<T>,
): CollectionChanges<T> {
  const reset = diff.sequence === 0n
  if (reset) target.clear()
  for (const id of diff.deletes) target.delete(id)
  for (const item of diff.upserts) target.set(item.id, item)

  return {
    sequence: diff.sequence,
    reset,
    deletes: diff.deletes,
    upserts: diff.upserts,
  }
}

/**
 * Stable, read-only keyed state for a live query or view.
 *
 * Diffs update the backing map in O(changes). Array materialization is lazy and
 * memoized per revision, so row-oriented consumers never pay an O(result size)
 * cost for an unrelated one-row update.
 */
export class LiveCollection<T extends Identified> {
  readonly #items = new Map<string, T>()
  #array: readonly T[] | undefined
  #status: LiveCollectionStatus = 'loading'
  #error: Error | undefined
  #sequence: bigint | null = null
  #revision = 0
  #changes = EMPTY_CHANGES as CollectionChanges<T>

  get items(): ReadonlyMap<string, T> {
    return this.#items
  }

  get size(): number {
    return this.#items.size
  }

  get status(): LiveCollectionStatus {
    return this.#status
  }

  get resolved(): boolean {
    return this.#sequence !== null
  }

  get error(): Error | undefined {
    return this.#error
  }

  get sequence(): bigint | null {
    return this.#sequence
  }

  /** Monotonically increases for every diff or terminal error notification. */
  get revision(): number {
    return this.#revision
  }

  /** The latest incremental change, suitable for framework-native adapters. */
  get changes(): CollectionChanges<T> {
    return this.#changes
  }

  get(id: string): T | undefined {
    return this.#items.get(id)
  }

  has(id: string): boolean {
    return this.#items.has(id)
  }

  entries(): IterableIterator<[string, T]> {
    return this.#items.entries()
  }

  /** Lazily materialize a stable array for the current revision. */
  toArray(): readonly T[] {
    this.#array ??= Array.from(this.#items.values())
    return this.#array
  }

  apply(diff: CollectionDiff<T>): this {
    this.#changes = applyCollectionDiff(this.#items, diff)
    this.#sequence = diff.sequence
    this.#status = 'live'
    this.#error = undefined
    this.#revision += 1
    this.#array = undefined
    return this
  }

  fail(error: unknown): this {
    this.#status = 'error'
    this.#error = error instanceof Error ? error : new Error(String(error))
    this.#revision += 1
    this.#changes = EMPTY_CHANGES as CollectionChanges<T>
    return this
  }
}

const EMPTY_GROUP = new Map<never, never>()

export type LiveIndexChanges<K> = {
  reset: boolean
  keys: readonly K[]
}

/**
 * Stable incremental secondary index over a `LiveCollection`.
 *
 * The first update groups the current snapshot. Consecutive source revisions
 * then touch only deleted and upserted items, using cached prior keys to move
 * an item between buckets without scanning the source collection. If a caller
 * skips revisions, `update` safely rebuilds from the authoritative source.
 */
export class LiveIndex<K, T extends Identified> {
  readonly #keyOf: (item: T) => K
  readonly #groups = new Map<K, Map<string, T>>()
  readonly #itemKeys = new Map<string, K>()
  readonly #arrays = new Map<K, readonly T[]>()
  #source: LiveCollection<T> | undefined
  #sourceRevision = -1
  #revision = 0
  #changes: LiveIndexChanges<K> = { reset: false, keys: [] }

  constructor(keyOf: (item: T) => K) {
    this.#keyOf = keyOf
  }

  get groups(): ReadonlyMap<K, ReadonlyMap<string, T>> {
    return this.#groups
  }

  get size(): number {
    return this.#groups.size
  }

  get status(): LiveCollectionStatus {
    return this.#source?.status ?? 'loading'
  }

  get resolved(): boolean {
    return this.#source?.resolved ?? false
  }

  get error(): Error | undefined {
    return this.#source?.error
  }

  get sequence(): bigint | null {
    return this.#source?.sequence ?? null
  }

  /** Increases once for every source revision incorporated by this index. */
  get revision(): number {
    return this.#revision
  }

  /** Last incorporated `LiveCollection.revision`, or -1 before first update. */
  get sourceRevision(): number {
    return this.#sourceRevision
  }

  /** Endpoint buckets affected by the latest incorporated source revision. */
  get changes(): LiveIndexChanges<K> {
    return this.#changes
  }

  has(key: K): boolean {
    return this.#groups.has(key)
  }

  /** A stable keyed bucket, or a shared immutable empty bucket. */
  get(key: K): ReadonlyMap<string, T> {
    return this.#groups.get(key) ?? (EMPTY_GROUP as ReadonlyMap<string, T>)
  }

  /** Lazily materialize and memoize one bucket for the current revision. */
  values(key: K): readonly T[] {
    const cached = this.#arrays.get(key)
    if (cached) return cached
    const values = Array.from(this.get(key).values())
    this.#arrays.set(key, values)
    return values
  }

  update(source: LiveCollection<T>): this {
    if (source.revision === this.#sourceRevision) return this

    const changedKeys = new Set<K>()
    const consecutive = source.revision === this.#sourceRevision + 1
    if (this.#sourceRevision < 0 || !consecutive || source.changes.reset) {
      for (const key of this.#groups.keys()) changedKeys.add(key)
      this.#rebuild(source.items.values())
      for (const key of this.#groups.keys()) changedKeys.add(key)
      this.#changes = { reset: true, keys: Array.from(changedKeys) }
    } else {
      for (const id of source.changes.deletes) this.#remove(id, changedKeys)
      for (const item of source.changes.upserts) this.#upsert(item, changedKeys)
      this.#changes = { reset: false, keys: Array.from(changedKeys) }
    }

    this.#source = source
    this.#sourceRevision = source.revision
    this.#revision += 1
    return this
  }

  #rebuild(items: Iterable<T>): void {
    for (const group of this.#groups.values()) group.clear()
    this.#groups.clear()
    this.#itemKeys.clear()
    this.#arrays.clear()
    for (const item of items) this.#insert(item)
  }

  #insert(item: T): void {
    const key = this.#keyOf(item)
    this.#insertAt(key, item)
  }

  #insertAt(key: K, item: T): void {
    let group = this.#groups.get(key)
    if (!group) {
      group = new Map<string, T>()
      this.#groups.set(key, group)
    }
    group.set(item.id, item)
    this.#itemKeys.set(item.id, key)
    this.#arrays.delete(key)
  }

  #upsert(item: T, changedKeys: Set<K>): void {
    const key = this.#keyOf(item)
    if (this.#itemKeys.has(item.id)) {
      const previousKey = this.#itemKeys.get(item.id) as K
      const previousGroup = this.#groups.get(previousKey)
      // Comparing the resolved buckets uses the Map's SameValueZero key
      // semantics and preserves a stable bucket when membership is unchanged.
      if (previousGroup && previousGroup === this.#groups.get(key)) {
        previousGroup.set(item.id, item)
        this.#itemKeys.set(item.id, key)
        this.#arrays.delete(previousKey)
        changedKeys.add(previousKey)
        return
      }
      this.#remove(item.id, changedKeys)
    }
    this.#insertAt(key, item)
    changedKeys.add(key)
  }

  #remove(id: string, changedKeys?: Set<K>): void {
    if (!this.#itemKeys.has(id)) return
    const key = this.#itemKeys.get(id) as K
    const group = this.#groups.get(key)
    group?.delete(id)
    this.#itemKeys.delete(id)
    this.#arrays.delete(key)
    changedKeys?.add(key)
    if (group?.size === 0) this.#groups.delete(key)
  }
}
