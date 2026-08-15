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
