/**
 * Reactive Client for Svelte 5
 *
 * Provides reactive query and report subscriptions that:
 * - Cache by query/report identity to avoid redundant subscriptions
 * - Return SvelteMap for O(1) lookups
 * - Auto-cleanup via reference counting
 * - Apply deltas directly (no Observable in $derived antipattern)
 */

import { onDestroy } from 'svelte'
import { SvelteMap } from 'svelte/reactivity'
import type { ID, MItem, MQuery, MReport, MReportResult } from '@myko/core'
import type { QueryDelta } from '@myko/ws'
import { catchError, of, startWith, type Subscription } from 'rxjs'
import { client } from './client.js'

/**
 * Cache entry for queries - uses $state class fields for reactivity
 */
class QueryCacheEntry<T extends MItem> {
  items = $state<T[]>([])
  byId = new SvelteMap<ID, T>()
  isLoading = $state(true)
  error = $state<string | null>(null)
  refCount = 0
  subscription: Subscription | null = null
}

/**
 * Cache entry for reports - uses $state class fields for reactivity
 */
class ReportCacheEntry<T> {
  data = $state<T | null>(null)
  isLoading = $state(true)
  error = $state<string | null>(null)
  refCount = 0
  subscription: Subscription | null = null
}

// Result type returned to consumers for queries
export type ReactiveQueryResult<T extends MItem> = {
  readonly items: T[]
  readonly byId: SvelteMap<ID, T>
  readonly isLoading: boolean
  readonly error: string | null
  release: () => void
}

// Result type returned to consumers for reports
export type ReactiveReportResult<T> = {
  readonly data: T | null
  readonly isLoading: boolean
  readonly error: string | null
  release: () => void
}

class ReactiveClient {
  #queryCache = new Map<string, QueryCacheEntry<any>>()
  #reportCache = new Map<string, ReportCacheEntry<any>>()

  /**
   * Get a cache key for a query based on queryId + serialized params
   */
  #getQueryKey(query: MQuery): string {
    const wrapped = query.wrap()
    // Serialize query params (excluding tx which is unique per instance)
    const params = JSON.stringify(query, (key, value) =>
      key === 'tx' || key === 'createdAt' ? undefined : value,
    )
    return `${wrapped.queryId}:${params}`
  }

  #getReportKey(report: MReport<unknown>): string {
    const wrapped = report.wrap()
    const params = JSON.stringify(report, (key, value) =>
      key === 'tx' || key === 'createdAt' ? undefined : value,
    )
    return `${wrapped.reportId}:${params}`
  }

  /**
   * Get reactive query results. Returns cached entry if exists, otherwise creates new.
   * Call release() when done to decrement ref count.
   *
   * @example
   * ```typescript
   * $effect(() => {
   *   const result = reactiveClient.query(() => new GetTargets({ sceneId }))
   *   // Access reactive state
   *   const items = result.items
   *   const byId = result.byId
   *   return () => result.release()
   * })
   * ```
   */
  query<T extends MQuery>(
    queryFactory: () => T,
  ): ReactiveQueryResult<T['$queryResult'][number]> {
    const query = queryFactory()
    const key = this.#getQueryKey(query)

    let entry = this.#queryCache.get(key) as
      | QueryCacheEntry<T['$queryResult'][number]>
      | undefined

    if (!entry) {
      // Create new cache entry (class instance with $state fields)
      entry = new QueryCacheEntry<T['$queryResult'][number]>()

      // Subscribe to delta stream
      entry.subscription = client
        .watchQueryDeltas(query)
        .pipe(
          catchError((err) => {
            entry!.error = err?.message ?? 'Query failed'
            entry!.isLoading = false
            return of({
              sequence: 0,
              upserts: [],
              deletes: [],
              isReset: true,
            } as QueryDelta<T['$queryResult'][number]>)
          }),
        )
        .subscribe((delta) => {
          if (delta.isReset) {
            entry!.byId.clear()
          }

          for (const id of delta.deletes) {
            entry!.byId.delete(id)
          }

          for (const item of delta.upserts) {
            entry!.byId.set(item.id, item)
          }

          // Update array from map (triggers reactivity)
          entry!.items = [...entry!.byId.values()]
          entry!.isLoading = false
        })

      this.#queryCache.set(key, entry)
    }

    entry.refCount++

    const release = () => {
      entry!.refCount--
      if (entry!.refCount === 0) {
        entry!.subscription?.unsubscribe()
        this.#queryCache.delete(key)
      }
    }

    return {
      get items() {
        return entry!.items
      },
      get byId() {
        return entry!.byId
      },
      get isLoading() {
        return entry!.isLoading
      },
      get error() {
        return entry!.error
      },
      release,
    }
  }

  /**
   * Get reactive report results.
   * Call release() when done to decrement ref count.
   *
   * @example
   * ```typescript
   * $effect(() => {
   *   const result = reactiveClient.report(() => new GetMachineStatus({ machineId }))
   *   const status = result.data
   *   return () => result.release()
   * })
   * ```
   */
  report<T extends MReport<unknown>>(
    reportFactory: () => T,
  ): ReactiveReportResult<MReportResult<T>> {
    const report = reportFactory()
    const key = this.#getReportKey(report)

    let entry = this.#reportCache.get(key) as
      | ReportCacheEntry<MReportResult<T>>
      | undefined

    if (!entry) {
      // Create new cache entry (class instance with $state fields)
      entry = new ReportCacheEntry<MReportResult<T>>()

      entry.subscription = client
        .watchReport(report)
        .pipe(
          startWith(null),
          catchError((err) => {
            entry!.error = err?.message ?? 'Report failed'
            entry!.isLoading = false
            return of(null)
          }),
        )
        .subscribe((data) => {
          entry!.data = data as MReportResult<T> | null
          entry!.isLoading = data === null && entry!.error === null
        })

      this.#reportCache.set(key, entry)
    }

    entry.refCount++

    const release = () => {
      entry!.refCount--
      if (entry!.refCount === 0) {
        entry!.subscription?.unsubscribe()
        this.#reportCache.delete(key)
      }
    }

    return {
      get data() {
        return entry!.data
      },
      get isLoading() {
        return entry!.isLoading
      },
      get error() {
        return entry!.error
      },
      release,
    }
  }

  /**
   * Clear all cached subscriptions. Useful for testing or logout scenarios.
   */
  clearAll() {
    for (const entry of this.#queryCache.values()) {
      entry.subscription?.unsubscribe()
    }
    this.#queryCache.clear()

    for (const entry of this.#reportCache.values()) {
      entry.subscription?.unsubscribe()
    }
    this.#reportCache.clear()
  }
}

export const reactiveClient = new ReactiveClient()

/**
 * Hook for reactive query subscriptions with automatic cleanup.
 * Must be called during component initialization.
 *
 * @example
 * ```typescript
 * const targets = useQuery(() => new GetTargetsBySceneId({ sceneId }))
 * // targets.items, targets.byId, targets.isLoading are reactive
 * ```
 */
export function useQuery<T extends MQuery>(
  queryFactory: () => T,
): ReactiveQueryResult<T['$queryResult'][number]> {
  const result = reactiveClient.query(queryFactory)
  onDestroy(() => result.release())
  return result
}

/**
 * Hook for reactive report subscriptions with automatic cleanup.
 * Must be called during component initialization.
 *
 * @example
 * ```typescript
 * const status = useReport(() => new SceneState(sceneId, sessionId))
 * // status.data, status.isLoading are reactive
 * ```
 */
export function useReport<T extends MReport<unknown>>(
  reportFactory: () => T,
): ReactiveReportResult<MReportResult<T>> {
  const result = reactiveClient.report(reportFactory)
  onDestroy(() => result.release())
  return result
}
