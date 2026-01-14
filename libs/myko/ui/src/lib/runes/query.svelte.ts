/**
 * Svelte 5 Rune-Based Query Wrapper
 *
 * Provides automatic subscription lifecycle management for Myko queries.
 */

import type { MItem, MQuery } from '@myko/core'
import { catchError, of, type Subscription } from 'rxjs'
import { untrack } from 'svelte'
import { client } from '../services/client.js'
import {
  emptyState,
  errorState,
  loadingState,
  readyState,
  type LoadState,
  type QueryResult,
  type UseQueryOptions,
} from './types.js'

/**
 * Create a reactive query that automatically manages subscription lifecycle.
 *
 * CRITICAL: The queryFactory is called reactively. Include all reactive
 * dependencies inside the factory function so they are properly tracked.
 *
 * @example Basic usage
 * ```svelte
 * <script lang="ts">
 *   import { useQuery } from '@myko/ui/runes'
 *   import { GetTargets } from '@rship/entities-core'
 *
 *   const targets = useQuery(() => new GetTargets(projectId))
 * </script>
 *
 * {#if targets.isLoading}
 *   <Loading />
 * {:else if targets.isError}
 *   <Error message={targets.error} />
 * {:else if targets.isEmpty}
 *   <Empty />
 * {:else}
 *   {#each targets.data as target}
 *     <TargetItem {target} />
 *   {/each}
 * {/if}
 * ```
 *
 * @example With skip condition
 * ```svelte
 * <script lang="ts">
 *   const sessions = useQuery(
 *     () => new GetSessions(projectId),
 *     { skip: !projectId }
 *   )
 * </script>
 * ```
 *
 * @example With reactive dependencies
 * ```svelte
 * <script lang="ts">
 *   let filter = $state('active')
 *
 *   // Query re-runs when filter changes (read inside factory)
 *   const items = useQuery(() => new GetItemsByFilter(projectId, filter))
 * </script>
 * ```
 */
export function useQuery<T extends MItem>(
  queryFactory: () => MQuery<T>,
  options: UseQueryOptions<T[]> = {},
): QueryResult<T> {
  const {
    detectEmpty = true,
    isEmpty: customIsEmpty,
    debugLabel,
  } = options

  // Internal reactive state
  let state = $state(loadingState<T[]>())
  let refreshTrigger = $state(0)
  let skip = $state(options.skip ?? false)

  // Update skip reactively if options.skip changes
  $effect(() => {
    skip = options.skip ?? false
  })

  const defaultIsEmpty = (data: unknown): boolean => {
    if (Array.isArray(data)) return data.length === 0
    if (data === null || data === undefined) return true
    return false
  }

  const checkEmpty = customIsEmpty ?? defaultIsEmpty

  // Subscription management
  let subscription: Subscription | null = null

  const cleanup = () => {
    if (subscription) {
      subscription.unsubscribe()
      subscription = null
    }
  }

  // Set up reactive subscription
  $effect(() => {
    // Read reactive dependencies
    const _refresh = refreshTrigger
    const _skip = skip

    // Cleanup previous subscription
    cleanup()

    // Skip if requested
    if (_skip) {
      state = loadingState()
      return cleanup
    }

    // Create query - untrack to avoid re-triggering on internal state changes
    // but the factory itself will read reactive deps
    let query: MQuery<T> | null = null
    try {
      query = queryFactory()
    } catch (err) {
      if (debugLabel) {
        console.warn(`[useQuery:${debugLabel}] Factory error:`, err)
      }
      state = errorState(err instanceof Error ? err.message : String(err))
      return cleanup
    }

    if (!query) {
      state = errorState('Failed to create query')
      return cleanup
    }

    if (debugLabel) {
      console.debug(`[useQuery:${debugLabel}] Subscribing`)
    }

    state = loadingState()

    subscription = client
      .watchQuery(query)
      .pipe(
        catchError((err) => {
          const message = err instanceof Error ? err.message : String(err)
          if (debugLabel) {
            console.warn(`[useQuery:${debugLabel}] Error:`, message)
          }
          state = errorState(message)
          return of([] as T[])
        }),
      )
      .subscribe((data: T[]) => {
        if (state.status === 'error') {
          // Don't overwrite error state from catchError
          return
        }

        if (detectEmpty && checkEmpty(data)) {
          if (debugLabel) {
            console.debug(`[useQuery:${debugLabel}] Empty`)
          }
          state = emptyState(data)
        } else {
          if (debugLabel) {
            console.debug(`[useQuery:${debugLabel}] Ready`, { count: data.length })
          }
          state = readyState(data)
        }
      })

    return cleanup
  })

  return {
    get state() {
      return state
    },
    get status() {
      return state.status
    },
    get data() {
      return state.status === 'ready' || state.status === 'empty'
        ? state.data
        : null
    },
    get error() {
      return state.status === 'error' ? state.error : null
    },
    get isLoading() {
      return state.status === 'loading'
    },
    get isReady() {
      return state.status === 'ready'
    },
    get isEmpty() {
      return state.status === 'empty'
    },
    get isError() {
      return state.status === 'error'
    },
    refresh() {
      refreshTrigger++
    },
  }
}
