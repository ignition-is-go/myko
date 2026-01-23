/**
 * Svelte 5 Rune-Based Report Wrapper
 *
 * Provides automatic subscription lifecycle management for Myko reports.
 * Reports return a single value (not an array like queries).
 */

import type { MReport } from '@myko/core'
import { catchError, of, type Subscription } from 'rxjs'
import { client } from '../services/client.js'
import {
  emptyState,
  errorState,
  loadingState,
  readyState,
  type LoadState,
  type ReportResult,
  type UseReportOptions,
} from './types.js'

/**
 * Create a reactive report that automatically manages subscription lifecycle.
 *
 * Reports return a single value, unlike queries which return arrays.
 *
 * @example Basic usage
 * ```svelte
 * <script lang="ts">
 *   import { useReport } from '@myko/ui/runes'
 *   import { MissingFiles } from '@rship/entities-core'
 *
 *   const missing = useReport(() => new MissingFiles(machineId))
 * </script>
 *
 * {#if missing.isReady}
 *   <MissingFilesList files={missing.data.missing} />
 * {/if}
 * ```
 *
 * @example With skip condition
 * ```svelte
 * <script lang="ts">
 *   const status = useReport(
 *     () => new GetMachineStatus(machineId),
 *     { skip: !machineId }
 *   )
 * </script>
 * ```
 */
export function useReport<T>(
  reportFactory: () => MReport<T>,
  options: UseReportOptions<T> = {},
): ReportResult<T> {
  const {
    detectEmpty = true,
    isEmpty: customIsEmpty,
    debugLabel,
  } = options

  // Internal reactive state
  let state = $state(loadingState<T>())
  let refreshTrigger = $state(0)
  let skip = $state(options.skip ?? false)

  // Update skip reactively if options.skip changes
  $effect(() => {
    skip = options.skip ?? false
  })

  const defaultIsEmpty = (data: unknown): boolean => {
    return data === null || data === undefined
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

    // Create report
    let report: MReport<T> | null = null
    try {
      report = reportFactory()
    } catch (err) {
      if (debugLabel) {
        console.warn(`[useReport:${debugLabel}] Factory error:`, err)
      }
      state = errorState(err instanceof Error ? err.message : String(err))
      return cleanup
    }

    if (!report) {
      state = errorState('Failed to create report')
      return cleanup
    }

    if (debugLabel) {
      console.debug(`[useReport:${debugLabel}] Subscribing`)
    }

    state = loadingState()

    subscription = client
      .watchReport(report)
      .pipe(
        catchError((err) => {
          const message = err instanceof Error ? err.message : String(err)
          if (debugLabel) {
            console.warn(`[useReport:${debugLabel}] Error:`, message)
          }
          state = errorState(message)
          return of(null as T)
        }),
      )
      .subscribe((data: T) => {
        if (state.status === 'error') {
          // Don't overwrite error state from catchError
          return
        }

        if (detectEmpty && checkEmpty(data)) {
          if (debugLabel) {
            console.debug(`[useReport:${debugLabel}] Empty`)
          }
          state = emptyState(data as T)
        } else {
          if (debugLabel) {
            console.debug(`[useReport:${debugLabel}] Ready`)
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
