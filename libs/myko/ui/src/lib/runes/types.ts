/**
 * Svelte 5 Rune-Based Query/Report Types
 *
 * These types support the useQuery and useReport factory functions
 * that provide automatic subscription lifecycle management.
 */

/**
 * Load state for query/report results.
 * Use the `status` field for discriminated union checks.
 */
export type LoadState<T> =
  | { status: 'loading'; data: null; error: null }
  | { status: 'ready'; data: T; error: null }
  | { status: 'empty'; data: T; error: null }
  | { status: 'error'; data: null; error: string }

/**
 * Options for useQuery
 */
export interface UseQueryOptions<T> {
  /** Detect empty arrays and set status to 'empty'. Default: true */
  detectEmpty?: boolean
  /** Custom function to detect empty state */
  isEmpty?: (data: T) => boolean
  /** Label for debug logging */
  debugLabel?: string
  /** Skip query execution when true */
  skip?: boolean
}

/**
 * Options for useReport
 */
export interface UseReportOptions<T> {
  /** Detect null/undefined and set status to 'empty'. Default: true */
  detectEmpty?: boolean
  /** Custom function to detect empty state */
  isEmpty?: (data: T) => boolean
  /** Label for debug logging */
  debugLabel?: string
  /** Skip report execution when true */
  skip?: boolean
}

/**
 * Result of useQuery - exposes reactive state via getters
 */
export interface QueryResult<T> {
  /** Current load state (loading/ready/empty/error) */
  readonly state: LoadState<T[]>
  /** Shorthand for state.status */
  readonly status: LoadState<T[]>['status']
  /** Data when ready/empty, null otherwise */
  readonly data: T[] | null
  /** Error message when in error state */
  readonly error: string | null
  /** Whether currently loading */
  readonly isLoading: boolean
  /** Whether data is ready (non-empty) */
  readonly isReady: boolean
  /** Whether data is empty */
  readonly isEmpty: boolean
  /** Whether in error state */
  readonly isError: boolean
  /** Force refresh the query */
  refresh: () => void
}

/**
 * Result of useReport - exposes reactive state via getters
 */
export interface ReportResult<T> {
  /** Current load state (loading/ready/empty/error) */
  readonly state: LoadState<T>
  /** Shorthand for state.status */
  readonly status: LoadState<T>['status']
  /** Data when ready/empty, null otherwise */
  readonly data: T | null
  /** Error message when in error state */
  readonly error: string | null
  /** Whether currently loading */
  readonly isLoading: boolean
  /** Whether data is ready (non-empty) */
  readonly isReady: boolean
  /** Whether data is empty */
  readonly isEmpty: boolean
  /** Whether in error state */
  readonly isError: boolean
  /** Force refresh the report */
  refresh: () => void
}

// Helper functions for creating LoadState values
export function loadingState<T>(): LoadState<T> {
  return { status: 'loading', data: null, error: null }
}

export function readyState<T>(data: T): LoadState<T> {
  return { status: 'ready', data, error: null }
}

export function emptyState<T>(data: T): LoadState<T> {
  return { status: 'empty', data, error: null }
}

export function errorState<T>(error: string): LoadState<T> {
  return { status: 'error', data: null, error }
}
