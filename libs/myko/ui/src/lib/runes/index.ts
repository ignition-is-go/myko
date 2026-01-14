/**
 * Svelte 5 Rune-Based Wrappers for Myko Client
 *
 * These utilities provide automatic subscription lifecycle management
 * for Myko queries and reports, exposing reactive state via Svelte 5 runes.
 *
 * @example
 * ```svelte
 * <script lang="ts">
 *   import { useQuery, useReport } from '@myko/ui/runes'
 *   import { GetTargets, MissingFiles } from '@rship/entities-core'
 *
 *   const targets = useQuery(() => new GetTargets(projectId))
 *   const missing = useReport(() => new MissingFiles(machineId))
 * </script>
 * ```
 */

export { useQuery } from './query.svelte.js'
export { useReport } from './report.svelte.js'
export {
  // Types
  type LoadState,
  type QueryResult,
  type ReportResult,
  type UseQueryOptions,
  type UseReportOptions,
  // Helper functions
  emptyState,
  errorState,
  loadingState,
  readyState,
} from './types.js'
