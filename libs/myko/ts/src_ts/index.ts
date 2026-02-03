/**
 * @myko/ts - Pure TypeScript client for Myko servers
 *
 * Browser-compatible WebSocket client with RxJS Observables.
 */

import type { ID, MEvent, WrappedItem, ItemStub, LogLevel } from '@myko/rs'

// Re-export all Rust-generated types from @myko/rs
// This includes the ID type alias (ID = string)
export * from '@myko/rs'

// Explicitly re-export type-only exports that don't come through `export *`
export type { LogLevel }

/**
 * Get the item type name from an entity type or query class.
 *
 * For query/report/command classes: Returns the static queryItemType, reportId, or commandId
 * For entity types in the new Rust system: Use the literal type name string instead
 *
 * @example
 * // Old style (decorator-based):
 * getItemName(Scene) // This won't work with Rust-generated types
 *
 * // New style - use literal strings:
 * 'Scene'
 *
 * // Or use query's static property:
 * GetScenesByIds.queryItemType // 'Scene'
 */
export function getItemName(
  item: { queryItemType?: string; name?: string } | string,
): string {
  if (typeof item === 'string') return item
  if ('queryItemType' in item && item.queryItemType) return item.queryItemType
  if ('name' in item && item.name) return item.name
  throw new Error(
    `Cannot get item name from ${item}. Use literal string like 'Scene' instead.`,
  )
}

/**
 * Compatibility alias for MWrappedItem.
 * In the new system, use WrappedItem<T> from @myko/rs.
 */
export type MWrappedItem<T = unknown> = WrappedItem<T>

/**
 * Compatibility alias for MItemStub.
 * In the new system, use ItemStub from @myko/rs.
 */
export type MItemStub = ItemStub

/**
 * Log levels ordered by severity (most severe first).
 * Used for filtering and comparing log levels.
 */
export const LOG_RANK = [
  'ERROR',
  'WARN',
  'INFO',
  'DEBUG',
  'VERBOSE',
] as const

/**
 * GetEventLog is now ServerEventLog in the Rust system.
 * @deprecated Use ServerEventLog from @myko/rs instead
 */
// Note: ServerEventLog is re-exported from @myko/rs via export * above

// Export the client
export {
  ConnectionStatus,
  MykoClient,
  MykoProtocol,
  type ClientStats,
  type Command,
  type CommandResult,
  type MykoError,
  type MykoErrorEvent,
  type Query,
  type QueryDiff,
  type QueryItem,
  type QueryResult,
  type Report,
  type ReportResult,
} from './client.js'

// ─────────────────────────────────────────────────────────────────────────────
// Legacy stubs (not yet in Rust)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Container for an event with associated metadata.
 * @deprecated Will be generated from Rust
 */
export type EventContainer = {
  id: ID
  event: MEvent
}

/**
 * Query that returns events for a specific entity.
 * @deprecated TODO: Implement in Rust server
 */
export class EventsForEntity {
  static readonly queryId = 'EventsForEntity'
  readonly queryId = 'EventsForEntity'
  static readonly queryItemType = 'EventContainer'
  readonly queryItemType = 'EventContainer'
  readonly query: { entityId: string; start?: string; end?: string }

  /** Phantom type marker for result inference */
  readonly $res!: () => EventContainer[]

  constructor(entityId: string, start?: string, end?: string) {
    this.query = { entityId, start, end }
  }
}
