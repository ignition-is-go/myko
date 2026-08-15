/**
 * @myko/core - Myko client and generated types
 *
 * Browser-compatible WebSocket client with RxJS Observables,
 * plus Rust-generated type bindings.
 */

import type { ID, ItemStub, LogLevel, MEvent, WrappedItem } from './generated'

// Re-export all Rust-generated types from ./generated
// This includes the ID type alias (ID = string)
export * from './generated'
export { type BoundGraph, bindGraph } from './graph-client.js'

export {
  applyCollectionDiff,
  type CollectionChanges,
  type CollectionDiff,
  type Identified,
  LiveCollection,
  type LiveCollectionStatus,
  LiveIndex,
  type LiveIndexChanges,
  type MutableKeyedCollection,
} from './live-collection.js'
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
 * In the new system, use WrappedItem from ./generated.
 */
export type MWrappedItem<T = unknown> = WrappedItem

/**
 * Compatibility alias for MItemStub.
 * In the new system, use ItemStub from ./generated.
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
 * @deprecated Use ServerEventLog from ./generated instead
 */
// Note: ServerEventLog is re-exported from ./generated via export * above

// Export the client
export {
  type ClientStats,
  type Command,
  type CommandResult,
  ConnectionStatus,
  MykoClient,
  type MykoError,
  type MykoErrorEvent,
  MykoProtocol,
  type Query,
  type QueryDiff,
  type QueryItem,
  type QueryResult,
  type QueryWatchOptions,
  type QueryWindow,
  type QueryWindowInfo,
  type Report,
  type ReportResult,
  type View,
  type ViewItem,
  type ViewResult,
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
