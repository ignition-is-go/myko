/**
 * @myko/ts - Pure TypeScript client for Myko servers
 *
 * Browser-compatible WebSocket client with RxJS Observables.
 */

// Re-export all Rust-generated types from @myko/rs
export * from '@myko/rs'

/**
 * ID type alias for entity identifiers.
 * In the Rust system, IDs are Arc<str> which serialize as strings.
 */
export type ID = string

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
export type { WrappedItem as MWrappedItem } from '@myko/rs'

/**
 * GetEventLog is now ServerEventLog in the Rust system.
 * @deprecated Use ServerEventLog from @myko/rs instead
 */
// Note: ServerEventLog is re-exported from @myko/rs via export * above

// Export the client
export {
  ConnectionStatus,
  MykoClient,
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
} from './client'

// ─────────────────────────────────────────────────────────────────────────────
// Core framework reports (compatibility layer for @myko/core types)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Stub representation of an entity for tree traversal.
 */
export type MItemStub = {
  id: ID
  itemType: string
  name?: string
  hash: string
}

/**
 * Report that fetches all items by type and IDs.
 */
export class GetItemsByTypeAndIds {
  static readonly reportId = 'GetItemsByTypeAndIds'
  readonly reportId = 'GetItemsByTypeAndIds'

  constructor(
    public type: string,
    public ids: ID[],
  ) {}
}

/**
 * Report that fetches immediate child entities of a parent.
 */
export class ChildEntities {
  static readonly reportId = 'ChildEntities'
  readonly reportId = 'ChildEntities'

  constructor(
    readonly parentType: string,
    readonly parentId: ID,
  ) {}
}

/**
 * Report that recursively fetches all child entities of a parent.
 */
export class FullChildEntities {
  static readonly reportId = 'FullChildEntities'
  readonly reportId = 'FullChildEntities'

  constructor(
    readonly parentType: string,
    readonly parentId: ID,
  ) {}
}

/**
 * Report that fetches all-time child entities (including deleted).
 */
export class ChildEntitiesAllTime {
  static readonly reportId = 'ChildEntitiesAllTime'
  readonly reportId = 'ChildEntitiesAllTime'

  constructor(
    readonly parentType: string,
    readonly parentId: ID,
  ) {}
}

/**
 * Data returned by EntitySnapshotDifference report.
 */
export type EntitySnapshotDifferenceData = {
  changed: MItemStub[]
  added: MItemStub[]
  removed: MItemStub[]
}

/**
 * Report that computes the difference between entity snapshots.
 */
export class EntitySnapshotDifference {
  static readonly reportId = 'EntitySnapshotDifference'
  readonly reportId = 'EntitySnapshotDifference'

  constructor(
    readonly parentType: string,
    readonly parentId: ID,
  ) {}
}
