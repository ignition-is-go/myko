/**
 * @myko/ts - Pure TypeScript client for Myko servers
 *
 * Browser-compatible WebSocket client with RxJS Observables.
 */

import type { MEvent } from '@myko/rs'

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
} from './client.js'

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
  readonly report: { parentType: string; parentId: ID }

  /** Phantom type marker for result inference */
  readonly $res!: () => MItemStub[]

  constructor(parentType: string, parentId: ID) {
    this.report = { parentType, parentId }
  }
}

/**
 * Report that recursively fetches all child entities of a parent.
 */
export class FullChildEntities {
  static readonly reportId = 'FullChildEntities'
  readonly reportId = 'FullChildEntities'
  readonly report: { parentType: string; parentId: ID }

  /** Phantom type marker for result inference */
  readonly $res!: () => MItemStub[]

  constructor(parentType: string, parentId: ID) {
    this.report = { parentType, parentId }
  }
}

/**
 * Report that fetches all-time child entities (including deleted).
 */
export class ChildEntitiesAllTime {
  static readonly reportId = 'ChildEntitiesAllTime'
  readonly reportId = 'ChildEntitiesAllTime'
  readonly report: { parentType: string; parentId: ID }

  /** Phantom type marker for result inference */
  readonly $res!: () => MItemStub[]

  constructor(parentType: string, parentId: ID) {
    this.report = { parentType, parentId }
  }
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
  readonly report: { parentType: string; parentId: ID }

  /** Phantom type marker for result inference */
  readonly $res!: () => EntitySnapshotDifferenceData

  constructor(parentType: string, parentId: ID) {
    this.report = { parentType, parentId }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Logging support
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Log level enum for controlling logging verbosity.
 */
export enum LogLevel {
  INFO = 'INFO',
  WARN = 'WARN',
  ERROR = 'ERROR',
  DEBUG = 'DEBUG',
  VERBOSE = 'VERBOSE',
}

/**
 * Log levels ordered by severity (most severe first).
 */
export const LOG_RANK = [
  LogLevel.ERROR,
  LogLevel.WARN,
  LogLevel.INFO,
  LogLevel.DEBUG,
  LogLevel.VERBOSE,
]

/**
 * Report that returns the list of available logger names.
 */
export class Loggers {
  static readonly reportId = 'Loggers'
  readonly reportId = 'Loggers'
  readonly report = {}

  /** Phantom type marker for result inference */
  readonly $res!: () => string[]
}

/**
 * Report that returns the current log level for a server.
 */
export class ServerLogLevel {
  static readonly reportId = 'ServerLogLevel'
  readonly reportId = 'ServerLogLevel'
  readonly report: { serverId: ID }

  /** Phantom type marker for result inference */
  readonly $res!: () => LogLevel

  constructor(serverId: ID) {
    this.report = { serverId }
  }
}

/**
 * Command to set the log level for a server.
 */
export class SetLogLevel {
  static readonly commandId = 'SetLogLevel'
  readonly commandId = 'SetLogLevel'
  readonly command: { serverId: ID; level: LogLevel }

  /** Phantom type marker for result inference */
  readonly $res!: () => void

  constructor(serverId: ID, level: LogLevel) {
    this.command = { serverId, level }
  }
}

/**
 * Report that checks if a peer server is alive and returns ping time.
 */
export class PeerAlive {
  static readonly reportId = 'PeerAlive'
  readonly reportId = 'PeerAlive'
  readonly report: { peerId: ID }

  /** Phantom type marker for result inference */
  readonly $res!: () => number | false

  constructor(peerId: ID) {
    this.report = { peerId }
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Event history support
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Container for an event with associated metadata.
 */
export type EventContainer = {
  id: ID
  event: MEvent
}

/**
 * Report that returns events for a specific transaction.
 */
export class EventsForTransaction {
  static readonly reportId = 'EventsForTransaction'
  readonly reportId = 'EventsForTransaction'
  readonly report: { transactionId: string }

  /** Phantom type marker for result inference */
  readonly $res!: () => MEvent[]

  constructor(transactionId: string) {
    this.report = { transactionId }
  }
}

/**
 * Query that returns events for a specific entity.
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
