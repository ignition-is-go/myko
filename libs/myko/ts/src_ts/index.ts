/**
 * @myko/ts - Pure TypeScript client for Myko servers
 *
 * Browser-compatible WebSocket client with RxJS Observables.
 */

// Re-export all Rust-generated types from @myko/rs
export * from '@myko/rs'

// Export the client
export {
  ConnectionStatus,
  MykoClient,
  type ClientStats,
  type Command,
  type CommandResult,
  type MykoError,
  type Query,
  type QueryDiff,
  type QueryItem,
  type QueryResult,
  type Report,
  type ReportResult,
} from './client'
