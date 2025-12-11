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
  type QueryResult,
  type ReportResult,
} from './client'
