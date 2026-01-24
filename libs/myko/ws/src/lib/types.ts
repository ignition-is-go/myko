import type {
  ID,
  MEvent,
  MWrappedCommand,
  MWrappedQuery,
  MWrappedReport,
} from '@myko/core'
import type {
  CommandError,
  CommandResponse,
  QueryError,
  QueryResponse,
  ReportError,
  ReportResponse,
} from '@myko/rs'

// Re-export Rust-generated types for convenience
export type {
  CommandError,
  CommandResponse,
  MykoMessage,
  QueryError,
  QueryResponse,
  ReportError,
  ReportResponse,
  MEvent as RsMEvent,
  WrappedQuery,
  WrappedReport,
} from '@myko/rs'

/**
 * Raw delta from a query subscription before accumulation.
 * Used by ReactiveClient to apply updates directly to SvelteMap.
 */
export type QueryDelta<T> = {
  sequence: number
  upserts: T[]
  deletes: ID[]
  /** True when sequence === 0, indicating a full reset */
  isReset: boolean
}

export const MEVENT_EVENT = 'ws:m:event'
export const MCOMMAND_EVENT = 'ws:m:command'
export const MCOMMAND_RESPONSE_EVENT = 'ws:m:command-response'
export const MCOMMAND_ERROR_EVENT = 'ws:m:command-error'
export const MQUERY_EVENT = 'ws:m:query'
export const MQUERY_RESPONSE_EVENT = 'ws:m:query-response'
export const MQUERY_ERROR_EVENT = 'ws:m:query-error'
export const MQUERY_CANCEL = 'ws:m:query-cancel'
export const MREPORT_EVENT = 'ws:m:report'
export const MREPORT_RESPONSE_EVENT = 'ws:m:report-response'
export const MREPORT_ERROR_EVENT = 'ws:m:report-error'
export const MREPORT_CANCEL = 'ws:m:report-cancel'
export const MPING_EVENT = 'ws:m:ping'
export const MBATCH_EVENT = 'ws:m:batch'

export const MYKO_WS_PORT = 5155

export type WSPingEvent = {
  event: typeof MPING_EVENT
  data: {
    id: string
    timestamp: number
  }
}

// EVENTS
export type WSMEvent = {
  event: typeof MEVENT_EVENT
  data: MEvent
}

// QUERY
export type WSMQuery = {
  event: typeof MQUERY_EVENT
  data: MWrappedQuery
}

// Aligned with Rust QueryResponse - note sequence is bigint in Rust but number here for compatibility
export type WSMQueryResponseData = QueryResponse

export type WSMQueryResponse = {
  event: typeof MQUERY_RESPONSE_EVENT
  data: WSMQueryResponseData
}

export type WSMQueryError = {
  event: typeof MQUERY_ERROR_EVENT
  data: QueryError
}

export type WSMQueryCancel = {
  tx: ID
  event: typeof MQUERY_CANCEL
}

// REPORT
export type WSMReport = {
  event: typeof MREPORT_EVENT
  data: MWrappedReport
}

export type WSMReportError = {
  event: typeof MREPORT_ERROR_EVENT
  data: ReportError
}

export type WSMReportResponse = {
  event: typeof MREPORT_RESPONSE_EVENT
  data: ReportResponse
}

export type WSMReportCancel = {
  tx: ID
  event: typeof MREPORT_CANCEL
}

// COMMAND
export type WSMCommand = {
  event: typeof MCOMMAND_EVENT
  data: MWrappedCommand
}

export type WSMCommandResponse = {
  data: CommandResponse
  event: typeof MCOMMAND_RESPONSE_EVENT
}

export type WSMCommandError = {
  data: CommandError
  event: typeof MCOMMAND_ERROR_EVENT
}

// BATCH - for bundling multiple messages in a single frame
export type WSMBatch = {
  event: typeof MBATCH_EVENT
  data: WSMMessage[]
}

export type WSMMessage =
  | WSMQuery
  | WSMCommand
  | WSMEvent
  | WSMQueryResponse
  | WSMCommandResponse
  | WSMCommandError
  | WSMQueryCancel
  | WSMQueryError
  | WSMReport
  | WSMReportResponse
  | WSMReportCancel
  | WSMReportError
  | WSPingEvent
  | WSMBatch
