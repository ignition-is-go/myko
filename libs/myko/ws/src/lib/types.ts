import type {
  ID,
  MEvent,
  MWrappedCommand,
  MWrappedItem,
  MWrappedQuery,
  MWrappedReport,
} from '@myko/core'

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

export type WSMQueryResponseData = {
  sequence: number
  upserts: MWrappedItem[]
  deletes: ID[]
  tx: ID
}

export type WSMQueryResponse = {
  event: typeof MQUERY_RESPONSE_EVENT
  data: WSMQueryResponseData
}

export type WSMQueryError = {
  event: typeof MQUERY_ERROR_EVENT
  data: {
    tx: ID
    message: string
  }
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
  data: {
    tx: ID
    message: string
  }
}

export type WSMReportResponse = {
  event: typeof MREPORT_RESPONSE_EVENT
  data: {
    tx: ID
    response: unknown
  }
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
  data: {
    tx: ID
    response: unknown
  }
  event: typeof MCOMMAND_RESPONSE_EVENT
}

export type WSMCommandError = {
  data: {
    tx: ID
    message: string
  }
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
