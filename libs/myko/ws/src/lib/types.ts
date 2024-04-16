import {
  ID,
  MCommand,
  MEvent,
  MItem,
  MQuery,
  MWrappedCommand,
  MWrappedItem,
  MWrappedQuery,
  MWrappedReport,
  MykoCommand,
  MykoQuery,
} from '@myko/core'

export const MEVENT_EVENT = 'ws:m:event'
export const MCOMMAND_EVENT = 'ws:m:command'
export const MCOMMAND_RESPONSE_EVENT = 'ws:m:command-response'
export const MCOMMAND_ERROR_EVENT = 'ws:m:command-error'
export const MQUERY_EVENT = 'ws:m:query'
export const MQUERY_RESPONSE_EVENT = 'ws:m:query-response'
export const MQUERY_CANCEL = 'ws:m:query-cancel'
export const MREPORT_EVENT = 'ws:m:report'
export const MREPORT_RESPONSE_EVENT = 'ws:m:report-response'
export const MREPORT_CANCEL = 'ws:m:report-cancel'
export const MPING_EVENT = 'ws:m:ping'

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

export type WSMQueryResponse = {
  tx: ID
  event: typeof MQUERY_RESPONSE_EVENT
  data: MWrappedItem[]
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

export type WSMReportResponse = {
  tx: ID
  event: typeof MREPORT_RESPONSE_EVENT
  data: unknown
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
  tx: ID
  data: unknown
  event: typeof MCOMMAND_RESPONSE_EVENT
}

export type WSMCommandError = {
  tx: ID
  message: string
  event: typeof MCOMMAND_ERROR_EVENT
}

export type WSMMessage =
  | WSMQuery
  | WSMCommand
  | WSMEvent
  | WSMQueryResponse
  | WSMCommandResponse
  | WSMCommandError
  | WSMQueryCancel
  | WSMReport
  | WSMReportResponse
  | WSMReportCancel
  | WSPingEvent

@MykoCommand('client:send-command')
export class ClientCommand extends MCommand {
  readonly command: MWrappedCommand
  constructor(
    command: MWrappedCommand,
    readonly clientId: ID,
  ) {
    super()
    this.command = command
  }
}

@MykoQuery('peer:send-query', MItem)
export class PeerQuery extends MQuery<MItem> {
  constructor(
    readonly query: MQuery,
    readonly peerId: ID,
  ) {
    super()
  }
}

@MykoCommand('peer:send-command')
export class PeerCommand extends MCommand {
  constructor(
    readonly command: MCommand,
    readonly peerId: ID,
  ) {
    super()
  }
}
