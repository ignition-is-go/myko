import {
  MWrappedCommand,
  MEvent,
  MWrappedQuery,
  MWrappedItem,
  ID,
  MykoCommand,
  MCommand,
} from '@myko/core'

export const MEVENT_EVENT = 'ws:m:event'
export const MCOMMAND_EVENT = 'ws:m:command'
export const MCOMMAND_RESPONSE_EVENT = 'ws:m:command-response'
export const MQUERY_EVENT = 'ws:m:query'
export const MQUERY_RESPONSE_EVENT = 'ws:m:query-response'
export const MQUERY_CANCEL = 'ws:m:query-cancel'

export const MYKO_WS_PORT = 5155

export type WSMEvent = {
  event: typeof MEVENT_EVENT
  data: MEvent & { clientId: ID }
}

export type WSMCommand = {
  event: typeof MCOMMAND_EVENT
  data: MWrappedCommand
}

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
  data: ID
  event: typeof MQUERY_CANCEL
}

export type WSMCommandResponse = {
  tx: ID
  data: unknown
  event: typeof MCOMMAND_RESPONSE_EVENT
}

export type WSMMessage =
  | WSMQuery
  | WSMCommand
  | WSMEvent
  | WSMQueryResponse
  | WSMCommandResponse
  | WSMQueryCancel

@MykoCommand('client:send-command')
export class ClientCommand extends MCommand {
  constructor(readonly command: MCommand, readonly clientId: ID) {
    super()
  }
}
