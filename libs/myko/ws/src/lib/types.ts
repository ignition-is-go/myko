import { MWrappedCommand, MEvent, MWrappedQuery } from '@myko/core'

export const MQUERY_EVENT = 'ws:m:command'
export const MEVENT_EVENT = 'ws:m:event'
export const MCOMMAND_EVENT = 'ws:m:query'

export const MYKO_WS_PORT = 5155

export type WSMEvent = {
  event: typeof MEVENT_EVENT
  data: MEvent
}

export type WSMCommand = {
  event: typeof MCOMMAND_EVENT
  data: MWrappedCommand
}

export type WSMQuery = {
  event: typeof MQUERY_EVENT
  data: MWrappedQuery
}

export type WSMMessage = WSMQuery | WSMCommand | WSMEvent
