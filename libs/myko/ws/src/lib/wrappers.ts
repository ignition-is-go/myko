import {
  MQuery,
  MQueryable,
  wrapQuery,
  MCommand,
  wrapCommand,
  MEvent,
  MItem,
  MWrappedItem,
  ID,
} from '@myko/core'
import {
  MCOMMAND_EVENT,
  MCOMMAND_RESPONSE_EVENT,
  MEVENT_EVENT,
  MQUERY_CANCEL,
  MQUERY_EVENT,
  MQUERY_RESPONSE_EVENT,
  WSMCommand,
  WSMCommandResponse,
  WSMEvent,
  WSMQuery,
  WSMQueryCancel,
  WSMQueryResponse,
} from './types'

export const wrapQueryWS = (query: MQuery<MQueryable>): WSMQuery => ({
  data: wrapQuery(query),
  event: MQUERY_EVENT,
})

export const wrapCommandWS = (command: MCommand): WSMCommand => ({
  data: wrapCommand(command),
  event: MCOMMAND_EVENT,
})

export const wrapEventWS = (event: MEvent): WSMEvent => ({
  data: event,
  event: MEVENT_EVENT,
})

export const wrapQueryResponseWS = (
  items: MWrappedItem[],
  tx: ID,
): WSMQueryResponse => ({
  data: items,
  event: MQUERY_RESPONSE_EVENT,
  tx,
})

export const wrapCommandResponseWS = (tx: ID): WSMCommandResponse => ({
  event: MCOMMAND_RESPONSE_EVENT,
  data: tx,
})

export const wrapQueryCancel = (tx: ID): WSMQueryCancel => ({
  event: MQUERY_CANCEL,
  data: tx,
})
