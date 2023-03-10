import {
  MQuery,
  wrapQuery,
  MCommand,
  wrapCommand,
  MEvent,
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

export const wrapQueryWS = (query: MQuery, clientId: ID): WSMQuery => ({
  data: { ...wrapQuery(query), clientId },
  event: MQUERY_EVENT,
})

export const wrapCommandWS = (
  command: MCommand<unknown>,
  clientId: ID,
): WSMCommand => ({
  data: { ...wrapCommand(command), clientId },
  event: MCOMMAND_EVENT,
})

export const wrapEventWS = (event: MEvent, clientId: string): WSMEvent => ({
  data: { ...event, clientId },
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

export const wrapCommandResponseWS = (
  tx: ID,
  response: unknown,
): WSMCommandResponse => ({
  event: MCOMMAND_RESPONSE_EVENT,
  tx,
  data: response,
})

export const wrapQueryCancel = (tx: ID): WSMQueryCancel => ({
  event: MQUERY_CANCEL,
  data: tx,
})
