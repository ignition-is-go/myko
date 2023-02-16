import {
  MQuery,
  MQueryable,
  wrapQuery,
  MCommand,
  wrapCommand,
  MEvent,
} from '@myko/core'
import {
  MCOMMAND_EVENT,
  MEVENT_EVENT,
  MQUERY_EVENT,
  WSMCommand,
  WSMQuery,
} from './types'

export const wrapQueryWS = (query: MQuery<MQueryable>): WSMQuery => ({
  data: wrapQuery(query),
  event: MQUERY_EVENT,
})

export const wrapCommandWS = (command: MCommand): WSMCommand => ({
  data: wrapCommand(command),
  event: MCOMMAND_EVENT,
})

export const wrapEventWS = (event: MEvent) => ({
  data: event,
  event: MEVENT_EVENT,
})
