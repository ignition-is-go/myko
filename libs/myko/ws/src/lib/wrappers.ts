import {
  MCommand,
  MEvent,
  MQuery,
  MReport,
  MWrappedCommand,
  wrapCommand,
  wrapQuery,
  wrapReport,
  type ID,
} from '@myko/core'
import {
  MCOMMAND_EVENT,
  MCOMMAND_RESPONSE_EVENT,
  MEVENT_EVENT,
  MQUERY_CANCEL,
  MQUERY_EVENT,
  MREPORT_CANCEL,
  MREPORT_EVENT,
  MREPORT_RESPONSE_EVENT,
  WSMCommand,
  WSMCommandResponse,
  WSMEvent,
  WSMQuery,
  WSMQueryCancel,
  WSMReport,
  WSMReportCancel,
  WSMReportResponse,
} from './types'

export const wrapQueryWS = (query: MQuery): WSMQuery => ({
  data: wrapQuery(query),
  event: MQUERY_EVENT,
})

export const wrapCommandWS = (command: MCommand<unknown>): WSMCommand => ({
  data: wrapCommand(command),
  event: MCOMMAND_EVENT,
})

export const wrapCommandOnlyWS = (command: MWrappedCommand): WSMCommand => ({
  data: command,
  event: MCOMMAND_EVENT,
})

export const wrapEventWS = (event: MEvent): WSMEvent => ({
  data: event,
  event: MEVENT_EVENT,
})

// export const wrapQueryResponseWS = (
//   items: MWrappedItem[],
//   tx: ID,
// ): WSMQueryResponse => ({
//   data: items,
//   event: MQUERY_RESPONSE_EVENT,
//   tx,
// })

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
  tx: tx,
})

export const wrapReportWS = (report: MReport<any>): WSMReport => ({
  data: wrapReport(report),
  event: MREPORT_EVENT,
})

export const wrapReportResponseWS = (
  tx: ID,
  response: unknown,
): WSMReportResponse => ({
  event: MREPORT_RESPONSE_EVENT,
  tx,
  data: response,
})

export const wrapReportCancel = (tx: ID): WSMReportCancel => ({
  event: MREPORT_CANCEL,
  tx: tx,
})
