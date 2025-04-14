import {
  MCommand,
  MQuery,
  MReport,
  MykoCommandError,
  type ID,
  type MEvent,
  type MWrappedCommand,
} from '@myko/core'
import {
  MCOMMAND_ERROR_EVENT,
  MCOMMAND_EVENT,
  MCOMMAND_RESPONSE_EVENT,
  MEVENT_EVENT,
  MPING_EVENT,
  MQUERY_CANCEL,
  MQUERY_ERROR_EVENT,
  MQUERY_EVENT,
  MQUERY_RESPONSE_EVENT,
  MREPORT_CANCEL,
  MREPORT_ERROR_EVENT,
  MREPORT_EVENT,
  MREPORT_RESPONSE_EVENT,
  type WSMCommand,
  type WSMCommandError,
  type WSMCommandResponse,
  type WSMEvent,
  type WSMMessage,
  type WSMQuery,
  type WSMQueryCancel,
  type WSMReport,
  type WSMReportCancel,
  type WSMReportResponse,
} from './types'

export const wrapQueryWS = (query: MQuery): WSMQuery => ({
  data: query.wrap(),
  event: MQUERY_EVENT,
})

export const wrapCommandWS = (command: MCommand<unknown>): WSMCommand => ({
  data: command.wrap(),
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
  data: {
    tx,
    response,
  },
})

export const wrapCommandErrorWS = (
  error: MykoCommandError,
): WSMCommandError => ({
  event: MCOMMAND_ERROR_EVENT,
  data: {
    tx: error.tx,
    message: error.message,
  },
})

export const wrapError = (e: Error, tx: ID): WSMCommandError => ({
  event: MCOMMAND_ERROR_EVENT,
  data: {
    tx,
    message: e.message,
  },
})

export const wrapQueryCancel = (tx: ID): WSMQueryCancel => ({
  event: MQUERY_CANCEL,
  tx: tx,
})

export const wrapReportWS = (report: MReport<any>): WSMReport => ({
  data: report.wrap(),
  event: MREPORT_EVENT,
})

export const wrapReportResponseWS = (
  tx: ID,
  response: unknown,
): WSMReportResponse => ({
  event: MREPORT_RESPONSE_EVENT,
  data: {
    response,
    tx,
  },
})

export const wrapReportCancel = (tx: ID): WSMReportCancel => ({
  event: MREPORT_CANCEL,
  tx: tx,
})

export const getTx = (m: WSMMessage): ID => {
  // command
  if (m.event === MCOMMAND_EVENT) {
    return m.data.command.tx
  }
  if (m.event === MCOMMAND_RESPONSE_EVENT) {
    return m.data.tx
  }
  if (m.event === MCOMMAND_ERROR_EVENT) {
    return m.data.tx
  }
  // query
  if (m.event === MQUERY_EVENT) {
    return m.data.query.tx
  }
  if (m.event === MQUERY_RESPONSE_EVENT) {
    return m.data.tx
  }
  if (m.event === MQUERY_CANCEL) {
    return m.tx
  }
  if (m.event === MQUERY_ERROR_EVENT) {
    return m.data.tx
  }

  // report
  if (m.event === MREPORT_EVENT) {
    return m.data.report.tx
  }
  if (m.event === MREPORT_RESPONSE_EVENT) {
    return m.data.tx
  }
  if (m.event === MREPORT_CANCEL) {
    return m.tx
  }
  if (m.event === MREPORT_ERROR_EVENT) {
    return m.data.tx
  }
  if (m.event === MEVENT_EVENT) {
    return m.data.tx
  }
  if (m.event === MPING_EVENT) {
    return m.data.id
  }
  throw new Error('Unknown event type')
}
