import { MReport, reportBus, type ID, type MWrappedReport } from '@myko/core'
import {
  MREPORT_ERROR_EVENT,
  wrapReportResponseWS,
  type WSMMessage,
  type WSMReportError,
} from '@myko/ws'
import {
  catchError,
  filter,
  map,
  merge,
  of,
  takeUntil,
  type Subject,
} from 'rxjs'
import { clientDisconnect, unsub } from './common'

export const handleReport = (
  clientId: ID,
  wrappedReport: MWrappedReport,
  respond: Subject<{ clientId: ID; data: WSMMessage }>,
) => {
  const report = MReport.fromWrappedReport(wrappedReport).withContext({
    commandClientId: clientId,
    tx: wrappedReport.report.tx,
  })

  const response = reportBus.watch(report).pipe(
    map((r) => wrapReportResponseWS(report.tx, r)),
    catchError((e) => {
      console.error(e.message)
      return of({
        data: {
          message: e.message,
          tx: report.tx,
        },
        event: MREPORT_ERROR_EVENT,
      } satisfies WSMReportError)
    }),
    takeUntil(
      merge(
        clientDisconnect(clientId),
        unsub.pipe(filter((u) => u === report.tx)),
      ),
    ),
  )

  response.subscribe((x) => {
    respond.next({
      clientId: clientId,
      data: x,
    })
  })
}
