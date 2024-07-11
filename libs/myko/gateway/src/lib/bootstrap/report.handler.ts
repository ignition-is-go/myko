import {
  reportBus,
  unwrapReport,
  type ID,
  type MWrappedReport,
} from '@myko/core'
import { wrapReportResponseWS, type WSMMessage } from '@myko/ws'
import { catchError, filter, map, takeUntil, type Subject } from 'rxjs'
import { clientDisconnect, unsub } from './common'

export const handleReport = (
  clientId: ID,
  wrappedReport: MWrappedReport,
  respond: Subject<{ clientId: ID; data: WSMMessage }>,
) => {
  const report = unwrapReport(wrappedReport)

  const response = reportBus.watch(report).pipe(
    map((r) => wrapReportResponseWS(report.tx, r)),
    catchError((e) => {
      console.log(e)
      throw e
    }),
    takeUntil(clientDisconnect(clientId)),
    takeUntil(unsub.pipe(filter((u) => u === report.tx))),
  )

  response.subscribe((x) => {
    respond.next({
      clientId: clientId,
      data: x,
    })
  })
}
