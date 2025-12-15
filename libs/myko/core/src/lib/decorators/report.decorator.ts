import { reportBus } from '../busses'
import { MYKO_HANDLER_REPORT_ID_KEY, MYKO_REPORT_ID_KEY } from '../constants'
import { reportHandlers, reports } from '../registry'
import type { MReport, MReportHandler } from '../types/report'

export const MykoReport =
  <R>() =>
  <T extends MReport<R>>(target: new (...args: any[]) => T): any => {
    const original: any = target
    const reportId = Object.getOwnPropertyDescriptors(original).name
      .value as string

    reports.add(reportId)

    const withType: any = (...args: any[]) => {
      const typed = new original(...args)
      Reflect.defineMetadata(MYKO_REPORT_ID_KEY, reportId, typed)
      return typed
    }
    Reflect.defineMetadata(MYKO_REPORT_ID_KEY, reportId, withType)
    return withType
  }

export const MykoReportHandler = <T extends MReport<T['$reportResult']>>(
  command: new (...args: any[]) => T,
): ((target: new (...args: any[]) => MReportHandler<T>) => void) => {
  return (target: new (...args: any[]) => MReportHandler<T>) => {
    const reportId = Reflect.getMetadata(MYKO_REPORT_ID_KEY, command)
    reportHandlers.add(reportId)
    Reflect.defineMetadata(MYKO_HANDLER_REPORT_ID_KEY, reportId, target)
    reportBus.registerHandler(target)
  }
}
